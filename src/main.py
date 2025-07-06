"""
FastAPI application for the sync service.
Provides REST API endpoints for managing sync operations and providers.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Dict, List
from uuid import UUID

import redis.asyncio as redis
import structlog
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi.responses import PlainTextResponse

from .models import (
    SyncOperation, SyncResult, ProviderConfig, HealthStatus,
    QueueMetrics, ProviderMetrics
)
from .sync_engine import SyncEngine

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Prometheus metrics
OPERATION_COUNTER = Counter(
    'sync_operations_total',
    'Total number of sync operations',
    ['operation_type', 'provider', 'status']
)

OPERATION_DURATION = Histogram(
    'sync_operation_duration_seconds',
    'Time spent processing sync operations',
    ['operation_type', 'provider']
)

QUEUE_SIZE = Gauge(
    'sync_queue_size',
    'Number of operations in each queue',
    ['queue_type']
)

RATE_LIMIT_UTILIZATION = Gauge(
    'sync_rate_limit_utilization',
    'Rate limit utilization per provider',
    ['provider']
)

# Global sync engine instance
sync_engine: SyncEngine = None
redis_client: redis.Redis = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global sync_engine, redis_client
    
    # Initialize Redis connection
    redis_client = redis.from_url("redis://localhost:6379", decode_responses=True)
    
    # Initialize sync engine
    sync_engine = SyncEngine(redis_client)
    
    # Start worker processes
    await sync_engine.start_workers(num_workers=3)
    
    logger.info("Sync service started")
    
    yield
    
    # Cleanup
    await sync_engine.stop_workers()
    await redis_client.close()
    
    logger.info("Sync service stopped")


# Create FastAPI application
app = FastAPI(
    title="Bi-Directional Record Sync Service",
    description="High-performance async sync service for external CRM providers",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_sync_engine() -> SyncEngine:
    """Dependency to get sync engine instance."""
    if sync_engine is None:
        raise HTTPException(status_code=503, detail="Sync engine not initialized")
    return sync_engine


@app.get("/health", response_model=HealthStatus)
async def health_check(engine: SyncEngine = Depends(get_sync_engine)):
    """Health check endpoint."""
    
    try:
        # Check Redis connection
        await redis_client.ping()
        redis_connected = True
    except Exception:
        redis_connected = False
    
    # Get queue metrics
    queue_metrics = await engine.get_queue_metrics()
    
    # Get provider metrics
    provider_metrics = []
    for provider_name in engine.providers:
        rate_status = await engine.rate_limiter.get_provider_status(provider_name)
        if rate_status:
            provider_metrics.append(ProviderMetrics(
                provider_name=provider_name,
                requests_per_minute=0,  # Would track this in production
                rate_limit_utilization=rate_status["utilization"],
                average_response_time_ms=0,  # Would track this in production
                success_rate_percentage=95.0,  # Would calculate from metrics
                last_request_at=None
            ))
    
    # Overall health status
    status = "healthy" if redis_connected and queue_metrics.error_rate_percentage < 10 else "degraded"
    
    return HealthStatus(
        status=status,
        redis_connected=redis_connected,
        active_providers=len(engine.providers),
        queue_metrics=queue_metrics,
        provider_metrics=provider_metrics
    )


@app.post("/sync", response_model=Dict[str, str])
async def submit_sync_operation(
    operation: SyncOperation,
    engine: SyncEngine = Depends(get_sync_engine)
):
    """Submit a new sync operation."""
    
    try:
        operation_id = await engine.submit_operation(operation)
        
        # Update metrics
        OPERATION_COUNTER.labels(
            operation_type=operation.operation.value,
            provider=operation.provider,
            status="submitted"
        ).inc()
        
        logger.info(
            "Sync operation submitted",
            operation_id=operation_id,
            operation_type=operation.operation,
            provider=operation.provider
        )
        
        return {
            "operation_id": str(operation_id),
            "status": "submitted",
            "message": "Operation queued for processing"
        }
        
    except ValueError as e:
        logger.error("Invalid sync operation", error=str(e))
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error("Failed to submit sync operation", error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/sync/status/{operation_id}", response_model=SyncResult)
async def get_sync_status(
    operation_id: UUID,
    engine: SyncEngine = Depends(get_sync_engine)
):
    """Get the status of a sync operation."""
    
    try:
        result = await engine.get_operation_status(operation_id)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Operation not found")
        
        return result
        
    except Exception as e:
        logger.error("Failed to get sync status", operation_id=operation_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/providers")
async def add_provider(
    config: ProviderConfig,
    engine: SyncEngine = Depends(get_sync_engine)
):
    """Add or update a provider configuration."""
    
    try:
        await engine.add_provider(config)
        
        logger.info("Provider configured", provider=config.name)
        
        return {
            "provider": config.name,
            "status": "configured",
            "message": "Provider configuration added successfully"
        }
        
    except Exception as e:
        logger.error("Failed to configure provider", provider=config.name, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to configure provider")


@app.get("/providers", response_model=List[str])
async def list_providers(engine: SyncEngine = Depends(get_sync_engine)):
    """List all configured providers."""
    return list(engine.providers.keys())


@app.delete("/providers/{provider_name}")
async def remove_provider(
    provider_name: str,
    engine: SyncEngine = Depends(get_sync_engine)
):
    """Remove a provider configuration."""
    
    try:
        await engine.remove_provider(provider_name)
        
        logger.info("Provider removed", provider=provider_name)
        
        return {
            "provider": provider_name,
            "status": "removed",
            "message": "Provider configuration removed successfully"
        }
        
    except Exception as e:
        logger.error("Failed to remove provider", provider=provider_name, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to remove provider")


@app.get("/providers/{provider_name}/status")
async def get_provider_status(
    provider_name: str,
    engine: SyncEngine = Depends(get_sync_engine)
):
    """Get rate limiter status for a specific provider."""
    
    if provider_name not in engine.providers:
        raise HTTPException(status_code=404, detail="Provider not found")
    
    status = await engine.rate_limiter.get_provider_status(provider_name)
    
    if status is None:
        raise HTTPException(status_code=404, detail="No rate limiter configured for provider")
    
    return {
        "provider": provider_name,
        "rate_limit_status": status
    }


@app.get("/metrics/queue", response_model=QueueMetrics)
async def get_queue_metrics(engine: SyncEngine = Depends(get_sync_engine)):
    """Get current queue metrics."""
    
    try:
        metrics = await engine.get_queue_metrics()
        
        # Update Prometheus metrics
        QUEUE_SIZE.labels(queue_type="pending").set(metrics.pending_operations)
        QUEUE_SIZE.labels(queue_type="processing").set(metrics.processing_operations)
        QUEUE_SIZE.labels(queue_type="failed").set(metrics.failed_operations)
        
        return metrics
        
    except Exception as e:
        logger.error("Failed to get queue metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get queue metrics")


@app.get("/metrics/providers")
async def get_provider_metrics(engine: SyncEngine = Depends(get_sync_engine)):
    """Get rate limiter metrics for all providers."""
    
    try:
        provider_status = await engine.rate_limiter.get_all_status()
        
        # Update Prometheus metrics
        for provider, status in provider_status.items():
            if status:
                RATE_LIMIT_UTILIZATION.labels(provider=provider).set(status["utilization"])
        
        return provider_status
        
    except Exception as e:
        logger.error("Failed to get provider metrics", error=str(e))
        raise HTTPException(status_code=500, detail="Failed to get provider metrics")


@app.get("/metrics/prometheus")
async def get_prometheus_metrics():
    """Get metrics in Prometheus format."""
    return PlainTextResponse(generate_latest(), media_type="text/plain")


# Background task for updating metrics
async def update_metrics_periodically():
    """Background task to update metrics periodically."""
    while True:
        try:
            if sync_engine:
                metrics = await sync_engine.get_queue_metrics()
                QUEUE_SIZE.labels(queue_type="pending").set(metrics.pending_operations)
                QUEUE_SIZE.labels(queue_type="processing").set(metrics.processing_operations)
                QUEUE_SIZE.labels(queue_type="failed").set(metrics.failed_operations)
                
                # Update provider metrics
                provider_status = await sync_engine.rate_limiter.get_all_status()
                for provider, status in provider_status.items():
                    if status:
                        RATE_LIMIT_UTILIZATION.labels(provider=provider).set(status["utilization"])
            
            await asyncio.sleep(30)  # Update every 30 seconds
            
        except Exception as e:
            logger.error("Error updating metrics", error=str(e))
            await asyncio.sleep(30)


# Start background metrics task
@app.on_event("startup")
async def start_background_tasks():
    """Start background tasks."""
    asyncio.create_task(update_metrics_periodically())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
