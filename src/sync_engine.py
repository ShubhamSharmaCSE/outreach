"""
Sync engine core - orchestrates the synchronization process.
Handles queue management, operation processing, and error handling.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from uuid import UUID

import redis.asyncio as redis
from structlog import get_logger

from .models import (
    SyncOperation, SyncResult, SyncStatus, OperationType,
    ProviderConfig, QueueMetrics
)
from .api_client import ExternalAPIClient, APIError, RateLimitError
from .rate_limiter import RateLimiterManager
from .schema_transformer import SchemaTransformer

logger = get_logger(__name__)


class SyncEngine:
    """
    Core sync engine that processes operations from the queue
    and manages external API interactions.
    """
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.rate_limiter = RateLimiterManager(redis_client)
        self.schema_transformer = SchemaTransformer()
        self.providers: Dict[str, ProviderConfig] = {}
        self.running = False
        self.worker_tasks: List[asyncio.Task] = []
        
        # Queue names
        self.PENDING_QUEUE = "sync:pending"
        self.PROCESSING_QUEUE = "sync:processing"
        self.COMPLETED_QUEUE = "sync:completed"
        self.FAILED_QUEUE = "sync:failed"
        self.DLQ = "sync:dlq"  # Dead letter queue
        
        # Metrics tracking
        self.metrics_key = "sync:metrics"
    
    async def add_provider(self, config: ProviderConfig) -> None:
        """Add or update a provider configuration."""
        self.providers[config.name] = config
        
        # Configure rate limiter
        await self.rate_limiter.add_provider(
            config.name,
            config.rate_limit_per_minute,
            config.burst_limit
        )
        
        logger.info("Provider added", provider=config.name, type=config.provider_type)
    
    async def remove_provider(self, provider_name: str) -> None:
        """Remove a provider configuration."""
        if provider_name in self.providers:
            del self.providers[provider_name]
            await self.rate_limiter.remove_provider(provider_name)
            logger.info("Provider removed", provider=provider_name)
    
    async def submit_operation(self, operation: SyncOperation) -> UUID:
        """Submit a sync operation to the queue."""
        
        # Validate provider exists
        if operation.provider not in self.providers:
            raise ValueError(f"Unknown provider: {operation.provider}")
        
        # Serialize operation
        operation_data = {
            "id": str(operation.id),
            "operation": operation.operation.value,
            "provider": operation.provider,
            "record_id": operation.record_id,
            "record_data": operation.record_data.dict() if operation.record_data else None,
            "priority": operation.priority,
            "created_at": operation.created_at.isoformat(),
            "scheduled_at": operation.scheduled_at.isoformat() if operation.scheduled_at else None
        }
        
        # Add to pending queue with priority (lower numbers = higher priority)
        await self.redis.zadd(
            self.PENDING_QUEUE,
            {json.dumps(operation_data): operation.priority}
        )
        
        # Update metrics
        await self._update_metrics("operations_submitted", 1)
        
        logger.info(
            "Operation submitted",
            operation_id=operation.id,
            operation_type=operation.operation,
            provider=operation.provider,
            priority=operation.priority
        )
        
        return operation.id
    
    async def get_operation_status(self, operation_id: UUID) -> Optional[SyncResult]:
        """Get the status of a sync operation."""
        
        # Check all queues for the operation
        queues = [
            self.PENDING_QUEUE,
            self.PROCESSING_QUEUE,
            self.COMPLETED_QUEUE,
            self.FAILED_QUEUE,
            self.DLQ
        ]
        
        for queue_name in queues:
            if queue_name in [self.PENDING_QUEUE, self.PROCESSING_QUEUE]:
                # These are sorted sets
                items = await self.redis.zrange(queue_name, 0, -1)
            else:
                # These are lists
                items = await self.redis.lrange(queue_name, 0, -1)
            
            for item in items:
                try:
                    data = json.loads(item)
                    if data.get("id") == str(operation_id):
                        return self._deserialize_sync_result(data, queue_name)
                except json.JSONDecodeError:
                    continue
        
        return None
    
    def _deserialize_sync_result(self, data: Dict, queue_name: str) -> SyncResult:
        """Convert stored data to SyncResult."""
        
        # Determine status based on queue
        status_mapping = {
            self.PENDING_QUEUE: SyncStatus.PENDING,
            self.PROCESSING_QUEUE: SyncStatus.PROCESSING,
            self.COMPLETED_QUEUE: SyncStatus.COMPLETED,
            self.FAILED_QUEUE: SyncStatus.FAILED,
            self.DLQ: SyncStatus.FAILED
        }
        
        return SyncResult(
            operation_id=UUID(data["id"]),
            status=status_mapping.get(queue_name, SyncStatus.PENDING),
            provider=data["provider"],
            external_id=data.get("external_id"),
            started_at=datetime.fromisoformat(data["started_at"]) if data.get("started_at") else None,
            completed_at=datetime.fromisoformat(data["completed_at"]) if data.get("completed_at") else None,
            error_message=data.get("error_message"),
            retry_count=data.get("retry_count", 0),
            response_data=data.get("response_data")
        )
    
    async def start_workers(self, num_workers: int = 3) -> None:
        """Start worker processes to handle sync operations."""
        self.running = True
        
        for i in range(num_workers):
            task = asyncio.create_task(self._worker_loop(f"worker-{i}"))
            self.worker_tasks.append(task)
        
        logger.info("Sync workers started", num_workers=num_workers)
    
    async def stop_workers(self) -> None:
        """Stop all worker processes."""
        self.running = False
        
        # Cancel all worker tasks
        for task in self.worker_tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.worker_tasks, return_exceptions=True)
        self.worker_tasks.clear()
        
        logger.info("Sync workers stopped")
    
    async def _worker_loop(self, worker_id: str) -> None:
        """Main worker loop for processing sync operations."""
        
        logger.info("Worker started", worker_id=worker_id)
        
        while self.running:
            try:
                # Get next operation from queue (blocking with timeout)
                result = await self.redis.bzpopmin(self.PENDING_QUEUE, timeout=5)
                
                if not result:
                    continue  # Timeout, check if still running
                
                queue_name, operation_data, priority = result
                operation_dict = json.loads(operation_data)
                
                # Move to processing queue
                await self.redis.zadd(
                    self.PROCESSING_QUEUE,
                    {operation_data: priority}
                )
                
                logger.info(
                    "Processing operation",
                    worker_id=worker_id,
                    operation_id=operation_dict["id"],
                    operation_type=operation_dict["operation"]
                )
                
                # Process the operation
                success = await self._process_operation(operation_dict, worker_id)
                
                # Remove from processing queue
                await self.redis.zrem(self.PROCESSING_QUEUE, operation_data)
                
                # Move to appropriate result queue
                if success:
                    await self.redis.lpush(self.COMPLETED_QUEUE, operation_data)
                    await self._update_metrics("operations_completed", 1)
                else:
                    retry_count = operation_dict.get("retry_count", 0)
                    max_retries = 3  # Could be configurable per operation
                    
                    if retry_count < max_retries:
                        # Retry with exponential backoff
                        operation_dict["retry_count"] = retry_count + 1
                        delay = min(300, 2 ** retry_count)  # Max 5 minutes
                        
                        # Schedule retry
                        scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
                        operation_dict["scheduled_at"] = scheduled_at.isoformat()
                        
                        await self.redis.zadd(
                            self.PENDING_QUEUE,
                            {json.dumps(operation_dict): priority}
                        )
                        
                        logger.info(
                            "Operation scheduled for retry",
                            operation_id=operation_dict["id"],
                            retry_count=retry_count + 1,
                            delay_seconds=delay
                        )
                    else:
                        # Move to dead letter queue
                        await self.redis.lpush(self.DLQ, operation_data)
                        await self._update_metrics("operations_failed", 1)
                        
                        logger.error(
                            "Operation moved to DLQ after max retries",
                            operation_id=operation_dict["id"],
                            retry_count=retry_count
                        )
                
            except asyncio.CancelledError:
                logger.info("Worker cancelled", worker_id=worker_id)
                break
            except Exception as e:
                logger.error("Worker error", worker_id=worker_id, error=str(e))
                await asyncio.sleep(1)  # Brief pause before continuing
        
        logger.info("Worker stopped", worker_id=worker_id)
    
    async def _process_operation(self, operation_dict: Dict, worker_id: str) -> bool:
        """Process a single sync operation."""
        
        operation_id = operation_dict["id"]
        operation_type = OperationType(operation_dict["operation"])
        provider_name = operation_dict["provider"]
        
        try:
            # Check if operation is scheduled for future
            if operation_dict.get("scheduled_at"):
                scheduled_time = datetime.fromisoformat(operation_dict["scheduled_at"])
                if datetime.utcnow() < scheduled_time:
                    # Put back in queue for later
                    await self.redis.zadd(
                        self.PENDING_QUEUE,
                        {json.dumps(operation_dict): operation_dict.get("priority", 5)}
                    )
                    return True
            
            # Get provider config
            provider_config = self.providers.get(provider_name)
            if not provider_config:
                raise ValueError(f"Provider not configured: {provider_name}")
            
            # Update operation with processing info
            operation_dict["started_at"] = datetime.utcnow().isoformat()
            
            # Create API client and execute operation
            async with ExternalAPIClient(
                provider_config,
                self.rate_limiter,
                self.schema_transformer
            ) as client:
                
                # Prepare operation arguments
                kwargs = {}
                if operation_dict.get("record_id"):
                    kwargs["record_id"] = operation_dict["record_id"]
                
                if operation_dict.get("record_data"):
                    from .models import RecordData
                    kwargs["record_data"] = RecordData(**operation_dict["record_data"])
                
                # Execute the operation
                response = await client.execute_operation(operation_type, **kwargs)
                
                # Update operation with success info
                operation_dict["completed_at"] = datetime.utcnow().isoformat()
                operation_dict["response_data"] = response
                
                # Extract external ID from response if available
                if operation_type == OperationType.CREATE and response:
                    operation_dict["external_id"] = response.get("id")
                
                logger.info(
                    "Operation completed successfully",
                    operation_id=operation_id,
                    worker_id=worker_id,
                    response_keys=list(response.keys()) if response else []
                )
                
                return True
                
        except RateLimitError as e:
            logger.warning(
                "Rate limit error, will retry",
                operation_id=operation_id,
                error=str(e)
            )
            operation_dict["error_message"] = str(e)
            return False
            
        except APIError as e:
            logger.error(
                "API error processing operation",
                operation_id=operation_id,
                error=str(e)
            )
            operation_dict["error_message"] = str(e)
            return False
            
        except Exception as e:
            logger.error(
                "Unexpected error processing operation",
                operation_id=operation_id,
                error=str(e),
                exc_info=True
            )
            operation_dict["error_message"] = f"Unexpected error: {str(e)}"
            return False
    
    async def _update_metrics(self, metric_name: str, value: int) -> None:
        """Update metrics in Redis."""
        current_hour = datetime.utcnow().strftime("%Y-%m-%d-%H")
        metric_key = f"{self.metrics_key}:{current_hour}:{metric_name}"
        await self.redis.incrby(metric_key, value)
        await self.redis.expire(metric_key, 86400)  # 24 hours TTL
    
    async def get_queue_metrics(self) -> QueueMetrics:
        """Get current queue metrics."""
        
        pending = await self.redis.zcard(self.PENDING_QUEUE)
        processing = await self.redis.zcard(self.PROCESSING_QUEUE)
        failed = await self.redis.llen(self.DLQ)
        
        # Get completed operations in last hour
        current_hour = datetime.utcnow().strftime("%Y-%m-%d-%H")
        completed_key = f"{self.metrics_key}:{current_hour}:operations_completed"
        completed_last_hour = int(await self.redis.get(completed_key) or 0)
        
        # Calculate average processing time (simplified)
        # In production, this would use more sophisticated time tracking
        avg_processing_time = 2.5  # Placeholder
        
        # Calculate error rate
        failed_key = f"{self.metrics_key}:{current_hour}:operations_failed"
        failed_last_hour = int(await self.redis.get(failed_key) or 0)
        total_operations = completed_last_hour + failed_last_hour
        error_rate = (failed_last_hour / total_operations * 100) if total_operations > 0 else 0
        
        return QueueMetrics(
            pending_operations=pending,
            processing_operations=processing,
            failed_operations=failed,
            completed_operations_last_hour=completed_last_hour,
            average_processing_time_seconds=avg_processing_time,
            error_rate_percentage=error_rate
        )
