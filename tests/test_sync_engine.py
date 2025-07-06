"""
Tests for the sync engine core functionality.
"""

import json
from datetime import datetime
from uuid import uuid4

import pytest

from src.models import SyncOperation, OperationType, RecordData, SyncStatus
from src.sync_engine import SyncEngine


@pytest.mark.asyncio
async def test_add_provider(sync_engine, sample_provider_config):
    """Test adding a provider configuration."""
    
    await sync_engine.add_provider(sample_provider_config)
    
    assert sample_provider_config.name in sync_engine.providers
    assert sync_engine.providers[sample_provider_config.name] == sample_provider_config


@pytest.mark.asyncio
async def test_submit_operation(sync_engine, sample_provider_config):
    """Test submitting a sync operation."""
    
    # Add provider first
    await sync_engine.add_provider(sample_provider_config)
    
    # Create test operation
    operation = SyncOperation(
        operation=OperationType.CREATE,
        provider=sample_provider_config.name,
        record_data=RecordData(
            data={
                "name": "John Doe",
                "email": "john@example.com"
            }
        ),
        priority=5
    )
    
    # Submit operation
    operation_id = await sync_engine.submit_operation(operation)
    
    assert operation_id == operation.id
    
    # Verify Redis operations were called
    sync_engine.redis.zadd.assert_called_once()
    
    # Check that the operation was added to pending queue
    call_args = sync_engine.redis.zadd.call_args
    queue_name = call_args[0][0]
    assert queue_name == sync_engine.PENDING_QUEUE


@pytest.mark.asyncio
async def test_submit_operation_unknown_provider(sync_engine):
    """Test submitting operation with unknown provider."""
    
    operation = SyncOperation(
        operation=OperationType.CREATE,
        provider="unknown_provider",
        record_data=RecordData(data={"test": "data"}),
        priority=5
    )
    
    with pytest.raises(ValueError, match="Unknown provider"):
        await sync_engine.submit_operation(operation)


@pytest.mark.asyncio
async def test_get_operation_status(sync_engine):
    """Test getting operation status."""
    
    operation_id = uuid4()
    
    # Mock Redis response for pending queue
    test_data = {
        "id": str(operation_id),
        "operation": "CREATE",
        "provider": "test_provider",
        "created_at": datetime.utcnow().isoformat()
    }
    
    sync_engine.redis.zrange.return_value = [json.dumps(test_data)]
    
    result = await sync_engine.get_operation_status(operation_id)
    
    assert result is not None
    assert result.operation_id == operation_id
    assert result.status == SyncStatus.PENDING


@pytest.mark.asyncio
async def test_get_queue_metrics(sync_engine):
    """Test getting queue metrics."""
    
    # Mock Redis responses
    sync_engine.redis.zcard.return_value = 5  # pending/processing queues
    sync_engine.redis.llen.return_value = 2   # failed queue
    sync_engine.redis.get.return_value = "10"  # completed operations
    
    metrics = await sync_engine.get_queue_metrics()
    
    assert metrics.pending_operations == 5
    assert metrics.processing_operations == 5
    assert metrics.failed_operations == 2
    assert metrics.completed_operations_last_hour == 10


@pytest.mark.asyncio
async def test_worker_startup_shutdown(sync_engine):
    """Test worker process startup and shutdown."""
    
    # Start workers
    await sync_engine.start_workers(num_workers=2)
    
    assert sync_engine.running is True
    assert len(sync_engine.worker_tasks) == 2
    
    # Stop workers
    await sync_engine.stop_workers()
    
    assert sync_engine.running is False
    assert len(sync_engine.worker_tasks) == 0
