"""
Tests for the rate limiter functionality.
"""

import asyncio
import time

import pytest

from src.rate_limiter import TokenBucket, RateLimiterManager


@pytest.mark.asyncio
async def test_token_bucket_consume_success(redis_client):
    """Test successful token consumption."""
    
    bucket = TokenBucket(
        capacity=10,
        refill_rate=2.0,  # 2 tokens per second
        redis_client=redis_client
    )
    
    # Mock successful consumption
    redis_client.eval.return_value = 1
    
    result = await bucket.consume("test_provider", tokens=1)
    
    assert result is True
    redis_client.eval.assert_called_once()


@pytest.mark.asyncio
async def test_token_bucket_consume_rate_limited(redis_client):
    """Test rate limited token consumption."""
    
    bucket = TokenBucket(
        capacity=10,
        refill_rate=2.0,
        redis_client=redis_client
    )
    
    # Mock rate limited response
    redis_client.eval.return_value = 0
    
    result = await bucket.consume("test_provider", tokens=1)
    
    assert result is False


@pytest.mark.asyncio
async def test_token_bucket_get_status(redis_client):
    """Test getting bucket status."""
    
    bucket = TokenBucket(
        capacity=10,
        refill_rate=2.0,
        redis_client=redis_client
    )
    
    # Mock Redis response
    current_time = time.time()
    redis_client.hmget.return_value = ["5.0", str(current_time - 1)]
    
    status = await bucket.get_status("test_provider")
    
    assert "current_tokens" in status
    assert "capacity" in status
    assert "refill_rate" in status
    assert "utilization" in status
    assert status["capacity"] == 10
    assert status["refill_rate"] == 2.0


@pytest.mark.asyncio
async def test_rate_limiter_manager_add_provider(rate_limiter):
    """Test adding a provider to rate limiter."""
    
    await rate_limiter.add_provider(
        provider="test_provider",
        rate_limit_per_minute=1200,  # 20 per second
        burst_limit=50
    )
    
    assert "test_provider" in rate_limiter.limiters
    
    limiter = rate_limiter.limiters["test_provider"]
    assert limiter.capacity == 50
    assert limiter.refill_rate == 20.0  # 1200/60


@pytest.mark.asyncio
async def test_rate_limiter_manager_can_make_request(rate_limiter):
    """Test checking if request can be made."""
    
    # Add provider
    await rate_limiter.add_provider(
        provider="test_provider",
        rate_limit_per_minute=1200,
        burst_limit=50
    )
    
    # Mock successful token consumption
    rate_limiter.redis_client.eval.return_value = 1
    
    result = await rate_limiter.can_make_request("test_provider")
    
    assert result is True


@pytest.mark.asyncio
async def test_rate_limiter_manager_unknown_provider(rate_limiter):
    """Test request for unknown provider."""
    
    result = await rate_limiter.can_make_request("unknown_provider")
    
    # Should allow request if no limiter configured
    assert result is True


@pytest.mark.asyncio
async def test_rate_limiter_wait_for_capacity(rate_limiter):
    """Test waiting for rate limit capacity."""
    
    await rate_limiter.add_provider(
        provider="test_provider",
        rate_limit_per_minute=1200,
        burst_limit=50
    )
    
    # Mock immediate success
    rate_limiter.redis_client.eval.return_value = 1
    
    result = await rate_limiter.wait_for_capacity("test_provider", timeout=1.0)
    
    assert result is True


@pytest.mark.asyncio
async def test_rate_limiter_wait_for_capacity_timeout(rate_limiter):
    """Test timeout when waiting for capacity."""
    
    await rate_limiter.add_provider(
        provider="test_provider",
        rate_limit_per_minute=1200,
        burst_limit=50
    )
    
    # Mock rate limited response
    rate_limiter.redis_client.eval.return_value = 0
    
    result = await rate_limiter.wait_for_capacity("test_provider", timeout=0.1)
    
    assert result is False
