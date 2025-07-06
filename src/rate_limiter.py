"""
Rate limiter implementation using token bucket algorithm.
Provides per-provider rate limiting with burst capacity.
"""

import asyncio
import time
from typing import Dict, Optional

import redis.asyncio as redis
from structlog import get_logger

logger = get_logger(__name__)


class TokenBucket:
    """Token bucket rate limiter for a single provider."""
    
    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        redis_client: redis.Redis,
        key_prefix: str = "rate_limit"
    ):
        self.capacity = capacity
        self.refill_rate = refill_rate  # tokens per second
        self.redis_client = redis_client
        self.key_prefix = key_prefix
        
    async def consume(self, provider: str, tokens: int = 1) -> bool:
        """
        Attempt to consume tokens from the bucket.
        
        Args:
            provider: Provider identifier
            tokens: Number of tokens to consume
            
        Returns:
            True if tokens were consumed, False if rate limited
        """
        bucket_key = f"{self.key_prefix}:{provider}"
        
        # Lua script for atomic token bucket operation
        lua_script = """
        local bucket_key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local tokens_requested = tonumber(ARGV[3])
        local current_time = tonumber(ARGV[4])
        
        -- Get current bucket state
        local bucket_data = redis.call('HMGET', bucket_key, 'tokens', 'last_refill')
        local current_tokens = tonumber(bucket_data[1]) or capacity
        local last_refill = tonumber(bucket_data[2]) or current_time
        
        -- Calculate tokens to add based on time elapsed
        local time_elapsed = current_time - last_refill
        local tokens_to_add = math.floor(time_elapsed * refill_rate)
        
        -- Update token count, capped at capacity
        current_tokens = math.min(capacity, current_tokens + tokens_to_add)
        
        -- Check if we can consume the requested tokens
        if current_tokens >= tokens_requested then
            -- Consume tokens
            current_tokens = current_tokens - tokens_requested
            
            -- Update bucket state
            redis.call('HMSET', bucket_key, 
                      'tokens', current_tokens, 
                      'last_refill', current_time)
            redis.call('EXPIRE', bucket_key, 3600)  -- 1 hour TTL
            
            return 1  -- Success
        else
            -- Update last_refill time but don't consume tokens
            redis.call('HMSET', bucket_key, 
                      'tokens', current_tokens, 
                      'last_refill', current_time)
            redis.call('EXPIRE', bucket_key, 3600)
            
            return 0  -- Rate limited
        end
        """
        
        current_time = time.time()
        result = await self.redis_client.eval(
            lua_script,
            1,  # Number of keys
            bucket_key,
            self.capacity,
            self.refill_rate,
            tokens,
            current_time
        )
        
        return bool(result)
    
    async def get_status(self, provider: str) -> Dict[str, float]:
        """Get current bucket status for a provider."""
        bucket_key = f"{self.key_prefix}:{provider}"
        current_time = time.time()
        
        bucket_data = await self.redis_client.hmget(
            bucket_key, 'tokens', 'last_refill'
        )
        
        current_tokens = float(bucket_data[0] or self.capacity)
        last_refill = float(bucket_data[1] or current_time)
        
        # Calculate current tokens after refill
        time_elapsed = current_time - last_refill
        tokens_to_add = time_elapsed * self.refill_rate
        current_tokens = min(self.capacity, current_tokens + tokens_to_add)
        
        return {
            "current_tokens": current_tokens,
            "capacity": self.capacity,
            "refill_rate": self.refill_rate,
            "utilization": 1.0 - (current_tokens / self.capacity)
        }


class RateLimiterManager:
    """Manages rate limiters for multiple providers."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis_client = redis_client
        self.limiters: Dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()
        
    async def add_provider(
        self,
        provider: str,
        rate_limit_per_minute: int,
        burst_limit: int
    ):
        """Add or update rate limiter for a provider."""
        async with self._lock:
            # Convert per-minute rate to per-second
            refill_rate = rate_limit_per_minute / 60.0
            
            self.limiters[provider] = TokenBucket(
                capacity=burst_limit,
                refill_rate=refill_rate,
                redis_client=self.redis_client,
                key_prefix=f"rate_limit:{provider}"
            )
            
            logger.info(
                "Rate limiter configured",
                provider=provider,
                rate_limit_per_minute=rate_limit_per_minute,
                burst_limit=burst_limit,
                refill_rate=refill_rate
            )
    
    async def can_make_request(self, provider: str, tokens: int = 1) -> bool:
        """
        Check if a request can be made to the provider.
        
        Args:
            provider: Provider identifier
            tokens: Number of tokens needed (default 1)
            
        Returns:
            True if request can be made, False if rate limited
        """
        if provider not in self.limiters:
            logger.warning("No rate limiter configured for provider", provider=provider)
            return True  # Allow request if no limiter configured
        
        limiter = self.limiters[provider]
        can_proceed = await limiter.consume(provider, tokens)
        
        if not can_proceed:
            logger.warning(
                "Rate limit exceeded",
                provider=provider,
                tokens_requested=tokens
            )
        
        return can_proceed
    
    async def wait_for_capacity(
        self,
        provider: str,
        tokens: int = 1,
        timeout: float = 30.0
    ) -> bool:
        """
        Wait for rate limit capacity to become available.
        
        Args:
            provider: Provider identifier
            tokens: Number of tokens needed
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if capacity became available, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if await self.can_make_request(provider, tokens):
                return True
            
            # Calculate wait time based on refill rate
            if provider in self.limiters:
                limiter = self.limiters[provider]
                wait_time = min(tokens / limiter.refill_rate, 1.0)
            else:
                wait_time = 0.1
            
            await asyncio.sleep(wait_time)
        
        logger.warning(
            "Timeout waiting for rate limit capacity",
            provider=provider,
            tokens=tokens,
            timeout=timeout
        )
        return False
    
    async def get_provider_status(self, provider: str) -> Optional[Dict[str, float]]:
        """Get rate limiter status for a provider."""
        if provider not in self.limiters:
            return None
        
        return await self.limiters[provider].get_status(provider)
    
    async def get_all_status(self) -> Dict[str, Dict[str, float]]:
        """Get rate limiter status for all providers."""
        status = {}
        for provider in self.limiters:
            status[provider] = await self.get_provider_status(provider)
        return status
    
    async def remove_provider(self, provider: str):
        """Remove rate limiter for a provider."""
        async with self._lock:
            if provider in self.limiters:
                del self.limiters[provider]
                logger.info("Rate limiter removed", provider=provider)
