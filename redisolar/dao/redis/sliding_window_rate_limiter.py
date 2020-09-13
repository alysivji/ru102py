# Uncomment for Challenge #7
import random
import time

from redis.client import Redis

from redisolar.dao.base import RateLimiterDaoBase, RateLimitExceededException
from redisolar.dao.redis.base import RedisDaoBase
from redisolar.dao.redis.key_schema import KeySchema


class SlidingWindowRateLimiter(RateLimiterDaoBase, RedisDaoBase):
    """A sliding-window rate-limiter."""

    def __init__(
        self,
        window_size_ms: float,
        max_hits: int,
        redis_client: Redis,
        key_schema: KeySchema = None,
        **kwargs,
    ):
        self.window_size_ms = window_size_ms
        self.max_hits = max_hits
        super().__init__(redis_client, key_schema, **kwargs)

    def _get_key(self, name: str) -> str:
        return self.key_schema.sliding_window_rate_limiter_key(
            name, self.window_size_ms, self.max_hits
        )

    def hit(self, name: str):
        """Record a hit using the rate-limiter."""
        # START Challenge #7
        now = int(time.time() * 1000)

        key = self._get_key(name)
        pipeline = self.redis.pipeline()

        pipeline.zadd(key, {f"{now}-{random.random()}": now})
        pipeline.zremrangebyscore(key, min=0, max=now - self.window_size_ms)
        pipeline.zcard(key)
        _, _, hits = pipeline.execute()

        if hits > self.max_hits:
            raise RateLimitExceededException()

        # END Challenge #7
