"""Centralized Redis client with connection pooling"""
import redis
import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get('REDIS_URL')
_redis_pool: Optional[redis.ConnectionPool] = None
_redis_client: Optional[redis.Redis] = None


def get_redis_pool() -> Optional[redis.ConnectionPool]:
    """Get shared connection pool (lazy init)"""
    global _redis_pool
    if _redis_pool is None and REDIS_URL:
        _redis_pool = redis.ConnectionPool.from_url(
            REDIS_URL,
            decode_responses=True,
            max_connections=20
        )
        logger.info("Redis connection pool created")
    return _redis_pool


def get_redis() -> Optional[redis.Redis]:
    """Get Redis client using shared pool.

    Thread-safe, uses connection pooling.
    """
    global _redis_client
    if _redis_client is None:
        pool = get_redis_pool()
        if pool:
            _redis_client = redis.Redis(connection_pool=pool)
            logger.info("Redis client created")
    return _redis_client


def create_redis_client() -> Optional[redis.Redis]:
    """Create new Redis client (for isolated contexts).

    Still uses shared pool for efficiency.
    """
    pool = get_redis_pool()
    if pool:
        return redis.Redis(connection_pool=pool)
    if REDIS_URL:
        return redis.from_url(REDIS_URL, decode_responses=True)
    return None


def wait_for_redis(max_wait: int = 30) -> bool:
    """Wait for Redis to become available with exponential backoff.

    Returns True if connected, False if timed out.
    """
    if not REDIS_URL:
        logger.warning("[REDIS] No REDIS_URL configured")
        return False

    start = time.time()
    attempt = 0
    delay = 0.5

    while time.time() - start < max_wait:
        attempt += 1
        try:
            client = get_redis()
            if client and client.ping():
                logger.info(f"[REDIS] Connected after {attempt} attempt(s)")
                return True
        except (redis.ConnectionError, redis.TimeoutError) as e:
            if attempt == 1 or attempt % 5 == 0:
                logger.warning(f"[REDIS] Not ready (attempt {attempt}): {e}")
        except Exception as e:
            logger.warning(f"[REDIS] Unexpected error (attempt {attempt}): {e}")

        time.sleep(delay)
        delay = min(delay * 2, 10)

    logger.error(f"[REDIS] Failed to connect after {max_wait}s ({attempt} attempts)")
    return False


def close_redis_pool():
    """Close Redis pool on shutdown"""
    global _redis_pool, _redis_client
    if _redis_pool:
        try:
            _redis_pool.disconnect()
        except Exception as e:
            logger.error(f"[REDIS] Error closing pool: {e}")
        _redis_pool = None
        _redis_client = None
        logger.info("[REDIS] Pool closed")
