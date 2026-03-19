"""Database access layer with context managers and reconnect logic"""
import psycopg2
import psycopg2.pool
from contextlib import contextmanager
from typing import Optional, Generator, Dict
import logging
import os
import time
import threading

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get('DATABASE_URL')
_db_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()

# Track connection ages for recycling stale connections
_conn_created_at: Dict[int, float] = {}
_conn_age_lock = threading.Lock()
MAX_CONN_AGE = 300  # Recycle connections older than 5 minutes


def get_db_pool() -> Optional[psycopg2.pool.ThreadedConnectionPool]:
    """Lazy init database pool"""
    global _db_pool
    if _db_pool is None and DATABASE_URL:
        with _pool_lock:
            if _db_pool is None:
                _db_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2, maxconn=10, dsn=DATABASE_URL,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=3,
                    options='-c statement_timeout=10000',  # 10s — prevents queries from hanging forever
                )
                logger.info("[DB] Pool created (minconn=2, maxconn=10)")
    return _db_pool


_pool_last_reset = 0
_POOL_RESET_COOLDOWN = 10


def _reset_pool():
    """Reset database pool (for reconnect), with cooldown to prevent stampede"""
    global _db_pool, _pool_last_reset
    with _pool_lock:
        now = time.time()
        if now - _pool_last_reset < _POOL_RESET_COOLDOWN:
            logger.debug(f"[DB] Pool reset skipped (cooldown, last {now - _pool_last_reset:.1f}s ago)")
            return
        if _db_pool:
            try:
                _db_pool.closeall()
            except Exception as e:
                logger.warning(f"[DB] Error closing pool during reset: {e}")
        _db_pool = None
        _pool_last_reset = now
    with _conn_age_lock:
        _conn_created_at.clear()
    logger.info("[DB] Pool reset")


def close_db_pool():
    """Close database pool on shutdown"""
    global _db_pool
    if _db_pool:
        try:
            _db_pool.closeall()
        except Exception as e:
            logger.error(f"[DB] Error closing pool: {e}")
        _db_pool = None
        with _conn_age_lock:
            _conn_created_at.clear()
        logger.info("[DB] Pool closed")


def _is_connection_error(e: Exception) -> bool:
    """Check if exception is a connection/SSL error that can be retried"""
    error_msg = str(e).lower()
    return any(x in error_msg for x in [
        'ssl connection has been closed',
        'connection reset',
        'connection refused',
        'connection timed out',
        'server closed the connection',
        'connection already closed',
        'connection is closed',
        'terminating connection',
        'could not connect',
        'network is unreachable',
        'no route to host',
        'too many connections',
        'connection pool exhausted',
        'remaining connection slots',
    ])


def get_pool_stats() -> dict:
    """Return current pool statistics for health endpoint"""
    pool = get_db_pool()
    if not pool:
        return {'status': 'not_initialized'}
    try:
        # ThreadedConnectionPool uses internal _lock (Lock) for thread safety
        with pool._lock:
            used = len(pool._used)
            free = len(pool._pool)
        return {
            'used': used,
            'free': free,
            'total': used + free,
            'maxconn': pool.maxconn,
            'minconn': pool.minconn,
        }
    except Exception as e:
        return {'error': str(e)}


def _is_conn_stale(conn) -> bool:
    """Check if connection is too old and should be recycled"""
    conn_id = id(conn)
    with _conn_age_lock:
        created = _conn_created_at.get(conn_id)
    if created is None:
        return False
    return (time.time() - created) > MAX_CONN_AGE


def _track_conn(conn):
    """Track when a connection was obtained (for age-based recycling)"""
    with _conn_age_lock:
        if id(conn) not in _conn_created_at:
            _conn_created_at[id(conn)] = time.time()


def _untrack_conn(conn):
    """Remove connection from age tracking"""
    with _conn_age_lock:
        _conn_created_at.pop(id(conn), None)


@contextmanager
def db_connection(max_retries: int = 3) -> Generator[psycopg2.extensions.connection, None, None]:
    """Context manager for database connection with auto-reconnect.

    Usage:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.commit()
    """
    last_error = None

    for attempt in range(max_retries):
        pool = get_db_pool()
        if not pool:
            raise RuntimeError("Database pool not initialized")

        conn = None
        try:
            thread_name = threading.current_thread().name
            t0 = time.time()
            logger.info(f"[DB] getconn start (thread={thread_name}, attempt={attempt + 1})")
            conn = pool.getconn()
            t1 = time.time()
            logger.info(f"[DB] getconn done in {t1 - t0:.2f}s (thread={thread_name})")
            _track_conn(conn)

            # Recycle stale connections from PG proxy
            if _is_conn_stale(conn):
                logger.info(f"[DB] Recycling stale connection (thread={thread_name})")
                _untrack_conn(conn)
                try:
                    pool.putconn(conn, close=True)
                except Exception:
                    pass
                conn = None  # Prevent double-putconn if next getconn raises
                conn = pool.getconn()
                _track_conn(conn)

            # NOTE: No SELECT 1 test — it can hang forever through Supavisor.
            # Dead connections will be caught by the actual query and retried.
            yield conn
            return  # Success, exit the retry loop

        except psycopg2.pool.PoolError as e:
            # Pool exhausted — phantom slots from hung psycopg2.connect() through Supavisor.
            # Reset pool to clear phantom _count, fresh pool starts at 0.
            last_error = e
            logger.warning(f"[DB] Pool exhausted (attempt {attempt + 1}/{max_retries}): {e} — resetting pool")
            _reset_pool()
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            raise

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last_error = e
            if _is_connection_error(e):
                logger.warning(f"[DB] Connection error (attempt {attempt + 1}/{max_retries}): {e}")
                if conn:
                    _untrack_conn(conn)
                    try:
                        pool.putconn(conn, close=True)
                    except Exception:
                        pass
                    conn = None
                if attempt < max_retries - 1:
                    _reset_pool()
                    time.sleep(0.5 * (attempt + 1))
                    continue
            raise

        finally:
            if conn:
                try:
                    logger.debug(f"[DB] putconn (thread={threading.current_thread().name})")
                    pool.putconn(conn)
                except Exception:
                    pass

    # If we get here, all retries failed
    if last_error:
        raise last_error
    raise RuntimeError("Database connection failed after retries")


@contextmanager
def db_cursor(commit: bool = False, max_retries: int = 3) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Context manager for database cursor with auto-reconnect.

    Args:
        commit: If True, commit transaction after block
        max_retries: Number of retry attempts for connection errors

    Usage:
        with db_cursor(commit=True) as cur:
            cur.execute("INSERT INTO ...")
    """
    with db_connection(max_retries=max_retries) as conn:
        cur = conn.cursor()
        try:
            yield cur
            if commit:
                conn.commit()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            if _is_connection_error(e):
                logger.error(f"[DB] Operation failed with connection error: {e}")
            raise
        finally:
            cur.close()
