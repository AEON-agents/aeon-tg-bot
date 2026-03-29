"""Database access layer with context managers and reconnect logic.
Strategy: Direct connection (port 5432) preferred, Supavisor pooler as fallback.
Direct is faster, supports LISTEN/NOTIFY and SET statement_timeout natively.
Supavisor drops SSL connections periodically — used only as fallback.
"""
import re
import psycopg2
import psycopg2.pool
from contextlib import contextmanager
from typing import Optional, Generator, Dict
import logging
import os
import time
import threading

logger = logging.getLogger(__name__)


def _fix_db_port(url):
    """Fix DB port based on host type."""
    if not url:
        return url
    if 'pooler.supabase.com' in url and ':5432' in url:
        return url.replace(':5432', ':6543')
    if 'pooler.supabase.com' not in url and ':6543' in url:
        return re.sub(r':6543\b', ':5432', url)
    return url


def _build_direct_url(pooler_url):
    """Derive direct connection URL from pooler URL.
    pooler: postgres.REF:PASS@xxx.pooler.supabase.com:6543/db
    direct: postgres:PASS@db.REF.supabase.co:5432/db"""
    if not pooler_url or 'pooler.supabase.com' not in pooler_url:
        return None
    try:
        from urllib.parse import urlparse
        parsed = urlparse(pooler_url)
        if not parsed.username or '.' not in parsed.username:
            return None
        ref = parsed.username.split('.', 1)[1]
        password = parsed.password
        if not password:
            return None
        return f"postgresql://postgres:{password}@db.{ref}.supabase.co:5432/postgres"
    except Exception:
        return None


# --- URL setup ---
DATABASE_URL_RAW = os.environ.get('DATABASE_URL', '')
DATABASE_URL_POOLER = _fix_db_port(os.environ.get('DATABASE_URL_POOL', DATABASE_URL_RAW))

# Direct URL: env var only if it's actually a direct host (not pooler session mode)
_direct_env = os.environ.get('DATABASE_URL_DIRECT', '')
if _direct_env and 'pooler.supabase.com' in _direct_env:
    logger.warning("[DB] DATABASE_URL_DIRECT contains pooler host — ignoring, will auto-derive")
    _direct_env = ''
DATABASE_URL_DIRECT = _direct_env or _build_direct_url(DATABASE_URL_POOLER)
# If primary URL is already direct (not pooler), use it for LISTEN/NOTIFY too
if not DATABASE_URL_DIRECT and DATABASE_URL_RAW and 'pooler.supabase.com' not in DATABASE_URL_RAW:
    DATABASE_URL_DIRECT = _fix_db_port(DATABASE_URL_RAW)
if DATABASE_URL_DIRECT:
    DATABASE_URL_DIRECT = _fix_db_port(DATABASE_URL_DIRECT)

# Preferred URL: direct if available, pooler as fallback
DATABASE_URL = DATABASE_URL_DIRECT or DATABASE_URL_POOLER
_using_direct = bool(DATABASE_URL_DIRECT and DATABASE_URL == DATABASE_URL_DIRECT)
logger.info(f"[DB] Primary: {'DIRECT' if _using_direct else 'POOLER'}")

_db_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()

# Track connection ages for recycling stale connections
_conn_created_at: Dict[int, float] = {}
_conn_age_lock = threading.Lock()
MAX_CONN_AGE = 300  # Recycle connections older than 5 minutes


_CONNECT_KWARGS = dict(
    connect_timeout=10,
    keepalives=1,
    keepalives_idle=30,
    keepalives_interval=10,
    keepalives_count=3,
    options='-c statement_timeout=10000',  # 10s — prevents queries from hanging forever
)


def get_db_pool() -> Optional[psycopg2.pool.ThreadedConnectionPool]:
    """Lazy init database pool. Try direct first, fall back to pooler."""
    global _db_pool
    if _db_pool is not None:
        return _db_pool

    urls_to_try = []
    if DATABASE_URL_DIRECT:
        urls_to_try.append(('DIRECT', DATABASE_URL_DIRECT))
    if DATABASE_URL_POOLER:
        urls_to_try.append(('POOLER', DATABASE_URL_POOLER))

    if not urls_to_try:
        return None

    with _pool_lock:
        if _db_pool is not None:
            return _db_pool
        for label, dsn in urls_to_try:
            try:
                _db_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2, maxconn=10, dsn=dsn, **_CONNECT_KWARGS,
                )
                logger.info(f"[DB] Pool created via {label} (minconn=2, maxconn=10)")
                return _db_pool
            except Exception as e:
                logger.warning(f"[DB] Pool creation failed ({label}): {e}")
                _db_pool = None
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
        return {'status': 'not_initialized', 'url_type': 'direct' if _using_direct else 'pooler'}
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
            'url_type': 'direct' if _using_direct else 'pooler',
        }
    except Exception as e:
        return {'error': str(e), 'url_type': 'direct' if _using_direct else 'pooler'}


def _is_conn_stale(conn) -> bool:
    """Check if connection is too old and should be recycled"""
    conn_id = id(conn)
    with _conn_age_lock:
        created = _conn_created_at.get(conn_id)
    if created is None:
        return False
    return (time.time() - created) > MAX_CONN_AGE


def _getconn_with_timeout(pool, timeout=10):
    """Get connection from pool with timeout to prevent infinite blocking."""
    result = [None]
    error = [None]
    def _get():
        try:
            result[0] = pool.getconn()
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_get, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        # Thread is stuck in getconn — connection leaked but better than hanging
        raise psycopg2.pool.PoolError(f"getconn() timed out after {timeout}s")
    if error[0]:
        raise error[0]
    return result[0]


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

    Retries only apply to connection acquisition (getconn). Once the connection
    is yielded, errors during usage are NOT retried — the caller must handle
    retries at a higher level if needed.

    IMPORTANT: A @contextmanager generator must yield exactly once. Previous
    versions had a retry loop around yield, which caused "generator didn't stop
    after throw()" when SSL dropped during yield — the except handler did
    `continue` which attempted a second yield, which @contextmanager rejects.

    Usage:
        with db_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.commit()
    """
    # Phase 1: Acquire connection (with retries)
    conn = None
    pool = None
    last_error = None

    for attempt in range(max_retries):
        pool = get_db_pool()
        if not pool:
            raise RuntimeError("Database pool not initialized")

        try:
            thread_name = threading.current_thread().name
            t0 = time.time()
            logger.info(f"[DB] getconn start (thread={thread_name}, attempt={attempt + 1})")
            conn = _getconn_with_timeout(pool)
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
                conn = _getconn_with_timeout(pool)
                _track_conn(conn)

            # Connection acquired successfully
            break

        except psycopg2.pool.PoolError as e:
            # Pool exhausted — phantom slots from hung psycopg2.connect() through Supavisor.
            # Reset pool to clear phantom _count, fresh pool starts at 0.
            last_error = e
            conn = None
            logger.warning(f"[DB] Pool exhausted (attempt {attempt + 1}/{max_retries}): {e} — resetting pool")
            _reset_pool()
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            raise

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            last_error = e
            if _is_connection_error(e):
                logger.warning(f"[DB] Connection error during acquire (attempt {attempt + 1}/{max_retries}): {e}")
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

        except Exception:
            # Unexpected error during acquisition — clean up conn if we got one
            if conn:
                _untrack_conn(conn)
                try:
                    pool.putconn(conn, close=True)
                except Exception:
                    pass
                conn = None
            raise

    if conn is None:
        if last_error:
            raise last_error
        raise RuntimeError("Database connection failed after retries")

    # Phase 2: Yield connection (no retries — single yield, always cleans up)
    try:
        yield conn
    except Exception:
        # Error during usage (SSL drop, query error, etc.)
        # Mark connection as bad so it gets closed, not returned to pool
        try:
            _untrack_conn(conn)
        except Exception:
            pass
        try:
            if pool and conn:
                pool.putconn(conn, close=True)
        except Exception:
            pass
        conn = None  # Prevent double-putconn in finally

        # Trigger pool reset for connection errors so next caller gets fresh connections
        # (but don't block — the caller's exception takes priority)
        try:
            _reset_pool()
        except Exception:
            pass

        raise
    finally:
        # Return healthy connection to pool (or clean up if something went wrong)
        if conn:
            try:
                logger.debug(f"[DB] putconn (thread={threading.current_thread().name})")
                pool.putconn(conn)
            except Exception:
                # Pool might have been reset by another thread — just discard
                try:
                    _untrack_conn(conn)
                except Exception:
                    pass


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
