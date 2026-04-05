"""PostgreSQL LISTEN/NOTIFY listener -- forwards notifications to Redis queue."""

__all__ = ['_pg_connect_with_timeout', 'pg_notify_listener_worker']

import time
import logging
import threading
import select

import psycopg2
import psycopg2.extensions
import redis as redis_lib

import shared
from shared import _shutdown_event, _health_state

logger = logging.getLogger(__name__)


def _pg_connect_with_timeout(database_url: str, timeout: int = 15):
    """Connect to PostgreSQL with a total timeout (including handshake).

    connect_timeout only covers TCP SYN/ACK -- the PG protocol handshake
    (auth, SSL, params) can hang forever if the pooler is stuck.
    We use a background thread to enforce a hard total timeout.
    """
    result = [None]
    error = [None]

    def _connect():
        try:
            result[0] = psycopg2.connect(
                database_url,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=10,
                keepalives_interval=5,
                keepalives_count=3,
                options='-c statement_timeout=0'
            )
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_connect, daemon=True)
    t.start()
    t.join(timeout=timeout)

    if t.is_alive():
        raise psycopg2.OperationalError(
            f"Connection timed out after {timeout}s (handshake stuck)"
        )

    if error[0]:
        raise error[0]

    return result[0]


def pg_notify_listener_worker(redis_url: str, database_url: str):
    """Listen for PostgreSQL NOTIFY events and forward to Redis queue.

    With auto-reconnect, keepalive, and hard timeouts.
    Uses direct connection (port 5432) -- LISTEN/NOTIFY does not work through Supavisor.
    """
    logger.info("[LISTENER] Starting pg_notify listener worker...")

    conn = None
    cur = None
    local_redis_client = None
    queue_key = 'telegram:send_queue'
    last_keepalive = time.time()
    KEEPALIVE_INTERVAL = 15
    reconnect_backoff = 5
    MAX_RECONNECT_BACKOFF = 30

    while not _shutdown_event.is_set():
        try:
            # ===== CONNECT TO POSTGRES =====
            if conn is None or conn.closed:
                shared._listener_degraded = True
                logger.info("[LISTENER] Connecting to Postgres...")
                conn = _pg_connect_with_timeout(database_url, timeout=15)
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute("LISTEN telegram_send;")
                last_keepalive = time.time()
                shared._listener_degraded = False
                reconnect_backoff = 5
                logger.info("[LISTENER] Listening on 'telegram_send'")

            # ===== CONNECT TO REDIS =====
            if local_redis_client is None:
                try:
                    local_redis_client = redis_lib.from_url(redis_url, decode_responses=True)
                    local_redis_client.ping()
                    logger.info("[LISTENER] Redis connected")
                except Exception as e:
                    logger.warning(f"[LISTENER] Redis not ready: {e}, retrying in 5s...")
                    _shutdown_event.wait(5)
                    continue

            # ===== LISTEN FOR NOTIFICATIONS =====
            try:
                fd = conn.fileno()
                if fd < 0:
                    raise ValueError(f"Invalid file descriptor: {fd}")
            except (ValueError, AttributeError) as e:
                logger.warning(f"[LISTENER] Connection fd invalid: {e}, reconnecting...")
                conn = None
                continue

            select_result = select.select([conn], [], [], 15)

            # ===== ALWAYS CHECK CONNECTION HEALTH =====
            now = time.time()
            if now - last_keepalive >= KEEPALIVE_INTERVAL:
                try:
                    cur.execute("SET statement_timeout = '5s'")
                    cur.execute("SELECT 1")
                    cur.fetchone()
                    cur.execute("SET statement_timeout = '0'")
                    last_keepalive = now
                    _health_state['pg_listener_last_activity'] = now
                except Exception as e:
                    logger.warning(f"[LISTENER] Keepalive failed: {e}, reconnecting...")
                    raise

            if select_result == ([], [], []):
                continue

            # ===== POLL FOR NOTIFICATIONS =====
            try:
                poll_ready = select.select([conn], [], [], 5)
                if not poll_ready[0]:
                    raise Exception("Connection stale: select returned ready but poll timed out")
                conn.poll()
            except Exception as e:
                logger.error(f"[LISTENER] conn.poll() failed: {e}")
                raise

            if conn.closed:
                logger.warning("[LISTENER] Connection closed after poll, reconnecting...")
                conn = None
                continue

            # ===== PROCESS ALL NOTIFICATIONS =====
            notify_count = len(conn.notifies)
            if notify_count > 0:
                logger.info(f"[LISTENER] Received {notify_count} notifications")

            processed = 0
            while conn.notifies:
                notify = conn.notifies.pop(0)
                task_json = notify.payload

                try:
                    local_redis_client.rpush(queue_key, task_json)
                    processed += 1
                    logger.info(f"[LISTENER] Pushed to Redis [{processed}/{notify_count}]: {task_json[:100]}...")
                except Exception as e:
                    logger.error(f"[LISTENER] Redis push error: {e}")
                    local_redis_client = None
                    break

            if processed > 0:
                logger.info(f"[LISTENER] Total pushed: {processed}/{notify_count}")
                _health_state['pg_listener_last_activity'] = time.time()

        except Exception as e:
            shared._listener_degraded = True
            logger.exception(f"[LISTENER] Error: {e}")

            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
                cur = None

            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None

            if local_redis_client:
                try:
                    local_redis_client.close()
                except Exception:
                    pass
                local_redis_client = None

            logger.info(f"[LISTENER] Waiting {reconnect_backoff}s before reconnect...")
            _shutdown_event.wait(reconnect_backoff)
            reconnect_backoff = min(reconnect_backoff * 2, MAX_RECONNECT_BACKOFF)

    logger.info("[LISTENER] Stopped (shutdown)")
