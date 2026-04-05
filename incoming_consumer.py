"""Incoming message consumer -- reads from Redis queue (from receiver service)."""

import time
import json
import logging

from aiogram.types import Update
import psycopg2.pool

from shared import (_shutdown_event, _consumer_stop_event, _health_state,
                    run_async, INCOMING_QUEUE_KEY)
import shared

logger = logging.getLogger(__name__)


def incoming_message_consumer():
    """Consume incoming messages from Redis queue (from receiver service).

    Recovery strategy:
    - Backoff capped at 5s (not 60s) -- messages pile up fast
    - After 3 consecutive errors, reset DB pool proactively
    - Track last success time for watchdog monitoring
    """
    from redis_client import get_redis

    logger.info("[CONSUMER] Starting incoming message consumer...")
    client = get_redis()

    if not client:
        logger.error("[CONSUMER] Redis not available, incoming consumer not started")
        return

    consecutive_errors = 0
    _health_state['consumer_last_activity'] = time.time()

    while not _shutdown_event.is_set() and not _consumer_stop_event.is_set():
        try:
            # Blocking pop with 5 second timeout
            result = client.blpop(INCOMING_QUEUE_KEY, timeout=5)
            _health_state['consumer_heartbeat'] = time.time()
            if result:
                _health_state['consumer_last_activity'] = time.time()
                _, update_json = result
                try:
                    update_data = json.loads(update_json)

                    # Process through aiogram dispatcher
                    update = Update(**update_data)
                    run_async(shared.dp.feed_update(shared.bot, update))

                    update_id = update_data.get('update_id', 'unknown')
                    logger.info(f"[CONSUMER] Processed incoming update: {update_id}")
                    consecutive_errors = 0
                    _health_state['consumer_last_activity'] = time.time()
                except json.JSONDecodeError as e:
                    logger.error(f"[CONSUMER] Invalid JSON in queue: {e}")
                except Exception as e:
                    # Re-push message back to queue with retry counter (max 3)
                    try:
                        retry_data = json.loads(update_json) if isinstance(update_json, (str, bytes)) else {}
                        retry_count = retry_data.get('_consumer_retry', 0)
                        if retry_count < 3:
                            retry_data['_consumer_retry'] = retry_count + 1
                            client.rpush(INCOMING_QUEUE_KEY, json.dumps(retry_data))
                            logger.warning(f"[CONSUMER] Re-queued message (retry {retry_count + 1}/3): {type(e).__name__}: {e}")
                        else:
                            logger.error(f"[CONSUMER] Message dropped after 3 retries: {type(e).__name__}: {e}")
                    except Exception:
                        pass
                    consecutive_errors += 1

        except psycopg2.pool.PoolError as e:
            consecutive_errors += 1
            logger.warning(f"[CONSUMER] Pool exhausted (#{consecutive_errors}): {e}, backing off 3s")
            for _ in range(30):
                if _shutdown_event.is_set() or _consumer_stop_event.is_set():
                    break
                time.sleep(0.1)
        except Exception as e:
            consecutive_errors += 1
            backoff = min(2 ** consecutive_errors, 5)
            if consecutive_errors == 1 or consecutive_errors % 5 == 0:
                logger.error(f"[CONSUMER] Error (#{consecutive_errors}): {type(e).__name__}: {e}")

            if consecutive_errors == 3:
                try:
                    from db import _reset_pool as reset_shared_pool
                    reset_shared_pool()
                    logger.info("[CONSUMER] Reset DB pool after 3 consecutive errors")
                except Exception:
                    pass

            if consecutive_errors == 10:
                try:
                    client = get_redis()
                    logger.info("[CONSUMER] Reconnected Redis after 10 consecutive errors")
                except Exception:
                    pass

            for _ in range(int(backoff * 10)):
                if _shutdown_event.is_set() or _consumer_stop_event.is_set():
                    break
                time.sleep(0.1)

    reason = "stop_event" if _consumer_stop_event.is_set() else "shutdown"
    logger.info(f"[CONSUMER] Stopped ({reason})")
