"""Stuck message retry -- finds queued messages that failed to send and re-queues them."""

import time
import json
import logging

import shared
from shared import _shutdown_event, _retry_stop_event
from db import db_connection

logger = logging.getLogger(__name__)


def retry_stuck_messages(max_age_minutes: int = 30, force_send: bool = False):
    """Find and re-queue stuck messages (status='queued' but no tg_id).

    When listener is degraded, picks up messages after 5 seconds instead of 60.
    force_send: if True, adds force_send flag to bypass sender dedup.
    """
    from redis_client import get_redis

    # In degraded mode, pick up messages much faster (5s vs 60s)
    min_age_seconds = 5 if shared._listener_degraded else 60

    try:
        t_conn = time.time()
        with db_connection() as conn:
            t_got_conn = time.time()
            logger.info(f"[RETRY] Got DB connection in {t_got_conn - t_conn:.1f}s")
            cur = conn.cursor()

            t_query = time.time()
            cur.execute("""
                SELECT h.id, h.chat_id, h.message, h.type_of_message, h.reply_to,
                       h.media_id, h.files_url, h.files_path,
                       c.type as chat_type, c.group_id, u.telegram_id,
                       h.thought_sessions_id
                FROM chat_history_tg h
                JOIN chats_tg c ON c.id = h.chat_id
                LEFT JOIN users_tg u ON u.id = c.user_id
                WHERE h.type = 'AEON'
                  AND h.status IN ('queued', 'failed')
                  AND h.tg_id IS NULL
                  AND h.created_at < NOW() - INTERVAL '1 second' * %s
                  AND h.created_at > NOW() - INTERVAL '%s minutes'
                ORDER BY h.created_at ASC
                LIMIT 100
            """, (min_age_seconds, max_age_minutes,))

            rows = cur.fetchall()
            t_fetched = time.time()
            logger.info(f"[RETRY] Query took {t_fetched - t_query:.1f}s, found {len(rows)} rows")
            if not rows:
                cur.close()
                return 0

            requeued = 0
            redis_client_local = get_redis()
            if not redis_client_local:
                logger.error("[RETRY] Redis not available")
                cur.close()
                return 0
            queue_key = 'telegram:send_queue'

            for row in rows:
                msg_id, chat_id, message, type_of_message, reply_to, media_id, files_url, files_path, chat_type, group_id, telegram_id, thought_sessions_id = row

                if chat_type == 'group':
                    if not group_id:
                        continue
                    chat_ident = str(-group_id)
                else:
                    if not telegram_id:
                        continue
                    chat_ident = str(telegram_id)

                if type_of_message in ('video_note', 'video', 'photo', 'document') and files_url:
                    request_body = {
                        'chat_id': chat_ident,
                        'type_of_message': type_of_message,
                        'file_url': files_url[0] if files_url else None
                    }
                    if message:
                        request_body['caption'] = message
                else:
                    request_body = {
                        'chat_id': chat_ident,
                        'type_of_message': type_of_message,
                        'message': message
                    }

                if reply_to:
                    cur.execute("""
                        SELECT tg_id FROM chat_history_tg
                        WHERE (id = %s OR tg_id = %s) AND chat_id = %s AND tg_id IS NOT NULL
                        LIMIT 1
                    """, (reply_to, reply_to, chat_id))
                    reply_row = cur.fetchone()
                    if reply_row and reply_row[0]:
                        request_body['reply_to'] = reply_row[0]

                task = {
                    'chat_history_id': msg_id,
                    'chat_ident': chat_ident,
                    'request_body': request_body,
                    'thought_sessions_id': thought_sessions_id,
                    'queued_at': time.time(),
                    'retry': True
                }
                if force_send:
                    task['force_send'] = True
                task_json = json.dumps(task)

                redis_client_local.rpush(queue_key, task_json)
                requeued += 1

            cur.close()
            logger.info(f"Re-queued {requeued} stuck messages")
            return requeued

    except Exception as e:
        logger.exception(f"[RETRY] retry_stuck_messages error: {e}")
        return 0


def stuck_messages_retry_worker():
    """Safety net: periodically check for and retry stuck messages.

    Adaptive: polls every 10s when LISTEN is degraded, every 60s normally.
    """
    NORMAL_INTERVAL = 60
    DEGRADED_INTERVAL = 10
    logger.info(f"[RETRY] Starting stuck messages retry worker (normal={NORMAL_INTERVAL}s, degraded={DEGRADED_INTERVAL}s)")

    # Wait 30 seconds on startup before first check
    for _ in range(60):  # 30s in 0.5s steps
        if _shutdown_event.is_set() or _retry_stop_event.is_set():
            break
        time.sleep(0.5)

    while not _shutdown_event.is_set() and not _retry_stop_event.is_set():
        try:
            is_degraded = shared._listener_degraded
            count = retry_stuck_messages(max_age_minutes=240)
            if count > 0:
                logger.info(f"[RETRY] Recovered {count} stuck messages" + (" (degraded mode)" if is_degraded else ""))
        except Exception as e:
            logger.error(f"[RETRY] Worker error: {e}")

        interval = DEGRADED_INTERVAL if shared._listener_degraded else NORMAL_INTERVAL
        for _ in range(int(interval * 10)):
            if _shutdown_event.is_set() or _retry_stop_event.is_set():
                break
            time.sleep(0.1)

    reason = "stop_event" if _retry_stop_event.is_set() else "shutdown"
    logger.info(f"[RETRY] Stopped ({reason})")
