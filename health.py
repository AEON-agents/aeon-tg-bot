"""Health check endpoint."""

import time
import logging

from flask import jsonify

import shared
from shared import flask_app
from db import db_cursor, get_pool_stats

logger = logging.getLogger(__name__)


@flask_app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint -- returns 503 if critical components are dead"""
    import shutil
    has_ffmpeg = shutil.which("ffmpeg") is not None

    # Check Redis connectivity
    redis_ok = False
    try:
        if shared.redis_client:
            redis_ok = shared.redis_client.ping()
    except Exception:
        pass

    # Check DB connectivity
    db_ok = False
    try:
        with db_cursor() as cur:
            cur.execute("SELECT 1")
            db_ok = True
    except Exception:
        pass

    bot_ok = shared.bot is not None
    sender_ok = shared.sender_bot.is_running if shared.sender_bot else False
    pg_listener_alive = shared.pg_listener_thread.is_alive() if shared.pg_listener_thread else False
    retry_ok = shared.retry_worker_thread.is_alive() if shared.retry_worker_thread else False
    consumer_ok = shared.incoming_consumer_thread.is_alive() if shared.incoming_consumer_thread else False

    # Queue length
    queue_len = 0
    try:
        if shared.redis_client:
            queue_len = shared.redis_client.llen('telegram:send_queue')
    except Exception:
        pass

    # Pool stats
    pool_stats = get_pool_stats()

    now = time.time()
    consumer_hb = shared._health_state.get('consumer_heartbeat', 0)
    consumer_act = shared._health_state.get('consumer_last_activity', 0)
    listener_act = shared._health_state.get('pg_listener_last_activity', 0)

    status_data = {
        "status": "ok",
        "bot_initialized": bot_ok,
        "sender_running": sender_ok,
        "redis_connected": redis_ok,
        "db_connected": db_ok,
        "db_pool": pool_stats,
        "pg_listener_alive": pg_listener_alive,
        "pg_listener_degraded": shared._listener_degraded,
        "pg_listener_last_activity_ago": round(now - listener_act) if listener_act else None,
        "stuck_retry_alive": retry_ok,
        "incoming_consumer_alive": consumer_ok,
        "consumer_heartbeat_ago": round(now - consumer_hb) if consumer_hb else None,
        "consumer_last_activity_ago": round(now - consumer_act) if consumer_act else None,
        "queue_length": queue_len,
        "ffmpeg_available": has_ffmpeg,
    }

    # LISTENER health
    listener_age = now - listener_act if listener_act else None
    listener_critical = False
    if listener_age is not None and listener_age > 180:
        listener_critical = True
    elif shared._listener_degraded and listener_age is not None and listener_age > 180:
        listener_critical = True

    status_data["pg_listener_critical"] = listener_critical

    # Return 503 if core components are down
    critical_ok = bot_ok and redis_ok
    if not critical_ok or listener_critical:
        status_data["status"] = "unhealthy"
        return jsonify(status_data), 503
    elif not (sender_ok and db_ok):
        status_data["status"] = "degraded"

    return jsonify(status_data)
