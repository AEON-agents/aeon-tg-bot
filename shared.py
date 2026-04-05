"""Shared globals, config, and event loop helpers -- LEAF MODULE.

Every other new module imports from here. This module only imports
from external packages and the existing ``db`` module.
"""

__all__ = [
    # Config
    'BOT_TOKEN', 'DATABASE_URL', 'DATABASE_URL_DIRECT', 'REDIS_URL',
    'WEBHOOK_PATH', 'WEBHOOK_SECRET', 'BASE_URL', 'N8N_WEBHOOK_URL',
    # Globals
    'bot', 'dp', 'router', 'flask_app', 'db_pool', 'sender_bot', 'redis_client',
    '_main_loop', '_loop_thread', 'pg_listener_thread', 'retry_worker_thread',
    'incoming_consumer_thread',
    # Threading state
    '_shutdown_event', '_health_state', '_listener_degraded',
    '_consumer_stop_event', '_retry_stop_event',
    # Media group state
    'media_group_buffer', 'media_group_flushed', 'media_group_lock',
    # Constants
    'INCOMING_QUEUE_KEY', 'TEMP_FILES_DIR',
    # Functions
    '_run_loop_forever', 'get_main_loop', 'run_async',
    # Rate limiter
    '_rate_limit_counters', '_rate_limit_lock', '_check_rate_limit',
    '_queue_task', '_resolve_tg_id',
    # Watchdog cooldown
    '_thread_restart_times', '_check_cooldown',
    'RESTART_COOLDOWN_WINDOW', 'MAX_RESTARTS_IN_WINDOW',
    # Placeholder
    '_ensure_initialized',
    # db re-export for backward compat
    'db_cursor',
    # logger
    'logger',
]

import os
import sys
import asyncio
import logging
import time
import json
import tempfile
import threading
from typing import Optional, Dict
from collections import defaultdict

from flask import Flask

# Database access layer
from db import DATABASE_URL, DATABASE_URL_DIRECT, db_cursor

# ============== LOGGING ==============
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ============== CONFIG ==============
BOT_TOKEN = os.environ.get('BOT_TOKEN')
REDIS_URL = os.environ.get('REDIS_URL')
WEBHOOK_PATH = os.environ.get('WEBHOOK_PATH', '/webhook/telegram')
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', '')
BASE_URL = os.environ.get('BASE_URL', '')
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL', '')

# ============== GLOBAL VARIABLES ==============
bot = None
dp = None
router = None
flask_app = Flask(__name__)
db_pool = None
sender_bot = None
redis_client = None
_main_loop = None
_loop_thread = None
pg_listener_thread = None
retry_worker_thread = None
incoming_consumer_thread = None

# Graceful shutdown flag — all background threads check this
_shutdown_event = threading.Event()

# Health state — tracks last activity timestamps for critical threads
_health_state = {
    'pg_listener_last_activity': time.time(),
    'consumer_last_activity': time.time(),
    'consumer_heartbeat': time.time(),
}

# Listener degraded flag — when True, retry worker polls aggressively
_listener_degraded = False

# Per-thread stop events — watchdog sets these to signal individual threads to exit
_consumer_stop_event = threading.Event()
_retry_stop_event = threading.Event()

# Media group buffer for collecting album messages
media_group_buffer: Dict[str, Dict] = {}
media_group_lock = asyncio.Lock() if asyncio.get_event_loop_policy() else None

# Track recently flushed media groups to handle late arrivals
media_group_flushed: Dict[str, Dict] = {}

# Queue key for incoming messages from receiver
INCOMING_QUEUE_KEY = 'telegram:incoming_queue'

# Temp files storage for voice/media download
TEMP_FILES_DIR = tempfile.gettempdir()


# ============== EVENT LOOP HELPERS ==============

def _run_loop_forever(loop):
    """Run event loop in dedicated thread"""
    asyncio.set_event_loop(loop)
    loop.run_forever()


def get_main_loop():
    """Get the main event loop running in dedicated thread"""
    import shared
    if shared._main_loop is None or shared._main_loop.is_closed():
        shared._main_loop = asyncio.new_event_loop()
        shared._loop_thread = threading.Thread(
            target=_run_loop_forever, args=(shared._main_loop,), daemon=True
        )
        shared._loop_thread.start()
        logger.info("Main event loop started in dedicated thread")
    return shared._main_loop


def run_async(coro, timeout=30):
    """Run coroutine in main loop from sync context (thread-safe).
    Cancels the future on timeout to prevent zombie coroutines.
    """
    loop = get_main_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout=timeout)
    except TimeoutError:
        future.cancel()
        raise


# ============== RATE LIMITER & QUEUE HELPERS ==============

_rate_limit_counters = defaultdict(list)
_rate_limit_lock = threading.Lock()


def _check_rate_limit(key: str, max_per_minute: int = 30) -> bool:
    """Check if request is within rate limit. Returns True if allowed."""
    now = time.time()
    with _rate_limit_lock:
        timestamps = _rate_limit_counters[key]
        _rate_limit_counters[key] = [t for t in timestamps if now - t < 60]
        if len(_rate_limit_counters[key]) >= max_per_minute:
            return False
        _rate_limit_counters[key].append(now)
        return True


def _queue_task(telegram_id, type_of_message: str, body: dict):
    """Push a task to the send queue for async processing by SenderBot."""
    from redis_client import get_redis
    client = get_redis()
    if not client:
        raise RuntimeError("Redis not available")

    task = {
        'chat_ident': str(telegram_id),
        'request_body': {
            'chat_id': str(telegram_id),
            'type_of_message': type_of_message,
            **body
        },
        'queued_at': time.time(),
    }
    client.rpush('telegram:send_queue', json.dumps(task))
    return True


def _resolve_tg_id(data):
    """Resolve telegram_id from request data. Returns telegram_id or None."""
    from db_helpers import resolve_telegram_chat_id
    telegram_id = data.get('telegram_id')
    if not telegram_id:
        chat_id = data.get('chat_id')
        if chat_id:
            telegram_id = resolve_telegram_chat_id(chat_id)
    return telegram_id


# ============== WATCHDOG COOLDOWN ==============

_thread_restart_times = {
    'pg_listener': [],
    'retry_worker': [],
    'incoming_consumer': [],
    'sender': [],
}
RESTART_COOLDOWN_WINDOW = 300   # 5 minutes
MAX_RESTARTS_IN_WINDOW = 3


def _check_cooldown(thread_name):
    """Returns True if restart is allowed, False if cooldown exceeded."""
    now = time.time()
    times = _thread_restart_times[thread_name]
    times[:] = [t for t in times if now - t < RESTART_COOLDOWN_WINDOW]
    if len(times) >= MAX_RESTARTS_IN_WINDOW:
        return False
    times.append(now)
    return True


# ============== PLACEHOLDER FOR _ensure_initialized ==============
# Set by app.py after it defines the real function.
_ensure_initialized = None
