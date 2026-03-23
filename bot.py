"""
AEON Telegram Bot - aiogram 3.x + Flask
Full API Migration from MTProto to Bot API
All endpoints work with telegram_id directly

Features:
- Send/receive messages
- Send video notes (circles)
- Voice message download
- File/media download
- Send documents
- Set reactions
- Chat actions (typing)
- Send stickers
"""

import os
import asyncio
import logging
import time
import json
import base64
import tempfile
from datetime import datetime
from typing import Optional, Dict, Any
from io import BytesIO

# aiogram
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, Update, ChatMemberUpdated,
    ContentType, ReactionTypeEmoji, FSInputFile, BufferedInputFile
)
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType, ChatAction

# Flask for webhooks + API
from flask import Flask, request, jsonify, send_file
import threading
import signal

# Database
import psycopg2
import psycopg2.pool
import psycopg2.extensions
import select
import redis as redis_lib

# Redis sender
from sender_bot import SenderBot

# Database access layer
from db import db_cursor, db_connection, get_db_pool

# ============== CONFIG ==============
BOT_TOKEN = os.environ.get('BOT_TOKEN')
DATABASE_URL = os.environ.get('DATABASE_URL')
DATABASE_URL_DIRECT = os.environ.get('DATABASE_URL_DIRECT', DATABASE_URL)  # Direct connection for LISTEN/NOTIFY
REDIS_URL = os.environ.get('REDIS_URL')
WEBHOOK_PATH = os.environ.get('WEBHOOK_PATH', '/webhook/telegram')
WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', '')
BASE_URL = os.environ.get('BASE_URL', '')
N8N_WEBHOOK_URL = os.environ.get('N8N_WEBHOOK_URL', '')

# ============== LOGGING ==============
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# ============== GLOBAL VARIABLES ==============
bot: Optional[Bot] = None
dp: Optional[Dispatcher] = None
router: Optional[Router] = None
flask_app = Flask(__name__)
db_pool = None
sender_bot: Optional[SenderBot] = None
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
    'consumer_heartbeat': time.time(),  # Updates every BLPOP cycle (even idle) — proves thread is alive
}

# Listener degraded flag — when True, retry worker polls aggressively
_listener_degraded = False

# Per-thread stop events — watchdog sets these to signal individual threads to exit
_consumer_stop_event = threading.Event()
_retry_stop_event = threading.Event()

# Media group buffer for collecting album messages
# Key: media_group_id, Value: {messages: [], chat_id: int, first_msg_time: float}
media_group_buffer: Dict[str, Dict] = {}
media_group_lock = asyncio.Lock() if asyncio.get_event_loop_policy() else None

# Track recently flushed media groups to handle late arrivals
# Key: media_group_id, Value: {history_id: int, flushed_at: float}
media_group_flushed: Dict[str, Dict] = {}

# Queue key for incoming messages from receiver
INCOMING_QUEUE_KEY = 'telegram:incoming_queue'

# Temp files storage for voice/media download
TEMP_FILES_DIR = tempfile.gettempdir()


def _run_loop_forever(loop):
    """Run event loop in dedicated thread"""
    asyncio.set_event_loop(loop)
    loop.run_forever()


def get_main_loop():
    """Get the main event loop running in dedicated thread"""
    global _main_loop, _loop_thread
    if _main_loop is None or _main_loop.is_closed():
        _main_loop = asyncio.new_event_loop()
        _loop_thread = threading.Thread(target=_run_loop_forever, args=(_main_loop,), daemon=True)
        _loop_thread.start()
        logger.info("✅ Main event loop started in dedicated thread")
    return _main_loop


def run_async(coro, timeout=30):
    """Run coroutine in main loop from sync context (thread-safe).
    Cancels the future on timeout to prevent zombie coroutines from piling up on the event loop.
    """
    loop = get_main_loop()
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    try:
        return future.result(timeout=timeout)
    except TimeoutError:
        future.cancel()
        raise


# ============== DATABASE HELPERS ==============

def get_or_create_user(telegram_id: int, first_name: str, last_name: str = None,
                       username: str = None, is_bot: bool = False) -> int:
    """Get or create user in users_tg, returns internal user id"""
    with db_cursor(commit=True) as cur:
        cur.execute("SELECT id FROM users_tg WHERE telegram_id = %s", (telegram_id,))
        row = cur.fetchone()

        if row:
            user_id = row[0]
            cur.execute("""
                UPDATE users_tg
                SET name = %s, last_name = %s, username = %s, is_bot = %s
                WHERE telegram_id = %s
            """, (first_name, last_name, username, is_bot, telegram_id))
        else:
            cur.execute("""
                INSERT INTO users_tg (telegram_id, name, last_name, username, is_bot, status)
                VALUES (%s, %s, %s, %s, %s, 'unblocked')
                RETURNING id
            """, (telegram_id, first_name, last_name, username, is_bot))
            user_id = cur.fetchone()[0]
            logger.info(f"Created new user: {telegram_id} -> id={user_id}")

        return user_id


def get_or_create_chat(chat_type: str, user_id: int = None, group_id: int = None) -> int:
    """Get or create chat in chats_tg, returns internal chat id"""
    with db_cursor(commit=True) as cur:
        if chat_type == 'user':
            cur.execute(
                "SELECT id FROM chats_tg WHERE type = 'user' AND user_id = %s",
                (user_id,)
            )
        else:
            cur.execute(
                "SELECT id FROM chats_tg WHERE type = 'group' AND group_id = %s",
                (group_id,)
            )

        row = cur.fetchone()

        if row:
            chat_id = row[0]
        else:
            if chat_type == 'user':
                cur.execute("""
                    INSERT INTO chats_tg (type, user_id)
                    VALUES ('user', %s)
                    RETURNING id
                """, (user_id,))
            else:
                cur.execute("""
                    INSERT INTO chats_tg (type, group_id)
                    VALUES ('group', %s)
                    RETURNING id
                """, (group_id,))

            chat_id = cur.fetchone()[0]
            logger.info(f"Created new chat: type={chat_type}, id={chat_id}")

        return chat_id


def get_or_create_group(telegram_group_id: int, title: str = None) -> int:
    """
    Get or create group in groups_tg.

    Args:
        telegram_group_id: Telegram group ID (positive, without minus sign)
        title: Group title

    Returns:
        telegram_group_id (NOT internal id!) for use in chats_tg.group_id
    """
    with db_cursor(commit=True) as cur:
        cur.execute(
            "SELECT id FROM groups_tg WHERE telegram_group_id = %s",
            (telegram_group_id,)
        )
        row = cur.fetchone()

        if row:
            if title:
                cur.execute(
                    "UPDATE groups_tg SET title = %s WHERE telegram_group_id = %s",
                    (title, telegram_group_id)
                )
        else:
            cur.execute("""
                INSERT INTO groups_tg (telegram_group_id, title)
                VALUES (%s, %s)
                RETURNING id
            """, (telegram_group_id, title))
            logger.info(f"Created new group: telegram_group_id={telegram_group_id}")

        return telegram_group_id


def add_user_to_group(telegram_group_id: int, user_id: int, role: str = 'member') -> bool:
    """
    Add user to group in groups_users_tg (if not exists).

    Args:
        telegram_group_id: Telegram group ID (positive, without minus sign)
        user_id: Internal user ID from users_tg.id
        role: User role in group (default: 'member')

    Returns:
        True if added or already exists
    """
    try:
        with db_cursor(commit=True) as cur:
            cur.execute(
                "SELECT id FROM groups_users_tg WHERE chat_id = %s AND user_id = %s",
                (telegram_group_id, user_id)
            )
            row = cur.fetchone()

            if not row:
                cur.execute("""
                    INSERT INTO groups_users_tg (chat_id, user_id, role, status)
                    VALUES (%s, %s, %s, 'active')
                """, (telegram_group_id, user_id, role))
                logger.debug(f"Added user {user_id} to group {telegram_group_id}")

            return True
    except Exception as e:
        logger.error(f"Error adding user to group: {e}")
        return False


def save_message_to_db(
    chat_id: int,
    message_text: str,
    msg_type: str,
    tg_id: int,
    type_of_message: str = 'text',
    group_sender_id: int = None,
    reply_to: int = None,
    files_path: list = None
) -> int:
    """Save message to chat_history_tg, returns chat_history_tg.id

    files_path: list of file_id strings (Telegram file IDs)
    """
    with db_cursor(commit=True) as cur:
        cur.execute("""
            INSERT INTO chat_history_tg
            (chat_id, message, type, tg_id, type_of_message, group_sender_id, reply_to, files_path, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'unread')
            ON CONFLICT (chat_id, tg_id) DO UPDATE SET
                message = EXCLUDED.message,
                type_of_message = EXCLUDED.type_of_message,
                group_sender_id = EXCLUDED.group_sender_id,
                files_path = EXCLUDED.files_path
            RETURNING id
        """, (
            chat_id, message_text, msg_type, tg_id, type_of_message,
            group_sender_id, reply_to, files_path
        ))

        history_id = cur.fetchone()[0]
        files_count = len(files_path) if files_path else 0
        logger.info(f"Saved message: chat={chat_id}, tg_id={tg_id}, type={type_of_message}, files={files_count}")
        return history_id


## save_document_to_db REMOVED — documents_tg table no longer used
## Documents are identified by file_id in chat_history_tg.files_path
## and can be downloaded via Telegram Bot API using /telegram/file/download


def resolve_telegram_chat_id(internal_chat_id: int) -> Optional[int]:
    """
    Resolve internal chat_id to Telegram chat_id.
    Returns telegram_id for users or -group_id for groups.

    Note: chats_tg.group_id stores telegram_group_id (not internal groups_tg.id),
    so we use it directly with negation, same as the PG trigger does.
    """
    with db_cursor() as cur:
        cur.execute("""
            SELECT
                c.type,
                CASE
                    WHEN c.type = 'group' THEN -c.group_id
                    WHEN c.type = 'user' THEN u.telegram_id
                END as telegram_id
            FROM chats_tg c
            LEFT JOIN users_tg u ON u.id = c.user_id
            WHERE c.id = %s
        """, (internal_chat_id,))

        row = cur.fetchone()
        if row:
            return row[1]
        return None


def get_chat_info_by_telegram_id(telegram_id: int) -> Optional[Dict]:
    """Get chat info by telegram_id (user) or group_id"""
    with db_cursor() as cur:
        # First try as user
        cur.execute("""
            SELECT c.id, c.type, u.id as user_id, u.telegram_id
            FROM chats_tg c
            JOIN users_tg u ON u.id = c.user_id
            WHERE u.telegram_id = %s AND c.type = 'user'
        """, (telegram_id,))
        row = cur.fetchone()

        if row:
            return {
                'chat_id': row[0],
                'type': row[1],
                'user_id': row[2],
                'telegram_id': row[3]
            }

        # Try as group
        cur.execute("""
            SELECT c.id, c.type, g.id as group_id, g.telegram_group_id
            FROM chats_tg c
            JOIN groups_tg g ON g.id = c.group_id
            WHERE g.telegram_group_id = %s AND c.type = 'group'
        """, (telegram_id,))
        row = cur.fetchone()

        if row:
            return {
                'chat_id': row[0],
                'type': row[1],
                'group_id': row[2],
                'telegram_group_id': row[3]
            }

        return None


# ============== AIOGRAM MESSAGE HANDLERS ==============

def setup_handlers():
    """Setup aiogram message handlers"""
    global router
    router = Router()
    
    @router.message(CommandStart())
    async def handle_start(message: Message):
        """Handle /start command"""
        user = message.from_user
        internal_user_id = await asyncio.to_thread(
            get_or_create_user,
            telegram_id=user.id,
            first_name=user.first_name,
            last_name=user.last_name,
            username=user.username,
            is_bot=user.is_bot
        )
        internal_chat_id = await asyncio.to_thread(
            get_or_create_chat, 'user', user_id=internal_user_id
        )

        # Save the /start message
        await asyncio.to_thread(
            save_message_to_db,
            chat_id=internal_chat_id,
            message_text='/start',
            msg_type='user',
            tg_id=message.message_id,
            type_of_message='text'
        )

        await message.answer("Привет! Я AEON. Чем могу помочь?")
    
    @router.message(F.content_type == ContentType.TEXT)
    async def handle_text_message(message: Message):
        """Handle incoming text messages"""
        await process_incoming_message(message, 'text')
    
    @router.message(F.content_type == ContentType.VOICE)
    async def handle_voice_message(message: Message):
        """Handle incoming voice messages"""
        await process_incoming_message(message, 'voice')
    
    @router.message(F.content_type == ContentType.VIDEO_NOTE)
    async def handle_video_note(message: Message):
        """Handle incoming video notes (circles)"""
        await process_incoming_message(message, 'video_note')
    
    @router.message(F.content_type == ContentType.PHOTO)
    async def handle_photo(message: Message):
        """Handle incoming photos"""
        await process_incoming_message(message, 'photo')
    
    @router.message(F.content_type == ContentType.DOCUMENT)
    async def handle_document(message: Message):
        """Handle incoming documents"""
        await process_incoming_message(message, 'document')
    
    @router.message(F.content_type == ContentType.STICKER)
    async def handle_sticker(message: Message):
        """Handle incoming stickers"""
        await process_incoming_message(message, 'sticker')
    
    @router.message(F.content_type == ContentType.VIDEO)
    async def handle_video(message: Message):
        """Handle incoming videos"""
        await process_incoming_message(message, 'video')
    
    return router


async def flush_media_group(group_id: str):
    """
    Flush buffered media group after delay.
    Called 1.5s after first message in group arrives.
    Increased from 500ms to handle slow networks and large albums.
    """
    await asyncio.sleep(1.5)  # Wait for all messages in group (increased for reliability)

    group_data = media_group_buffer.pop(group_id, None)
    if not group_data:
        return

    messages = group_data['messages']
    if not messages:
        return

    # Sort by message_id to preserve order
    messages.sort(key=lambda m: m['message_id'])

    # Collect all file IDs in order
    files_path = [m['file_id'] for m in messages]

    # Use first message for metadata
    first_msg = messages[0]

    # Determine type_of_message
    types = set(m['media_type'] for m in messages)
    if len(types) == 1:
        type_of_message = list(types)[0]  # all same type
    else:
        type_of_message = 'media_group'  # mixed types

    # Get caption from first message with caption
    caption = ''
    for m in messages:
        if m.get('caption'):
            caption = m['caption']
            break

    logger.info(f"Flushing media group {group_id}: {len(files_path)} files, type={type_of_message}")

    # Save as single message with files_path array (in thread pool)
    try:
        history_id = await asyncio.to_thread(
            save_message_to_db,
            chat_id=first_msg['chat_id'],
            message_text=caption,
            msg_type='user',
            tg_id=first_msg['message_id'],
            type_of_message=type_of_message,
            group_sender_id=first_msg.get('sender_telegram_id'),
            reply_to=first_msg.get('reply_to'),
            files_path=files_path
        )

        # Track flushed group for late arrivals (keep for 30 seconds)
        media_group_flushed[group_id] = {
            'history_id': history_id,
            'flushed_at': time.time(),
            'files_path': files_path.copy()
        }

        # Cleanup old entries (older than 60 seconds)
        cutoff = time.time() - 60
        to_remove = [k for k, v in media_group_flushed.items() if v['flushed_at'] < cutoff]
        for k in to_remove:
            del media_group_flushed[k]

    except Exception as e:
        logger.error(f"Error saving media group {group_id}: {e}")


def _resolve_chat_and_user_sync(chat, user, is_group):
    """Sync DB work for resolving chat/user IDs. Runs in thread pool to avoid blocking event loop."""
    sender_user_id = None
    sender_telegram_id = None

    if is_group:
        telegram_group_id = abs(chat.id)
        group_id_for_chat = get_or_create_group(
            telegram_group_id=telegram_group_id,
            title=chat.title
        )
        internal_chat_id = get_or_create_chat('group', group_id=group_id_for_chat)

        if user:
            sender_telegram_id = user.id
            sender_user_id = get_or_create_user(
                telegram_id=user.id,
                first_name=user.first_name,
                last_name=user.last_name,
                username=user.username,
                is_bot=user.is_bot
            )
            get_or_create_chat('user', user_id=sender_user_id)
            add_user_to_group(telegram_group_id, sender_user_id)
    else:
        internal_user_id = get_or_create_user(
            telegram_id=user.id,
            first_name=user.first_name,
            last_name=user.last_name,
            username=user.username,
            is_bot=user.is_bot
        )
        internal_chat_id = get_or_create_chat('user', user_id=internal_user_id)

    return internal_chat_id, sender_user_id, sender_telegram_id


async def process_incoming_message(message: Message, message_type: str):
    """Process incoming message and save to database.
    All sync DB calls run in thread pool (asyncio.to_thread) to avoid blocking the event loop.
    """
    try:
        chat = message.chat
        user = message.from_user

        # Determine if this is a group or private chat
        is_group = chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]

        # Run all DB lookups in thread pool — never block the event loop
        internal_chat_id, sender_user_id, sender_telegram_id = await asyncio.to_thread(
            _resolve_chat_and_user_sync, chat, user, is_group
        )

        # Prepare message content (no DB calls, safe on event loop)
        message_text = ''
        mime = None
        file_id = None

        if message_type == 'text':
            message_text = message.text or ''

        elif message_type == 'voice':
            message_text = ''
            mime = message.voice.mime_type or 'audio/ogg'
            file_id = message.voice.file_id

        elif message_type == 'video_note':
            message_text = ''
            mime = 'video/mp4'
            file_id = message.video_note.file_id

        elif message_type == 'photo':
            message_text = message.caption or ''
            mime = 'image/jpeg'
            photo = message.photo[-1]
            file_id = photo.file_id

        elif message_type == 'document':
            message_text = message.caption or ''
            doc = message.document
            file_id = doc.file_id

        elif message_type == 'sticker':
            message_text = ''
            file_id = message.sticker.file_id

        elif message_type == 'video':
            message_text = message.caption or ''
            mime = message.video.mime_type or 'video/mp4'
            file_id = message.video.file_id

        # Get reply_to if exists
        reply_to = None
        if message.reply_to_message:
            reply_to = message.reply_to_message.message_id

        # Check if this is part of media group (album)
        if message.media_group_id and message_type in ('photo', 'video', 'document'):
            group_id = message.media_group_id

            # Check if this group was already flushed (late arrival)
            if group_id in media_group_flushed:
                flushed = media_group_flushed[group_id]
                # Add file to existing record
                if file_id and file_id not in flushed['files_path']:
                    flushed['files_path'].append(file_id)
                    try:
                        await asyncio.to_thread(
                            _update_media_group_file, flushed['files_path'], flushed['history_id']
                        )
                        logger.info(f"Late arrival for media group {group_id}: added file, total={len(flushed['files_path'])}")
                    except Exception as e:
                        logger.error(f"Error updating media group {group_id} with late file: {e}")
                return  # Don't create new record

            # First message in group - start timer
            if group_id not in media_group_buffer:
                media_group_buffer[group_id] = {
                    'messages': [],
                    'chat_id': internal_chat_id
                }
                # Start flush timer
                asyncio.create_task(flush_media_group(group_id))

            # Add message to buffer
            media_group_buffer[group_id]['messages'].append({
                'message_id': message.message_id,
                'file_id': file_id,
                'media_type': message_type,
                'caption': message_text,
                'chat_id': internal_chat_id,
                'sender_telegram_id': sender_telegram_id if is_group else None,
                'reply_to': reply_to
            })

            logger.info(f"Buffered media group {group_id}: msg_id={message.message_id}, type={message_type}")
            return  # Don't save individually, will be saved by flush_media_group

        # Save to DB in thread pool
        files_path_arr = [file_id] if file_id else None

        history_id = await asyncio.to_thread(
            save_message_to_db,
            chat_id=internal_chat_id,
            message_text=message_text,
            msg_type='user',
            tg_id=message.message_id,
            type_of_message=message_type,
            group_sender_id=sender_telegram_id if is_group else None,
            reply_to=reply_to,
            files_path=files_path_arr
        )

        # Trigger n8n webhook in thread pool (sync HTTP call)
        if N8N_WEBHOOK_URL:
            try:
                webhook_data = {
                    'chat_id': internal_chat_id,
                    'telegram_id': user.id if user else chat.id,
                    'message': message_text,
                    'message_type': message_type,
                    'tg_message_id': message.message_id,
                    'is_group': is_group,
                    'chat_history_id': history_id,
                    'file_id': file_id
                }
                await asyncio.to_thread(_send_n8n_webhook, webhook_data)
            except Exception as e:
                logger.error(f"Error calling n8n webhook: {e}")

    except Exception as e:
        logger.exception(f"[WEBHOOK] Error processing message: {e}")


def _update_media_group_file(files_path, history_id):
    """Sync helper for updating media group files in DB"""
    with db_cursor(commit=True) as cur:
        cur.execute("""
            UPDATE chat_history_tg
            SET files_path = %s
            WHERE id = %s
        """, (files_path, history_id))


def _send_n8n_webhook(webhook_data):
    """Sync helper for sending n8n webhook"""
    import requests
    requests.post(N8N_WEBHOOK_URL, json=webhook_data, timeout=5)


# ============== FLASK WEBHOOK ENDPOINT ==============

@flask_app.route(WEBHOOK_PATH, methods=['POST'])
def telegram_webhook():
    """Handle incoming Telegram webhook updates"""
    global dp, bot
    
    # Ensure bot is initialized
    if dp is None or bot is None:
        try:
            _ensure_initialized()
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
        
        if dp is None or bot is None:
            logger.error("Bot not initialized, returning 503")
            return jsonify({"error": "Bot not initialized"}), 503
    
    try:
        # Verify secret token if set
        if WEBHOOK_SECRET:
            token = request.headers.get('X-Telegram-Bot-Api-Secret-Token', '')
            if token != WEBHOOK_SECRET:
                logger.warning("Invalid webhook secret token")
                return jsonify({"error": "Unauthorized"}), 401
        
        update_data = request.get_json()
        update = Update(**update_data)

        run_async(dp.feed_update(bot, update))

        return jsonify({"ok": True})
    
    except Exception as e:
        logger.exception(f"[WEBHOOK] Webhook error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint — returns 503 if critical components are dead"""
    import shutil
    has_ffmpeg = shutil.which("ffmpeg") is not None

    # Check Redis connectivity
    redis_ok = False
    try:
        if redis_client:
            redis_ok = redis_client.ping()
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

    bot_ok = bot is not None
    sender_ok = sender_bot.is_running if sender_bot else False
    pg_listener_alive = pg_listener_thread.is_alive() if pg_listener_thread else False
    retry_ok = retry_worker_thread.is_alive() if retry_worker_thread else False
    consumer_ok = incoming_consumer_thread.is_alive() if incoming_consumer_thread else False

    # Queue length
    queue_len = 0
    try:
        if redis_client:
            queue_len = redis_client.llen('telegram:send_queue')
    except Exception:
        pass

    # Pool stats
    from db import get_pool_stats
    pool_stats = get_pool_stats()

    now = time.time()
    consumer_hb = _health_state.get('consumer_heartbeat', 0)
    consumer_act = _health_state.get('consumer_last_activity', 0)
    listener_act = _health_state.get('pg_listener_last_activity', 0)

    status_data = {
        "status": "ok",
        "bot_initialized": bot_ok,
        "sender_running": sender_ok,
        "redis_connected": redis_ok,
        "db_connected": db_ok,
        "db_pool": pool_stats,
        "pg_listener_alive": pg_listener_alive,
        "pg_listener_degraded": _listener_degraded,
        "pg_listener_last_activity_ago": round(now - listener_act) if listener_act else None,
        "stuck_retry_alive": retry_ok,
        "incoming_consumer_alive": consumer_ok,
        "consumer_heartbeat_ago": round(now - consumer_hb) if consumer_hb else None,
        "consumer_last_activity_ago": round(now - consumer_act) if consumer_act else None,
        "queue_length": queue_len,
        "ffmpeg_available": has_ffmpeg,
    }

    # Return 503 only if bot itself is not initialized
    # DB/sender may still be starting up — Railway healthcheck comes early
    # All other issues are reported as degraded but don't block deployment
    critical_ok = bot_ok and redis_ok
    if not critical_ok:
        status_data["status"] = "degraded"
        return jsonify(status_data), 503
    elif not (sender_ok and db_ok):
        status_data["status"] = "degraded"

    return jsonify(status_data)


@flask_app.route('/telegram/queue/peek', methods=['GET'])
def api_queue_peek():
    """Peek at send queue contents without removing them"""
    try:
        limit = request.args.get('limit', 50, type=int)
        items = redis_client.lrange('telegram:send_queue', 0, limit - 1)
        result = []
        for raw in items:
            try:
                task = json.loads(raw)
                rb = task.get('request_body', {})
                result.append({
                    "chat_history_id": task.get('chat_history_id'),
                    "chat_id": rb.get('chat_id'),
                    "text": (rb.get('text') or '')[:120],
                    "type": rb.get('type_of_message', 'text'),
                    "retry_count": task.get('retry_count', 0),
                })
            except Exception:
                result.append({"raw": str(raw)[:200]})
        return jsonify({"queue_length": len(items), "items": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/queue/flush', methods=['POST'])
def api_queue_flush():
    """Flush entire send queue (emergency). Returns deleted count."""
    try:
        length = redis_client.llen('telegram:send_queue')
        redis_client.delete('telegram:send_queue')
        logger.warning(f"[QUEUE] Flushed {length} items from send_queue")
        return jsonify({"success": True, "flushed": length})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/queue/dedup', methods=['POST'])
def api_queue_dedup():
    """Remove duplicate messages from queue, keep only unique chat_history_id entries"""
    try:
        items = redis_client.lrange('telegram:send_queue', 0, -1)
        seen = set()
        unique = []
        dupes = 0
        for raw in items:
            try:
                task = json.loads(raw)
                key = task.get('chat_history_id')
                if key and key in seen:
                    dupes += 1
                    continue
                if key:
                    seen.add(key)
                unique.append(raw)
            except Exception:
                unique.append(raw)

        # Replace queue atomically
        pipe = redis_client.pipeline()
        pipe.delete('telegram:send_queue')
        if unique:
            pipe.rpush('telegram:send_queue', *unique)
        pipe.execute()

        logger.info(f"[QUEUE] Deduped: {dupes} duplicates removed, {len(unique)} remaining")
        return jsonify({"success": True, "before": len(items), "after": len(unique), "duplicates_removed": dupes})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/queue/force_send', methods=['POST'])
def api_queue_force_send():
    """Force re-send specific message IDs by clearing dedup keys and re-queuing.
    Body: {"ids": [139123]}
    """
    try:
        data = request.get_json() or {}
        ids = data.get('ids', [])
        if not ids:
            return jsonify({"error": "ids required"}), 400

        # Clear dedup keys
        for msg_id in ids:
            redis_client.delete(f'telegram:sent:{msg_id}')
            # Clear content dedup keys too
            for key in redis_client.scan_iter(f'tg:dedup:*'):
                val = redis_client.get(key)
                if val and str(msg_id).encode() in val:
                    redis_client.delete(key)

        # Re-queue via retry_stuck
        count = retry_stuck_messages(max_age_minutes=1440)  # 24h window
        return jsonify({"success": True, "dedup_cleared": ids, "requeued": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/retry_stuck', methods=['POST'])
def api_retry_stuck():
    """
    Retry sending stuck messages (queued but not sent).

    Body (optional): {"max_age_minutes": 60}
    """
    try:
        data = request.get_json() or {}
        max_age = data.get('max_age_minutes', 60)
        count = retry_stuck_messages(max_age_minutes=max_age)
        return jsonify({"success": True, "requeued": count})
    except Exception as e:
        logger.error(f"❌ Retry stuck error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== API ENDPOINTS ==============

@flask_app.route('/telegram/send', methods=['POST'])
def api_send_message():
    """
    Send a text message to a user/chat
    
    Body:
    {
        "telegram_id": 123456789,  # Direct telegram user/chat ID
        # OR
        "chat_id": 1,  # Internal chat_id (will be resolved)
        
        "message": "Hello!",
        "reply_to_message_id": null  # Optional
    }
    """
    try:
        data = request.get_json()
        
        telegram_id = data.get('telegram_id')
        if not telegram_id:
            # Try to resolve from internal chat_id
            chat_id = data.get('chat_id')
            if chat_id:
                telegram_id = resolve_telegram_chat_id(chat_id)
        
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400
        
        message = data.get('message', '')
        reply_to = data.get('reply_to_message_id')

        async def send():
            from aiogram.enums import ParseMode
            from aiogram.exceptions import TelegramBadRequest

            # Try with Markdown first, fallback to plain text
            try:
                msg = await bot.send_message(
                    chat_id=telegram_id,
                    text=message,
                    reply_to_message_id=reply_to,
                    parse_mode=ParseMode.MARKDOWN
                )
                return msg.message_id
            except TelegramBadRequest as e:
                error_msg = str(e).lower()
                if "can't parse" in error_msg or "parse" in error_msg:
                    # Fallback to plain text (explicit None to override default)
                    logger.warning(f"⚠️ Markdown failed, sending plain: {e}")
                    msg = await bot.send_message(
                        chat_id=telegram_id,
                        text=message,
                        reply_to_message_id=reply_to,
                        parse_mode=None
                    )
                    return msg.message_id
                raise

        msg_id = run_async(send())

        return jsonify({
            "success": True,
            "message_id": msg_id
        })

    except Exception as e:
        logger.error(f"❌ Send message error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/typing', methods=['POST'])
def api_send_typing():
    """
    Send typing indicator
    
    Body:
    {
        "telegram_id": 123456789,
        "action": "typing"  # typing, upload_photo, record_video, etc.
    }
    """
    try:
        data = request.get_json()
        
        telegram_id = data.get('telegram_id')
        if not telegram_id:
            chat_id = data.get('chat_id')
            if chat_id:
                telegram_id = resolve_telegram_chat_id(chat_id)
        
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400
        
        action_str = data.get('action', 'typing')
        action_map = {
            'typing': ChatAction.TYPING,
            'upload_photo': ChatAction.UPLOAD_PHOTO,
            'record_video': ChatAction.RECORD_VIDEO,
            'upload_video': ChatAction.UPLOAD_VIDEO,
            'record_voice': ChatAction.RECORD_VOICE,
            'upload_voice': ChatAction.UPLOAD_VOICE,
            'upload_document': ChatAction.UPLOAD_DOCUMENT,
            'choose_sticker': ChatAction.CHOOSE_STICKER,
            'find_location': ChatAction.FIND_LOCATION,
            'record_video_note': ChatAction.RECORD_VIDEO_NOTE,
            'upload_video_note': ChatAction.UPLOAD_VIDEO_NOTE
        }
        action = action_map.get(action_str, ChatAction.TYPING)

        async def send_action():
            await bot.send_chat_action(chat_id=telegram_id, action=action)

        run_async(send_action())

        return jsonify({"success": True})

    except Exception as e:
        logger.error(f"❌ Typing error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/api/media/download', methods=['POST'])
def api_media_download():
    """
    Download media by Telegram file_id and save to cache.
    Used by Claude's /media skill for on-demand media access.

    Body:
    {
        "file_id": "AgACAgIAAxkBAAI...",  # Telegram file_id from video_url field
        "history_id": 123  # Optional: chat_history_tg.id for folder organization
    }

    Returns:
    {
        "success": true,
        "path": "/tmp/media_cache/123/media_AgACAgIAAx.jpg",
        "size": 12345
    }
    """
    try:
        data = request.get_json()
        file_id = data.get('file_id')
        history_id = data.get('history_id')

        if not file_id:
            return jsonify({"error": "file_id required"}), 400

        # Determine cache directory
        cache_dir = os.environ.get('MEDIA_CACHE_DIR', '/tmp/media_cache')
        if history_id:
            save_dir = f"{cache_dir}/{history_id}"
        else:
            save_dir = cache_dir

        os.makedirs(save_dir, exist_ok=True)

        async def download():
            # Get file info from Telegram
            file = await bot.get_file(file_id)
            file_bytes = await bot.download_file(file.file_path)

            # Determine extension from file_path
            ext = os.path.splitext(file.file_path)[1] if file.file_path else '.bin'
            if not ext:
                ext = '.bin'

            # Create filename
            filename = f"media_{file_id[:20]}{ext}"
            file_path = os.path.join(save_dir, filename)

            # Save file
            content = file_bytes.read()
            with open(file_path, 'wb') as f:
                f.write(content)

            return file_path, len(content)

        file_path, size = run_async(download())

        return jsonify({
            "success": True,
            "path": file_path,
            "size": size
        })

    except Exception as e:
        logger.exception(f"[API] Media download error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/api/media/get', methods=['POST'])
def api_media_get():
    """
    Get media file contents (base64) from cache or download on-demand.

    Body:
    {
        "file_id": "AgACAgIAAxkBAAI...",
        "history_id": 123
    }

    Returns:
    {
        "success": true,
        "filename": "media_AgACAgIAAx.jpg",
        "content_type": "image/jpeg",
        "base64": "..."
    }
    """
    try:
        data = request.get_json()
        file_id = data.get('file_id')
        history_id = data.get('history_id')

        if not file_id:
            return jsonify({"error": "file_id required"}), 400

        cache_dir = os.environ.get('MEDIA_CACHE_DIR', '/tmp/media_cache')
        if history_id:
            save_dir = f"{cache_dir}/{history_id}"
        else:
            save_dir = cache_dir

        # Check if already cached
        cached_file = None
        if os.path.exists(save_dir):
            for f in os.listdir(save_dir):
                if file_id[:20] in f:
                    cached_file = os.path.join(save_dir, f)
                    break

        # Download if not cached
        if not cached_file or not os.path.exists(cached_file):
            os.makedirs(save_dir, exist_ok=True)

            async def download():
                file = await bot.get_file(file_id)
                file_bytes = await bot.download_file(file.file_path)
                ext = os.path.splitext(file.file_path)[1] if file.file_path else '.bin'
                if not ext:
                    ext = '.bin'
                filename = f"media_{file_id[:20]}{ext}"
                file_path = os.path.join(save_dir, filename)
                content = file_bytes.read()
                with open(file_path, 'wb') as f:
                    f.write(content)
                return file_path

            cached_file = run_async(download())

        # Read and return as base64
        with open(cached_file, 'rb') as f:
            content = f.read()

        filename = os.path.basename(cached_file)
        ext = os.path.splitext(filename)[1].lower()

        # Determine content type
        content_types = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
            '.webp': 'image/webp',
            '.mp4': 'video/mp4',
            '.ogg': 'audio/ogg',
            '.oga': 'audio/ogg',
            '.mp3': 'audio/mpeg',
            '.pdf': 'application/pdf',
        }
        content_type = content_types.get(ext, 'application/octet-stream')

        return jsonify({
            "success": True,
            "filename": filename,
            "content_type": content_type,
            "size": len(content),
            "base64": base64.b64encode(content).decode('utf-8')
        })

    except Exception as e:
        logger.exception(f"[API] Media get error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/api/media/serve/<int:history_id>/<filename>')
def api_media_serve(history_id, filename):
    """
    Serve media file directly from cache.

    URL: /api/media/serve/123/media_AgACAgIAAx.jpg
    """
    cache_dir = os.environ.get('MEDIA_CACHE_DIR', '/tmp/media_cache')
    file_path = os.path.join(cache_dir, str(history_id), filename)

    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    return send_file(file_path)


@flask_app.route('/telegram/reaction', methods=['POST'])
def api_set_reaction():
    """
    Set reaction on a message
    
    Body:
    {
        "telegram_id": 123456789,
        "message_id": 123,
        "emoji": "👍"
    }
    """
    try:
        data = request.get_json()
        
        telegram_id = data.get('telegram_id')
        if not telegram_id:
            chat_id = data.get('chat_id')
            if chat_id:
                telegram_id = resolve_telegram_chat_id(chat_id)
        
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400
        
        message_id = data.get('message_id')
        emoji = data.get('emoji', '👍')
        
        if not message_id:
            return jsonify({"error": "message_id required"}), 400

        async def set_reaction():
            await bot.set_message_reaction(
                chat_id=telegram_id,
                message_id=message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)]
            )

        run_async(set_reaction())
        
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"❌ Reaction error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/sticker', methods=['POST'])
def api_send_sticker():
    """
    Send a sticker
    
    Body:
    {
        "telegram_id": 123456789,
        "sticker_set": "AnimatedEmojies",
        "emoji": "👋",
        "reply_to_message_id": null
    }
    """
    try:
        data = request.get_json()
        
        telegram_id = data.get('telegram_id')
        if not telegram_id:
            chat_id = data.get('chat_id')
            if chat_id:
                telegram_id = resolve_telegram_chat_id(chat_id)
        
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400
        
        sticker_set_name = data.get('sticker_set', 'AnimatedEmojies')
        emoji = data.get('emoji', '👋')
        reply_to = data.get('reply_to_message_id')

        async def send_sticker():
            # Get sticker set
            sticker_set = await bot.get_sticker_set(sticker_set_name)

            # Find sticker by emoji
            target_sticker = None
            for sticker in sticker_set.stickers:
                if sticker.emoji == emoji:
                    target_sticker = sticker
                    break

            if not target_sticker and sticker_set.stickers:
                target_sticker = sticker_set.stickers[0]

            if target_sticker:
                msg = await bot.send_sticker(
                    chat_id=telegram_id,
                    sticker=target_sticker.file_id,
                    reply_to_message_id=reply_to
                )
                return msg.message_id
            return None

        msg_id = run_async(send_sticker())
        
        if msg_id:
            return jsonify({"success": True, "message_id": msg_id})
        return jsonify({"error": "Sticker not found"}), 404
        
    except Exception as e:
        logger.error(f"❌ Sticker error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/video_note', methods=['POST'])
def api_send_video_note():
    """
    Send a video note (circle)
    
    Body:
    {
        "telegram_id": 123456789,
        "video_url": "https://...",  # URL to video
        # OR
        "video_data": "base64...",   # Base64 encoded video
        "reply_to_message_id": null
    }
    """
    try:
        data = request.get_json()
        
        telegram_id = data.get('telegram_id')
        if not telegram_id:
            chat_id = data.get('chat_id')
            if chat_id:
                telegram_id = resolve_telegram_chat_id(chat_id)
        
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400
        
        video_url = data.get('video_url')
        video_data = data.get('video_data')
        reply_to = data.get('reply_to_message_id')

        async def send_video_note():
            import aiohttp
            import subprocess
            
            # Get video bytes
            if video_url:
                async with aiohttp.ClientSession() as session:
                    async with session.get(video_url) as resp:
                        video_bytes = await resp.read()
            elif video_data:
                video_bytes = base64.b64decode(video_data)
            else:
                raise ValueError("video_url or video_data required")
            
            # Convert to circle format using ffmpeg
            with tempfile.NamedTemporaryFile(suffix='.mp4', delete=False) as temp_in:
                temp_in.write(video_bytes)
                temp_in_path = temp_in.name
            
            temp_out_path = temp_in_path.replace('.mp4', '_circle.mp4')
            
            try:
                # FFmpeg command for video note (circular, max 1 minute, 384x384)
                cmd = [
                    'ffmpeg', '-y', '-i', temp_in_path,
                    '-vf', 'scale=384:384:force_original_aspect_ratio=increase,crop=384:384',
                    '-t', '60',
                    '-c:v', 'libx264', '-preset', 'ultrafast',
                    '-c:a', 'aac', '-b:a', '128k',
                    temp_out_path
                ]
                subprocess.run(cmd, check=True, capture_output=True)
                
                with open(temp_out_path, 'rb') as f:
                    video_note_bytes = f.read()
                
                video_note = BufferedInputFile(video_note_bytes, filename='video_note.mp4')
                
                msg = await bot.send_video_note(
                    chat_id=telegram_id,
                    video_note=video_note,
                    reply_to_message_id=reply_to
                )
                return msg.message_id
            finally:
                # Cleanup temp files
                try:
                    os.unlink(temp_in_path)
                except:
                    pass
                try:
                    os.unlink(temp_out_path)
                except:
                    pass

        msg_id = run_async(send_video_note())

        return jsonify({"success": True, "message_id": msg_id})

    except Exception as e:
        logger.error(f"❌ Video note error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/voice/download', methods=['GET', 'POST'])
def api_download_voice():
    """
    Download voice message by file_id
    
    GET /telegram/voice/download?file_id=xxx&format=file|base64
    POST with body {"file_id": "xxx", "format": "base64"}
    """
    try:
        if request.method == 'GET':
            file_id = request.args.get('file_id')
            output_format = request.args.get('format', 'file')
        else:
            data = request.get_json()
            file_id = data.get('file_id')
            output_format = data.get('format', 'base64')
        
        if not file_id:
            return jsonify({"error": "file_id required"}), 400

        async def download():
            file = await bot.get_file(file_id)
            file_bytes = await bot.download_file(file.file_path)
            return file_bytes.read()

        voice_bytes = run_async(download())
        
        if output_format == 'file':
            return send_file(
                BytesIO(voice_bytes),
                mimetype='audio/ogg',
                as_attachment=True,
                download_name='voice.ogg'
            )
        
        return jsonify({
            "success": True,
            "data": base64.b64encode(voice_bytes).decode('utf-8'),
            "mime": "audio/ogg",
            "size_bytes": len(voice_bytes)
        })
        
    except Exception as e:
        logger.error(f"❌ Voice download error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/document', methods=['POST'])
def api_send_document():
    """
    Send a document
    
    Body:
    {
        "telegram_id": 123456789,
        "document_url": "https://...",  # URL to document
        # OR
        "document_data": "base64...",   # Base64 encoded document
        "filename": "document.pdf",
        "caption": "Here's the document",
        "reply_to_message_id": null
    }
    """
    try:
        data = request.get_json()
        
        telegram_id = data.get('telegram_id')
        if not telegram_id:
            chat_id = data.get('chat_id')
            if chat_id:
                telegram_id = resolve_telegram_chat_id(chat_id)
        
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400
        
        document_url = data.get('document_url')
        document_data = data.get('document_data')
        filename = data.get('filename', 'document.pdf')
        caption = data.get('caption', '')
        reply_to = data.get('reply_to_message_id')

        async def send_doc():
            # Get document bytes
            if document_url:
                import aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(document_url) as resp:
                        doc_bytes = await resp.read()
            elif document_data:
                doc_bytes = base64.b64decode(document_data)
            else:
                raise ValueError("document_url or document_data required")

            document = BufferedInputFile(doc_bytes, filename=filename)

            msg = await bot.send_document(
                chat_id=telegram_id,
                document=document,
                caption=caption,
                reply_to_message_id=reply_to
            )
            return msg.message_id

        msg_id = run_async(send_doc())
        
        return jsonify({"success": True, "message_id": msg_id})
        
    except Exception as e:
        logger.error(f"❌ Send document error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== SENDER STATS API ==============

@flask_app.route('/telegram/sender/stats', methods=['GET'])
def api_sender_stats():
    """Get sender statistics"""
    if sender_bot:
        return jsonify({
            "success": True,
            "stats": sender_bot.get_stats()
        })
    return jsonify({"error": "Sender not initialized"}), 500


@flask_app.route('/telegram/sender/start', methods=['POST'])
def api_sender_start():
    """Start the sender if stopped"""
    global sender_bot
    
    if sender_bot and sender_bot.is_running:
        return jsonify({"message": "Sender already running"})
    
    if not sender_bot:
        if REDIS_URL:
            sender_bot = SenderBot(bot, REDIS_URL, DATABASE_URL)
    
    if sender_bot:
        sender_bot.start()
        return jsonify({"success": True, "message": "Sender started"})
    
    return jsonify({"error": "Cannot create sender"}), 500


@flask_app.route('/telegram/sender/stop', methods=['POST'])
def api_sender_stop():
    """Stop the sender"""
    if sender_bot:
        sender_bot.stop()
        return jsonify({"success": True, "message": "Sender stopped"})
    return jsonify({"error": "Sender not initialized"}), 500


# ============== MEDIA DOWNLOAD ENDPOINT (for n8n) ==============

@flask_app.route('/telegram/message/<telegram_id>/<int:message_id>/media/download', methods=['GET'])
def api_download_media(telegram_id, message_id):
    """
    Download document by message — uses Telegram file_id from chat_history_tg.files_path

    For voice/video use POST /telegram/voice/download or /telegram/file/download with file_id
    """
    try:
        tg_id = int(telegram_id)
        output_format = request.args.get('format', 'base64')

        chat_info = get_chat_info_by_telegram_id(tg_id)
        if not chat_info:
            return jsonify({"error": "Chat not found", "success": False}), 404

        internal_chat_id = chat_info['chat_id']

        with db_cursor() as cur:
            cur.execute("""
                SELECT id, type_of_message, files_path
                FROM chat_history_tg
                WHERE chat_id = %s AND tg_id = %s
                LIMIT 1
            """, (internal_chat_id, message_id))
            row = cur.fetchone()

            if not row:
                return jsonify({"error": "Message not found", "success": False}), 404

            history_id, msg_type, files_path = row

        if not files_path or len(files_path) == 0:
            return jsonify({
                "error": "No file attached to this message",
                "success": False
            }), 404

        # Download via Telegram Bot API using file_id
        file_id = files_path[0]

        async def download():
            file = await bot.get_file(file_id)
            file_bytes = await bot.download_file(file.file_path)
            return file_bytes.read(), file.file_path

        file_data, file_path = run_async(download())
        filename = os.path.basename(file_path) if file_path else 'document'
        mime_type = 'application/octet-stream'

        if output_format == 'file':
            return send_file(
                BytesIO(file_data),
                mimetype=mime_type,
                as_attachment=True,
                download_name=filename
            )

        return jsonify({
            "success": True,
            "message_id": message_id,
            "data": base64.b64encode(file_data).decode('utf-8'),
            "mime": mime_type,
            "type": msg_type,
            "size_bytes": len(file_data),
            "metadata": {
                "filename": filename,
                "size": len(file_data),
                "mime_type": mime_type
            }
        })

    except Exception as e:
        logger.exception(f"[API] Media download error: {e}")
        return jsonify({"error": str(e), "success": False}), 500


@flask_app.route('/telegram/file/download', methods=['POST'])
def api_download_file_by_id():
    """
    Download any file from Telegram by file_id
    
    Body: { "file_id": "xxx", "format": "base64" or "file" }
    Returns: file or JSON with base64
    """
    try:
        data = request.get_json()
        file_id = data.get('file_id')
        output_format = data.get('format', 'base64')
        filename = data.get('filename', 'file')
        mime_type = data.get('mime', 'application/octet-stream')
        
        if not file_id:
            return jsonify({"error": "file_id required"}), 400

        async def download():
            file = await bot.get_file(file_id)
            file_bytes = await bot.download_file(file.file_path)
            return file_bytes.read()

        file_data = run_async(download())
        
        if output_format == 'file':
            return send_file(
                BytesIO(file_data),
                mimetype=mime_type,
                as_attachment=True,
                download_name=filename
            )
        
        return jsonify({
            "success": True,
            "data": base64.b64encode(file_data).decode('utf-8'),
            "mime": mime_type,
            "size_bytes": len(file_data)
        })
        
    except Exception as e:
        logger.error(f"❌ File download error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== TEMP FILES ENDPOINT ==============

@flask_app.route('/files/temp/<filename>', methods=['GET'])
def serve_temp_file(filename):
    """
    Serve temporary files (voice, video, etc.)
    Note: Files are ephemeral - use /media/download for persistent access
    """
    try:
        filepath = os.path.join(TEMP_FILES_DIR, filename)
        if os.path.exists(filepath):
            # Determine mimetype
            mimetype = 'application/octet-stream'
            if filename.endswith('.ogg'):
                mimetype = 'audio/ogg'
            elif filename.endswith('.mp4'):
                mimetype = 'video/mp4'
            elif filename.endswith('.jpg') or filename.endswith('.jpeg'):
                mimetype = 'image/jpeg'
            elif filename.endswith('.png'):
                mimetype = 'image/png'
            
            return send_file(filepath, mimetype=mimetype)
        
        return jsonify({
            "error": "File not found",
            "hint": "Temporary files are ephemeral. Use /telegram/message/{tg_id}/{msg_id}/media/download for persistent access from database"
        }), 404
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============== MESSAGE REACTION ENDPOINTS ==============

@flask_app.route('/telegram/message/<telegram_id>/<int:message_id>/reaction', methods=['POST'])
def api_set_message_reaction(telegram_id, message_id):
    """
    Set reaction on a message
    
    Body: {"emoji": "👍"}
    """
    try:
        tg_id = int(telegram_id)
        data = request.get_json() or {}
        emoji = data.get('emoji', '👍')

        async def set_reaction():
            await bot.set_message_reaction(
                chat_id=tg_id,
                message_id=message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)]
            )

        run_async(set_reaction())
        
        return jsonify({"success": True, "emoji": emoji})
        
    except Exception as e:
        logger.error(f"❌ Reaction error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/message/<telegram_id>/<int:message_id>/reaction', methods=['DELETE'])
def api_remove_message_reaction(telegram_id, message_id):
    """Remove reaction from a message"""
    try:
        tg_id = int(telegram_id)

        async def remove_reaction():
            await bot.set_message_reaction(
                chat_id=tg_id,
                message_id=message_id,
                reaction=[]
            )

        run_async(remove_reaction())
        
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"❌ Remove reaction error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== PIN/UNPIN MESSAGE ENDPOINTS ==============

@flask_app.route('/telegram/chat/<chat_id>/pin', methods=['POST'])
def api_pin_message(chat_id):
    """
    Pin a message in chat

    Body: {"message_id": 123, "notify": true}

    Note: For groups, bot must be admin with pin_messages permission
    """
    try:
        tg_chat_id = int(chat_id)
        data = request.get_json() or {}
        message_id = data.get('message_id')
        disable_notification = not data.get('notify', True)

        if not message_id:
            return jsonify({"error": "message_id required"}), 400

        async def pin():
            await bot.pin_chat_message(
                chat_id=tg_chat_id,
                message_id=int(message_id),
                disable_notification=disable_notification
            )

        run_async(pin())

        logger.info(f"📌 Message pinned: chat={tg_chat_id}, msg={message_id}")
        return jsonify({"success": True, "message_id": message_id, "pinned": True})

    except Exception as e:
        logger.error(f"❌ Pin message error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/chat/<chat_id>/unpin', methods=['POST'])
def api_unpin_message(chat_id):
    """
    Unpin a message in chat

    Body: {"message_id": 123}

    If message_id is not provided, unpins the most recent pinned message
    """
    try:
        tg_chat_id = int(chat_id)
        data = request.get_json() or {}
        message_id = data.get('message_id')

        async def unpin():
            if message_id:
                await bot.unpin_chat_message(
                    chat_id=tg_chat_id,
                    message_id=int(message_id)
                )
            else:
                # Unpin most recent pinned message
                await bot.unpin_chat_message(chat_id=tg_chat_id)

        run_async(unpin())

        logger.info(f"📌 Message unpinned: chat={tg_chat_id}, msg={message_id}")
        return jsonify({"success": True, "message_id": message_id, "pinned": False})

    except Exception as e:
        logger.error(f"❌ Unpin message error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/chat/<chat_id>/unpin_all', methods=['POST'])
def api_unpin_all_messages(chat_id):
    """
    Unpin all messages in chat

    Note: For groups, bot must be admin with pin_messages permission
    """
    try:
        tg_chat_id = int(chat_id)

        async def unpin_all():
            await bot.unpin_all_chat_messages(chat_id=tg_chat_id)

        run_async(unpin_all())

        logger.info(f"📌 All messages unpinned: chat={tg_chat_id}")
        return jsonify({"success": True, "unpinned_all": True})

    except Exception as e:
        logger.error(f"❌ Unpin all messages error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== FORWARD MESSAGE ENDPOINT ==============

@flask_app.route('/telegram/forward', methods=['POST'])
def api_forward_message():
    """
    Forward a message from one chat to another.
    Body: { "chat_id": 123, "from_chat_id": 456, "message_id": 789 }
    """
    try:
        data = request.get_json()
        chat_id = data.get('chat_id')
        from_chat_id = data.get('from_chat_id')
        message_id = data.get('message_id')

        if not all([chat_id, from_chat_id, message_id]):
            return jsonify({"error": "chat_id, from_chat_id, message_id required"}), 400

        result = run_async(sender_bot.worker_bot.forward_message(
            chat_id=chat_id,
            from_chat_id=from_chat_id,
            message_id=message_id
        ))
        return jsonify({"success": True, "message_id": result.message_id})

    except Exception as e:
        logger.error(f"Forward error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== INTERNAL UPDATE ENDPOINT (for PostgreSQL triggers) ==============

@flask_app.route('/internal/update_message', methods=['POST'])
def api_internal_update_message():
    """
    Internal endpoint for updating message status from triggers
    NOT for external use
    """
    try:
        data = request.get_json()
        chat_history_id = data.get('chat_history_id')
        tg_message_id = data.get('tg_message_id')
        status = data.get('status')
        error = data.get('error')

        with db_cursor(commit=True) as cur:
            cur.execute("""
                UPDATE chat_history_tg
                SET tg_id = COALESCE(%s, tg_id),
                    status = COALESCE(%s, status)
                WHERE id = %s
            """, (tg_message_id, status, chat_history_id))
        
        return jsonify({"success": True})
        
    except Exception as e:
        logger.error(f"❌ Internal update error: {e}")
        return jsonify({"error": str(e)}), 500


def incoming_message_consumer():
    """
    Consume incoming messages from Redis queue (from receiver service).
    This allows the receiver to quickly save messages while bot-api processes them.

    Recovery strategy:
    - Backoff capped at 5s (not 60s) — messages pile up fast
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
            _health_state['consumer_heartbeat'] = time.time()  # Thread alive signal (even on idle BLPOP timeout)
            if result:
                _health_state['consumer_last_activity'] = time.time()
                _, update_json = result
                update_data = json.loads(update_json)

                # Process through aiogram dispatcher
                update = Update(**update_data)
                run_async(dp.feed_update(bot, update))

                update_id = update_data.get('update_id', 'unknown')
                logger.info(f"[CONSUMER] Processed incoming update: {update_id}")
                consecutive_errors = 0
                _health_state['consumer_last_activity'] = time.time()

        except json.JSONDecodeError as e:
            logger.error(f"[CONSUMER] Invalid JSON in queue: {e}")
        except psycopg2.pool.PoolError as e:
            consecutive_errors += 1
            logger.warning(f"[CONSUMER] Pool exhausted (#{consecutive_errors}): {e}, backing off 3s")
            for _ in range(30):
                if _shutdown_event.is_set() or _consumer_stop_event.is_set():
                    break
                time.sleep(0.1)
        except Exception as e:
            consecutive_errors += 1
            # Cap backoff at 5 seconds — messages pile up fast, can't afford 60s gaps
            backoff = min(2 ** consecutive_errors, 5)
            if consecutive_errors == 1 or consecutive_errors % 5 == 0:
                logger.error(f"[CONSUMER] Error (#{consecutive_errors}): {type(e).__name__}: {e}")

            # After 3 errors, proactively reset DB pool (likely stale connections)
            if consecutive_errors == 3:
                try:
                    from db import _reset_pool as reset_shared_pool
                    reset_shared_pool()
                    logger.info("[CONSUMER] Reset DB pool after 3 consecutive errors")
                except Exception:
                    pass

            # After 10 errors, try reconnecting Redis too
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


def retry_stuck_messages(max_age_minutes: int = 30):
    """
    Find and re-queue stuck messages (status='queued' but no tg_id).
    When listener is degraded, picks up messages after 5 seconds instead of 60.
    """
    from redis_client import get_redis

    # In degraded mode, pick up messages much faster (5s vs 60s)
    min_age_seconds = 5 if _listener_degraded else 60

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
                       c.type as chat_type, c.group_id, u.telegram_id
                FROM chat_history_tg h
                JOIN chats_tg c ON c.id = h.chat_id
                LEFT JOIN users_tg u ON u.id = c.user_id
                WHERE h.type = 'AEON'
                  AND h.status = 'queued'
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
                msg_id, chat_id, message, type_of_message, reply_to, media_id, files_url, files_path, chat_type, group_id, telegram_id = row

                # Determine chat_ident
                if chat_type == 'group':
                    if not group_id:
                        continue
                    chat_ident = str(-group_id)
                else:
                    if not telegram_id:
                        continue
                    chat_ident = str(telegram_id)

                # Build request body based on type
                if type_of_message in ('video_note', 'video', 'photo', 'document') and files_url:
                    # Media message with URL
                    request_body = {
                        'chat_id': chat_ident,
                        'type_of_message': type_of_message,
                        'file_url': files_url[0] if files_url else None
                    }
                    if message:
                        request_body['caption'] = message
                else:
                    # Text message
                    request_body = {
                        'chat_id': chat_ident,
                        'type_of_message': 'text',
                        'message': message
                    }

                # Resolve reply_to if present
                if reply_to:
                    cur.execute("""
                        SELECT tg_id FROM chat_history_tg
                        WHERE (id = %s OR tg_id = %s) AND chat_id = %s AND tg_id IS NOT NULL
                        LIMIT 1
                    """, (reply_to, reply_to, chat_id))
                    reply_row = cur.fetchone()
                    if reply_row and reply_row[0]:
                        request_body['reply_to'] = reply_row[0]

                task_json = json.dumps({
                    'chat_history_id': msg_id,
                    'chat_ident': chat_ident,
                    'request_body': request_body,
                    'queued_at': time.time(),
                    'retry': True
                })

                redis_client_local.rpush(queue_key, task_json)
                requeued += 1

            cur.close()
            logger.info(f"Re-queued {requeued} stuck messages")
            return requeued

    except Exception as e:
        logger.exception(f"[RETRY] retry_stuck_messages error: {e}")
        return 0


def _pg_connect_with_timeout(database_url: str, timeout: int = 15):
    """
    Connect to PostgreSQL with a total timeout (including handshake).
    connect_timeout only covers TCP SYN/ACK — the PG protocol handshake
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
                keepalives_idle=10,     # Send TCP keepalive after 10s idle (was 30)
                keepalives_interval=5,  # Retry every 5s (was 10)
                keepalives_count=3,
                options='-c statement_timeout=0'  # Explicit: no timeout for LISTEN
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
    """
    Listen for PostgreSQL NOTIFY events and forward to Redis queue.
    With auto-reconnect, keepalive, and hard timeouts.
    Uses direct connection (port 5432) — LISTEN/NOTIFY does not work through Supavisor.
    """
    global _listener_degraded
    logger.info("[LISTENER] Starting pg_notify listener worker...")

    conn = None
    cur = None
    local_redis_client = None
    queue_key = 'telegram:send_queue'
    last_keepalive = time.time()
    KEEPALIVE_INTERVAL = 15  # Aggressive keepalive to prevent Supabase from killing connection

    while not _shutdown_event.is_set():
        try:
            # ===== CONNECT TO POSTGRES =====
            if conn is None or conn.closed:
                _listener_degraded = True
                logger.info("[LISTENER] Connecting to Postgres...")
                conn = _pg_connect_with_timeout(database_url, timeout=15)
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute("LISTEN telegram_send;")
                last_keepalive = time.time()
                _listener_degraded = False
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
                    # Simple keepalive — no statement_timeout wrapper
                    # (SET statement_timeout was being cancelled by Supabase)
                    cur.execute("SELECT 1")
                    cur.fetchone()
                    last_keepalive = now
                    _health_state['pg_listener_last_activity'] = now
                except Exception as e:
                    logger.warning(f"[LISTENER] Keepalive failed: {e}, reconnecting...")
                    raise

            if select_result == ([], [], []):
                continue

            # ===== POLL FOR NOTIFICATIONS =====
            try:
                # Double-check with short timeout before poll (prevents hang on half-open connections)
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
            _listener_degraded = True
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

            logger.info("[LISTENER] Waiting 5 seconds before reconnect...")
            _shutdown_event.wait(5)

    logger.info("[LISTENER] Stopped (shutdown)")


def stuck_messages_retry_worker():
    """
    Safety net: periodically check for and retry stuck messages.
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
            is_degraded = _listener_degraded
            count = retry_stuck_messages(max_age_minutes=240)
            if count > 0:
                logger.info(f"[RETRY] Recovered {count} stuck messages" + (" (degraded mode)" if is_degraded else ""))
        except Exception as e:
            logger.error(f"[RETRY] Worker error: {e}")

        interval = DEGRADED_INTERVAL if _listener_degraded else NORMAL_INTERVAL
        for _ in range(int(interval * 10)):
            if _shutdown_event.is_set() or _retry_stop_event.is_set():
                break
            time.sleep(0.1)

    reason = "stop_event" if _retry_stop_event.is_set() else "shutdown"
    logger.info(f"[RETRY] Stopped ({reason})")


def thread_watchdog():
    """
    Monitor daemon threads and restart them if they die.
    Checks every 60 seconds. Uses per-thread stop events for safe restart.
    """
    global pg_listener_thread, retry_worker_thread, incoming_consumer_thread
    CHECK_INTERVAL = 60
    HEALTH_LOG_INTERVAL = 300
    last_health_log = time.time()
    restart_counter = 0
    consecutive_pg_restarts = 0
    MAX_PG_RESTARTS = 5

    _shutdown_event.wait(30)
    logger.info("[WATCHDOG] Thread watchdog started")

    while not _shutdown_event.is_set():
        try:
            # Check PgNotifyListener
            pg_stale = (time.time() - _health_state.get('pg_listener_last_activity', 0)) > 120
            pg_dead = pg_listener_thread and not pg_listener_thread.is_alive()

            if pg_dead or (pg_listener_thread and pg_stale):
                reason = "DEAD" if pg_dead else "STALE (no activity >120s)"
                logger.error(f"[WATCHDOG] PgNotifyListener {reason} — restarting!")
                consecutive_pg_restarts += 1

                if consecutive_pg_restarts >= MAX_PG_RESTARTS:
                    logger.critical(f"[WATCHDOG] PG listener restarted {consecutive_pg_restarts}x — KILLING PROCESS for clean restart")
                    os._exit(1)

                if pg_listener_thread:
                    pg_listener_thread.join(timeout=2)
                restart_counter += 1
                pg_listener_thread = threading.Thread(
                    target=pg_notify_listener_worker,
                    args=(REDIS_URL, DATABASE_URL_DIRECT),
                    daemon=True,
                    name=f"PgNotifyListener-{restart_counter}"
                )
                pg_listener_thread.start()
                _health_state['pg_listener_last_activity'] = time.time()
                logger.info(f"[WATCHDOG] PgNotifyListener restarted (#{restart_counter}, consecutive={consecutive_pg_restarts})")
            else:
                # Only reset consecutive counter after 5+ minutes of stability
                pg_healthy_duration = time.time() - _health_state.get('pg_listener_last_activity', 0)
                if pg_healthy_duration < 30 and consecutive_pg_restarts > 0:
                    # Recently active = truly healthy, reset counter
                    consecutive_pg_restarts = 0

            # Check StuckMessagesRetry
            if retry_worker_thread and not retry_worker_thread.is_alive():
                logger.error("[WATCHDOG] StuckMessagesRetry thread DEAD — restarting!")
                _retry_stop_event.set()
                retry_worker_thread.join(timeout=2)
                _retry_stop_event.clear()
                restart_counter += 1
                retry_worker_thread = threading.Thread(
                    target=stuck_messages_retry_worker,
                    daemon=True,
                    name=f"StuckMessagesRetry-{restart_counter}"
                )
                retry_worker_thread.start()
                logger.info(f"[WATCHDOG] StuckMessagesRetry restarted (#{restart_counter})")

            # Check IncomingConsumer
            # Use heartbeat (updates every BLPOP cycle ~5s) for liveness, not activity (only on real messages)
            consumer_dead = incoming_consumer_thread and not incoming_consumer_thread.is_alive()
            consumer_stuck = False
            if incoming_consumer_thread and incoming_consumer_thread.is_alive():
                last_hb = _health_state.get('consumer_heartbeat', time.time())
                consumer_stuck = (time.time() - last_hb) > 30  # 30s = 6x BLPOP cycles missed

            if consumer_dead or consumer_stuck:
                reason = "DEAD" if consumer_dead else f"STUCK (no heartbeat {time.time() - _health_state.get('consumer_heartbeat', 0):.0f}s)"
                logger.error(f"[WATCHDOG] IncomingConsumer {reason} — restarting!")
                _consumer_stop_event.set()
                if incoming_consumer_thread and incoming_consumer_thread.is_alive():
                    incoming_consumer_thread.join(timeout=10)
                    if incoming_consumer_thread.is_alive():
                        logger.warning("[WATCHDOG] Old IncomingConsumer still alive after 10s — skipping restart")
                        _consumer_stop_event.clear()
                    else:
                        _consumer_stop_event.clear()
                        restart_counter += 1
                        incoming_consumer_thread = threading.Thread(
                            target=incoming_message_consumer, daemon=True,
                            name=f"IncomingConsumer-{restart_counter}")
                        incoming_consumer_thread.start()
                        _health_state['consumer_last_activity'] = time.time()
                        _health_state['consumer_heartbeat'] = time.time()
                        logger.info(f"[WATCHDOG] IncomingConsumer restarted (#{restart_counter})")
                else:
                    _consumer_stop_event.clear()
                    restart_counter += 1
                    incoming_consumer_thread = threading.Thread(
                        target=incoming_message_consumer, daemon=True,
                        name=f"IncomingConsumer-{restart_counter}")
                    incoming_consumer_thread.start()
                    _health_state['consumer_last_activity'] = time.time()
                    _health_state['consumer_heartbeat'] = time.time()
                    logger.info(f"[WATCHDOG] IncomingConsumer restarted (#{restart_counter})")

            # Check SenderBot worker
            if sender_bot:
                thread_dead = sender_bot.worker_thread and not sender_bot.worker_thread.is_alive()
                flag_says_running = sender_bot.is_running
                if thread_dead or (not flag_says_running and sender_bot.worker_thread):
                    logger.error(f"[WATCHDOG] SenderBot worker DEAD (is_running={flag_says_running}, thread_alive={not thread_dead}) — restarting!")
                    sender_bot.is_running = False
                    sender_bot.start()
                    logger.info("[WATCHDOG] SenderBot restarted")

            # Count dead critical threads — if all dead, exit for Railway restart
            critical_dead = 0
            if pg_listener_thread and not pg_listener_thread.is_alive():
                critical_dead += 1
            if incoming_consumer_thread and not incoming_consumer_thread.is_alive():
                critical_dead += 1
            if sender_bot and sender_bot.worker_thread and not sender_bot.worker_thread.is_alive():
                critical_dead += 1

            if critical_dead >= 2:
                logger.critical(f"[WATCHDOG] {critical_dead} critical threads dead — exiting for Railway restart")
                os._exit(1)

            # Total restart counter — too many restarts means something is fundamentally broken
            if restart_counter >= 15:
                logger.critical(f"[WATCHDOG] {restart_counter} total restarts — exiting for Railway restart")
                os._exit(1)

            # Periodic health status log
            now = time.time()
            if now - last_health_log >= HEALTH_LOG_INTERVAL:
                pg_alive = pg_listener_thread.is_alive() if pg_listener_thread else False
                retry_alive = retry_worker_thread.is_alive() if retry_worker_thread else False
                consumer_alive = incoming_consumer_thread.is_alive() if incoming_consumer_thread else False
                sender_alive = (sender_bot.worker_thread.is_alive() if sender_bot and sender_bot.worker_thread else False)
                queue_len = 0
                sender_stats = {}
                try:
                    if sender_bot:
                        sender_stats = sender_bot.get_stats()
                        queue_len = sender_stats.get('queue_length', 0)
                except Exception:
                    pass
                degraded_str = " DEGRADED(polling)" if _listener_degraded else ""
                logger.info(
                    f"[WATCHDOG] Status: pg_listener={'alive' if pg_alive else 'DEAD'}{degraded_str} "
                    f"sender={'alive' if sender_alive else 'DEAD'} "
                    f"consumer={'alive' if consumer_alive else 'DEAD'} "
                    f"retry={'alive' if retry_alive else 'DEAD'} "
                    f"queue={queue_len} "
                    f"sent={sender_stats.get('sent', 0)} errors={sender_stats.get('failed', 0)}"
                )
                last_health_log = now

        except Exception as e:
            logger.error(f"[WATCHDOG] Error: {e}")

        _shutdown_event.wait(CHECK_INTERVAL)

    logger.info("[WATCHDOG] Stopped (shutdown)")


# ============== AVATAR ENDPOINTS ==============

@flask_app.route('/api/user/<int:telegram_id>/avatar', methods=['GET'])
def api_get_user_avatar(telegram_id):
    """
    Get user avatar from Telegram.
    Returns JPEG image or 404 if no photo.
    """
    try:
        async def get_avatar():
            photos = await bot.get_user_profile_photos(user_id=telegram_id, limit=1)
            if not photos.photos:
                return None
            # Get largest size of first photo
            largest = photos.photos[0][-1]
            file = await bot.get_file(largest.file_id)
            file_bytes = await bot.download_file(file.file_path)
            return file_bytes.read()

        avatar_bytes = run_async(get_avatar())
        if avatar_bytes is None:
            return jsonify({"error": "No photo"}), 404

        return send_file(
            BytesIO(avatar_bytes),
            mimetype='image/jpeg',
            max_age=3600  # Cache for 1 hour
        )

    except Exception as e:
        logger.error(f"❌ Get user avatar error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/api/chat/<int:chat_id>/avatar', methods=['GET'])
def api_get_chat_avatar(chat_id):
    """
    Get chat avatar from Telegram.
    For groups: returns group photo.
    For users: pass telegram_id as chat_id.
    Returns JPEG image or 404 if no photo.
    """
    try:
        async def get_avatar():
            chat = await bot.get_chat(chat_id)
            if not chat.photo:
                return None
            file = await bot.get_file(chat.photo.big_file_id)
            file_bytes = await bot.download_file(file.file_path)
            return file_bytes.read()

        avatar_bytes = run_async(get_avatar())
        if avatar_bytes is None:
            return jsonify({"error": "No photo"}), 404

        return send_file(
            BytesIO(avatar_bytes),
            mimetype='image/jpeg',
            max_age=3600  # Cache for 1 hour
        )

    except Exception as e:
        logger.error(f"❌ Get chat avatar error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== STARTUP / SHUTDOWN ==============

async def on_startup():
    """Called on bot startup"""
    global bot, dp, sender_bot, redis_client, router, pg_listener_thread, retry_worker_thread, incoming_consumer_thread

    logger.info("[STARTUP] Starting AEON Telegram Bot...")
    
    # Init bot and dispatcher
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    dp = Dispatcher()
    
    # Setup handlers
    router = setup_handlers()
    dp.include_router(router)
    
    # Init database pool (retry up to 30s — Supabase may be slow after restart)
    db_ready = False
    for db_attempt in range(6):  # 6 × 5s = 30s max
        try:
            pool = get_db_pool()
            if pool:
                db_ready = True
                break
        except Exception as e:
            logger.warning(f"[STARTUP] DB pool init failed (attempt {db_attempt + 1}/6): {e}")
            if db_attempt < 5:
                time.sleep(5)
    if not db_ready:
        logger.error("[STARTUP] DB pool not available — will retry in background threads")

    # Wait for Redis to be available before starting dependent threads
    from redis_client import get_redis, wait_for_redis
    wait_for_redis(max_wait=30)

    # Init Redis client (for general use)
    redis_client = get_redis()
    if redis_client:
        logger.info("[STARTUP] Redis client ready")
    
    # Start pg_notify listener (uses direct connection for LISTEN/NOTIFY)
    if DATABASE_URL_DIRECT and REDIS_URL:
        pg_listener_thread = threading.Thread(
            target=pg_notify_listener_worker,
            args=(REDIS_URL, DATABASE_URL_DIRECT),
            daemon=True,
            name="PgNotifyListener"
        )
        pg_listener_thread.start()
        logger.info("[STARTUP] pg_notify listener thread started")

        # Start stuck messages retry worker (safety net)
        retry_worker_thread = threading.Thread(
            target=stuck_messages_retry_worker,
            daemon=True,
            name="StuckMessagesRetry"
        )
        retry_worker_thread.start()
        logger.info("[STARTUP] Stuck messages retry worker started")

    # Init sender
    if REDIS_URL:
        sender_bot = SenderBot(bot, REDIS_URL, DATABASE_URL)
        sender_bot.start()
        logger.info("[STARTUP] Sender started")

    # Start incoming message consumer (reads from receiver queue)
    if REDIS_URL:
        incoming_consumer_thread = threading.Thread(
            target=incoming_message_consumer,
            daemon=True,
            name="IncomingConsumer"
        )
        incoming_consumer_thread.start()
        logger.info("[STARTUP] Incoming message consumer started")

    # Start thread watchdog (restarts dead threads)
    if REDIS_URL:
        watchdog_thread = threading.Thread(
            target=thread_watchdog,
            daemon=True,
            name="ThreadWatchdog"
        )
        watchdog_thread.start()
        logger.info("[STARTUP] Thread watchdog started")

    # Set webhook
    if BASE_URL:
        webhook_url = f"{BASE_URL}{WEBHOOK_PATH}"
        await bot.set_webhook(
            url=webhook_url,
            secret_token=WEBHOOK_SECRET if WEBHOOK_SECRET else None,
            drop_pending_updates=False  # Keep pending updates after restart
        )
        logger.info(f"[STARTUP] Webhook set: {webhook_url}")
    else:
        logger.warning("[STARTUP] BASE_URL not set, webhook not configured")

    # Note: stuck messages will be picked up by the retry worker within ~30s of startup
    logger.info("[STARTUP] Startup complete")


async def on_shutdown():
    """Called on bot shutdown"""
    global sender_bot

    logger.info("[SHUTDOWN] Shutting down...")

    # Signal all background threads to stop
    _shutdown_event.set()

    try:
        if sender_bot:
            sender_bot.stop()
            logger.info("[SHUTDOWN] SenderBot stopped")
    except Exception as e:
        logger.error(f"[SHUTDOWN] SenderBot stop error: {e}")

    try:
        if bot:
            await bot.delete_webhook()
            await bot.session.close()
            logger.info("[SHUTDOWN] Bot session closed")
    except Exception as e:
        logger.error(f"[SHUTDOWN] Bot session close error: {e}")

    try:
        from db import close_db_pool
        close_db_pool()
        logger.info("[SHUTDOWN] DB pool closed")
    except Exception as e:
        logger.error(f"[SHUTDOWN] DB pool close error: {e}")

    try:
        from redis_client import close_redis_pool
        close_redis_pool()
        logger.info("[SHUTDOWN] Redis pool closed")
    except Exception as e:
        logger.error(f"[SHUTDOWN] Redis pool close error: {e}")

    logger.info("[SHUTDOWN] Shutdown complete")


def run_flask():
    """Run Flask in main thread"""
    port = int(os.environ.get('PORT', 8080))
    logger.info(f"🚀 Starting Flask on port {port}")
    flask_app.run(host='0.0.0.0', port=port, threaded=True)


def _handle_sigterm(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown"""
    sig_name = signal.Signals(signum).name
    logger.info(f"[SHUTDOWN] {sig_name} received, initiating graceful shutdown...")
    _shutdown_event.set()
    try:
        run_async(on_shutdown())
    except Exception as e:
        logger.error(f"[SHUTDOWN] Error during shutdown: {e}")


def init_app():
    """Initialize the application (for gunicorn or direct run)"""
    global bot, dp, sender_bot, redis_client, router, pg_listener_thread

    # Only initialize once
    if bot is not None and dp is not None:
        return

    logger.info("[STARTUP] Initializing AEON Telegram Bot...")

    # Register signal handlers for graceful shutdown
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, _handle_sigterm)
        logger.info("[STARTUP] SIGTERM/SIGINT handlers registered")
    except (ValueError, OSError) as e:
        # Can fail if not in main thread (e.g. gunicorn worker)
        logger.warning(f"[STARTUP] Could not register signal handlers: {e}")

    # Start main event loop in dedicated thread and run startup
    run_async(on_startup())

    logger.info("[STARTUP] Application initialized")


# ===== AUTO-INITIALIZE ON IMPORT =====
# Thread-safe initialization
_init_lock = threading.Lock()
_initialized = False


def _ensure_initialized():
    """Ensure bot is initialized (thread-safe, called on first request)"""
    global _initialized
    
    with _init_lock:
        if _initialized and bot is not None and dp is not None:
            return
        
        if not _initialized:
            _initialized = True
            init_app()


def _auto_init():
    """Auto-initialize on module import (for gunicorn)"""
    # Skip in test environment
    if os.environ.get('PYTEST_CURRENT_TEST') or os.environ.get('TESTING'):
        return
    # Initialize synchronously - no threading delays
    # This ensures the bot is ready before first request
    try:
        _ensure_initialized()
    except Exception as e:
        logger.critical(f"Auto-init failed: {e}")
        # Exit so Railway restarts with fresh state
        os._exit(1)


# Auto-init when imported (e.g., by gunicorn)
_auto_init()


if __name__ == '__main__':
    # Direct run (python bot.py)
    init_app()

    # Run Flask (blocking)
    try:
        run_flask()
    except Exception as e:
        logger.critical(f"Flask crashed: {e}")
        import sys
        sys.exit(1)
    finally:
        run_async(on_shutdown())