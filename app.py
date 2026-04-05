"""Application startup, shutdown, message processing -- main entry point."""

__all__ = [
    'setup_handlers', 'flush_media_group', '_resolve_chat_and_user_sync',
    'process_incoming_message', 'on_startup', 'on_shutdown',
    'run_flask', '_handle_sigterm', 'init_app', '_ensure_initialized',
    '_auto_init',
]

import os
import asyncio
import time
import logging
import threading
import signal

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, Update, ContentType
from aiogram.filters import CommandStart
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType

import shared
from shared import (flask_app, _shutdown_event, _health_state,
                    run_async, get_main_loop,
                    media_group_buffer, media_group_flushed,
                    BOT_TOKEN, DATABASE_URL, DATABASE_URL_DIRECT, REDIS_URL,
                    WEBHOOK_PATH, WEBHOOK_SECRET, BASE_URL, N8N_WEBHOOK_URL,
                    INCOMING_QUEUE_KEY)
from db_helpers import (get_or_create_user, get_or_create_chat, get_or_create_group,
                        add_user_to_group, _mark_group_as_forum, save_forum_topic,
                        update_forum_topic_name, update_forum_topic_closed,
                        save_message_to_db, _update_media_group_file, _send_n8n_webhook)
from db import get_db_pool
from sender_bot import SenderBot
from pg_listener import pg_notify_listener_worker
from stuck_retry import stuck_messages_retry_worker
from incoming_consumer import incoming_message_consumer
from watchdog import thread_watchdog, _graceful_exit

# Import these to register Flask routes on shared.flask_app
import health      # noqa: F401 — registers /health
import endpoints   # noqa: F401 — registers all HTTP routes

logger = logging.getLogger(__name__)


# ============== AIOGRAM MESSAGE HANDLERS ==============

def setup_handlers():
    """Setup aiogram message handlers"""
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
        await process_incoming_message(message, 'text')

    @router.message(F.content_type == ContentType.VOICE)
    async def handle_voice_message(message: Message):
        await process_incoming_message(message, 'voice')

    @router.message(F.content_type == ContentType.VIDEO_NOTE)
    async def handle_video_note(message: Message):
        await process_incoming_message(message, 'video_note')

    @router.message(F.content_type == ContentType.PHOTO)
    async def handle_photo(message: Message):
        await process_incoming_message(message, 'photo')

    @router.message(F.content_type == ContentType.DOCUMENT)
    async def handle_document(message: Message):
        await process_incoming_message(message, 'document')

    @router.message(F.content_type == ContentType.STICKER)
    async def handle_sticker(message: Message):
        await process_incoming_message(message, 'sticker')

    @router.message(F.content_type == ContentType.VIDEO)
    async def handle_video(message: Message):
        await process_incoming_message(message, 'video')

    # ========== FORUM TOPIC EVENT HANDLERS ==========

    @router.message(F.content_type == ContentType.FORUM_TOPIC_CREATED)
    async def handle_forum_topic_created(message: Message):
        try:
            chat = message.chat
            topic = message.forum_topic_created
            telegram_group_id = abs(chat.id)
            thread_id = message.message_thread_id

            await asyncio.to_thread(
                save_forum_topic,
                chat_id=telegram_group_id,
                topic_id=thread_id,
                name=topic.name,
                icon_color=topic.icon_color
            )
            await asyncio.to_thread(_mark_group_as_forum, telegram_group_id)
            logger.info(f"Forum topic created: group={telegram_group_id}, topic={thread_id}, name='{topic.name}'")
        except Exception as e:
            logger.error(f"Error handling forum_topic_created: {e}")

    @router.message(F.content_type == ContentType.FORUM_TOPIC_EDITED)
    async def handle_forum_topic_edited(message: Message):
        try:
            chat = message.chat
            topic = message.forum_topic_edited
            telegram_group_id = abs(chat.id)
            thread_id = message.message_thread_id

            await asyncio.to_thread(
                update_forum_topic_name,
                chat_id=telegram_group_id,
                topic_id=thread_id,
                name=topic.name,
                icon_color=getattr(topic, 'icon_color', None)
            )
            logger.info(f"Forum topic edited: group={telegram_group_id}, topic={thread_id}, name='{topic.name}'")
        except Exception as e:
            logger.error(f"Error handling forum_topic_edited: {e}")

    @router.message(F.content_type == ContentType.FORUM_TOPIC_CLOSED)
    async def handle_forum_topic_closed(message: Message):
        try:
            chat = message.chat
            telegram_group_id = abs(chat.id)
            thread_id = message.message_thread_id

            await asyncio.to_thread(
                update_forum_topic_closed,
                chat_id=telegram_group_id,
                topic_id=thread_id,
                is_closed=True
            )
            logger.info(f"Forum topic closed: group={telegram_group_id}, topic={thread_id}")
        except Exception as e:
            logger.error(f"Error handling forum_topic_closed: {e}")

    @router.message(F.content_type == ContentType.FORUM_TOPIC_REOPENED)
    async def handle_forum_topic_reopened(message: Message):
        try:
            chat = message.chat
            telegram_group_id = abs(chat.id)
            thread_id = message.message_thread_id

            await asyncio.to_thread(
                update_forum_topic_closed,
                chat_id=telegram_group_id,
                topic_id=thread_id,
                is_closed=False
            )
            logger.info(f"Forum topic reopened: group={telegram_group_id}, topic={thread_id}")
        except Exception as e:
            logger.error(f"Error handling forum_topic_reopened: {e}")

    return router


# ============== MEDIA GROUP FLUSH ==============

async def flush_media_group(group_id: str):
    """Flush buffered media group after delay."""
    await asyncio.sleep(1.5)

    group_data = media_group_buffer.pop(group_id, None)
    if not group_data:
        return

    messages = group_data['messages']
    if not messages:
        return

    messages.sort(key=lambda m: m['message_id'])

    files_path = [m['file_id'] for m in messages]

    first_msg = messages[0]

    types = set(m['media_type'] for m in messages)
    if len(types) == 1:
        type_of_message = list(types)[0]
    else:
        type_of_message = 'media_group'

    caption = ''
    for m in messages:
        if m.get('caption'):
            caption = m['caption']
            break

    logger.info(f"Flushing media group {group_id}: {len(files_path)} files, type={type_of_message}")

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
            files_path=files_path,
            message_thread_id=first_msg.get('message_thread_id')
        )

        media_group_flushed[group_id] = {
            'history_id': history_id,
            'flushed_at': time.time(),
            'files_path': files_path.copy()
        }

        cutoff = time.time() - 60
        to_remove = [k for k, v in media_group_flushed.items() if v['flushed_at'] < cutoff]
        for k in to_remove:
            del media_group_flushed[k]

    except Exception as e:
        logger.error(f"Error saving media group {group_id}: {e}")


# ============== MESSAGE PROCESSING ==============

def _resolve_chat_and_user_sync(chat, user, is_group):
    """Sync DB work for resolving chat/user IDs."""
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
    """Process incoming message and save to database."""
    try:
        chat = message.chat
        user = message.from_user

        is_group = chat.type in [ChatType.GROUP, ChatType.SUPERGROUP]

        internal_chat_id, sender_user_id, sender_telegram_id = await asyncio.to_thread(
            _resolve_chat_and_user_sync, chat, user, is_group
        )

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

        reply_to = None
        if message.reply_to_message:
            reply_to = message.reply_to_message.message_id

        thread_id = message.message_thread_id

        if thread_id is not None and is_group:
            try:
                await asyncio.to_thread(_mark_group_as_forum, abs(chat.id))
            except Exception as e:
                logger.warning(f"Failed to mark group as forum: {e}")

        # Check if this is part of media group (album)
        if message.media_group_id and message_type in ('photo', 'video', 'document'):
            group_id = message.media_group_id

            if group_id in media_group_flushed:
                flushed = media_group_flushed[group_id]
                if file_id and file_id not in flushed['files_path']:
                    flushed['files_path'].append(file_id)
                    try:
                        await asyncio.to_thread(
                            _update_media_group_file, flushed['files_path'], flushed['history_id']
                        )
                        logger.info(f"Late arrival for media group {group_id}: added file, total={len(flushed['files_path'])}")
                    except Exception as e:
                        logger.error(f"Error updating media group {group_id} with late file: {e}")
                return

            if group_id not in media_group_buffer:
                media_group_buffer[group_id] = {
                    'messages': [],
                    'chat_id': internal_chat_id
                }
                asyncio.create_task(flush_media_group(group_id))

            media_group_buffer[group_id]['messages'].append({
                'message_id': message.message_id,
                'file_id': file_id,
                'media_type': message_type,
                'caption': message_text,
                'chat_id': internal_chat_id,
                'sender_telegram_id': sender_telegram_id if is_group else None,
                'reply_to': reply_to,
                'message_thread_id': thread_id
            })

            logger.info(f"Buffered media group {group_id}: msg_id={message.message_id}, type={message_type}")
            return

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
            files_path=files_path_arr,
            message_thread_id=thread_id
        )

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


# ============== STARTUP / SHUTDOWN ==============

async def on_startup():
    """Called on bot startup"""
    logger.info("[STARTUP] Starting AEON Telegram Bot...")

    # Init bot and dispatcher
    shared.bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN))
    shared.dp = Dispatcher()

    # Setup handlers
    shared.router = setup_handlers()
    shared.dp.include_router(shared.router)

    # Init database pool
    db_ready = False
    for db_attempt in range(6):
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
        logger.error("[STARTUP] DB pool not available -- will retry in background threads")

    # Wait for Redis
    from redis_client import get_redis, wait_for_redis
    wait_for_redis(max_wait=30)

    shared.redis_client = get_redis()
    if shared.redis_client:
        logger.info("[STARTUP] Redis client ready")

    # Start pg_notify listener
    if DATABASE_URL_DIRECT and REDIS_URL:
        shared.pg_listener_thread = threading.Thread(
            target=pg_notify_listener_worker,
            args=(REDIS_URL, DATABASE_URL_DIRECT),
            daemon=True,
            name="PgNotifyListener"
        )
        shared.pg_listener_thread.start()
        logger.info("[STARTUP] pg_notify listener thread started")

        shared.retry_worker_thread = threading.Thread(
            target=stuck_messages_retry_worker,
            daemon=True,
            name="StuckMessagesRetry"
        )
        shared.retry_worker_thread.start()
        logger.info("[STARTUP] Stuck messages retry worker started")

    # Init sender
    if REDIS_URL:
        shared.sender_bot = SenderBot(shared.bot, REDIS_URL, DATABASE_URL)
        shared.sender_bot.start()
        logger.info("[STARTUP] Sender started")

    # Start incoming message consumer
    if REDIS_URL:
        shared.incoming_consumer_thread = threading.Thread(
            target=incoming_message_consumer,
            daemon=True,
            name="IncomingConsumer"
        )
        shared.incoming_consumer_thread.start()
        logger.info("[STARTUP] Incoming message consumer started")

    # Start thread watchdog
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
        await shared.bot.set_webhook(
            url=webhook_url,
            secret_token=WEBHOOK_SECRET if WEBHOOK_SECRET else None,
            drop_pending_updates=False
        )
        logger.info(f"[STARTUP] Webhook set: {webhook_url}")
    else:
        logger.warning("[STARTUP] BASE_URL not set, webhook not configured")

    logger.info("[STARTUP] Startup complete")


async def on_shutdown():
    """Called on bot shutdown"""
    logger.info("[SHUTDOWN] Shutting down...")

    _shutdown_event.set()

    try:
        if shared.sender_bot:
            shared.sender_bot.stop()
            logger.info("[SHUTDOWN] SenderBot stopped")
    except Exception as e:
        logger.error(f"[SHUTDOWN] SenderBot stop error: {e}")

    try:
        if shared.bot:
            await shared.bot.delete_webhook()
            await shared.bot.session.close()
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
    logger.info(f"Starting Flask on port {port}")
    flask_app.run(host='0.0.0.0', port=port, threaded=True)


def _handle_sigterm(signum, frame):
    """Handle SIGTERM/SIGINT for graceful shutdown"""
    sig_name = signal.Signals(signum).name
    logger.info(f"[SHUTDOWN] {sig_name} received, initiating graceful shutdown...")
    _graceful_exit(0, timeout=15)


def init_app():
    """Initialize the application (for gunicorn or direct run)"""
    # Only initialize once
    if shared.bot is not None and shared.dp is not None:
        return

    logger.info("[STARTUP] Initializing AEON Telegram Bot...")

    # Register signal handlers for graceful shutdown
    try:
        signal.signal(signal.SIGTERM, _handle_sigterm)
        signal.signal(signal.SIGINT, _handle_sigterm)
        logger.info("[STARTUP] SIGTERM/SIGINT handlers registered")
    except (ValueError, OSError) as e:
        logger.warning(f"[STARTUP] Could not register signal handlers: {e}")

    # Start main event loop in dedicated thread and run startup
    run_async(on_startup())

    logger.info("[STARTUP] Application initialized")


# ===== AUTO-INITIALIZE ON IMPORT =====
_init_lock = threading.Lock()
_initialized = False


def _ensure_initialized():
    """Ensure bot is initialized (thread-safe, called on first request)"""
    global _initialized

    with _init_lock:
        if _initialized and shared.bot is not None and shared.dp is not None:
            return

        if not _initialized:
            _initialized = True
            init_app()


# Make _ensure_initialized available in shared for endpoints to use
shared._ensure_initialized = _ensure_initialized


def _auto_init():
    """Auto-initialize on module import (for gunicorn)"""
    if os.environ.get('PYTEST_CURRENT_TEST') or os.environ.get('TESTING'):
        return
    try:
        _ensure_initialized()
    except Exception as e:
        logger.critical(f"Auto-init failed: {e}")
        os._exit(1)


# Auto-init when imported (e.g., by gunicorn)
_auto_init()


if __name__ == '__main__':
    init_app()

    try:
        run_flask()
    except Exception as e:
        logger.critical(f"Flask crashed: {e}")
        import sys
        sys.exit(1)
    finally:
        run_async(on_shutdown())
