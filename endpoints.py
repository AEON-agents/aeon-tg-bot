"""All HTTP routes (Flask endpoints)."""

import os
import json
import time
import base64
import logging
import tempfile
from io import BytesIO

from flask import request, jsonify, send_file
from aiogram.types import Update
from aiogram.enums import ParseMode, ChatAction

import shared
from shared import (flask_app, run_async, _check_rate_limit,
                    _queue_task, _resolve_tg_id,
                    WEBHOOK_SECRET, WEBHOOK_PATH, TEMP_FILES_DIR)
from db_helpers import resolve_telegram_chat_id, get_chat_info_by_telegram_id
from stuck_retry import retry_stuck_messages
from db import db_cursor

logger = logging.getLogger(__name__)


# ============== FLASK WEBHOOK ENDPOINT ==============

@flask_app.route(WEBHOOK_PATH, methods=['POST'])
def telegram_webhook():
    """Handle incoming Telegram webhook updates"""
    # Ensure bot is initialized
    if shared.dp is None or shared.bot is None:
        try:
            if shared._ensure_initialized:
                shared._ensure_initialized()
        except Exception as e:
            logger.error(f"Initialization failed: {e}")

        if shared.dp is None or shared.bot is None:
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

        run_async(shared.dp.feed_update(shared.bot, update))

        return jsonify({"ok": True})

    except Exception as e:
        logger.exception(f"[WEBHOOK] Webhook error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== QUEUE MANAGEMENT ==============

@flask_app.route('/telegram/queue/peek', methods=['GET'])
def api_queue_peek():
    """Peek at send queue contents without removing them"""
    try:
        limit = request.args.get('limit', 50, type=int)
        items = shared.redis_client.lrange('telegram:send_queue', 0, limit - 1)
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
        length = shared.redis_client.llen('telegram:send_queue')
        shared.redis_client.delete('telegram:send_queue')
        logger.warning(f"[QUEUE] Flushed {length} items from send_queue")
        return jsonify({"success": True, "flushed": length})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/queue/dedup', methods=['POST'])
def api_queue_dedup():
    """Remove duplicate messages from queue, keep only unique chat_history_id entries"""
    try:
        items = shared.redis_client.lrange('telegram:send_queue', 0, -1)
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
        pipe = shared.redis_client.pipeline()
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
    """Force re-send specific message IDs by clearing dedup keys and re-queuing."""
    try:
        data = request.get_json() or {}
        ids = data.get('ids', [])
        if not ids:
            return jsonify({"error": "ids required"}), 400

        # Clear dedup keys
        for msg_id in ids:
            shared.redis_client.delete(f'telegram:sent:{msg_id}')
            for key in shared.redis_client.scan_iter(f'tg:dedup:*'):
                val = shared.redis_client.get(key)
                if val and str(msg_id).encode() in val:
                    shared.redis_client.delete(key)

        count = retry_stuck_messages(max_age_minutes=1440, force_send=True)
        return jsonify({"success": True, "dedup_cleared": ids, "requeued": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/retry_stuck', methods=['POST'])
def api_retry_stuck():
    """Retry sending stuck messages (queued but not sent)."""
    try:
        data = request.get_json() or {}
        max_age = data.get('max_age_minutes', 60)
        count = retry_stuck_messages(max_age_minutes=max_age)
        return jsonify({"success": True, "requeued": count})
    except Exception as e:
        logger.error(f"Retry stuck error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== API ENDPOINTS ==============

@flask_app.route('/telegram/send', methods=['POST'])
def api_send_message():
    """Send a text message to a user/chat"""
    try:
        data = request.get_json()
        telegram_id = _resolve_tg_id(data)

        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400

        if not _check_rate_limit(f"send:{telegram_id}", max_per_minute=30):
            return jsonify({"error": "Rate limited", "retry_after": 60}), 429

        message = data.get('message', '')
        reply_to = data.get('reply_to_message_id')

        async def send():
            from aiogram.enums import ParseMode
            from aiogram.exceptions import TelegramBadRequest

            try:
                msg = await shared.bot.send_message(
                    chat_id=telegram_id,
                    text=message,
                    reply_to_message_id=reply_to,
                    parse_mode=ParseMode.MARKDOWN
                )
                return msg.message_id
            except TelegramBadRequest as e:
                error_msg = str(e).lower()
                if "can't parse" in error_msg or "parse" in error_msg:
                    logger.warning(f"Markdown failed, sending plain: {e}")
                    msg = await shared.bot.send_message(
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
        logger.error(f"Send message error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/typing', methods=['POST'])
def api_send_typing():
    """Send typing indicator"""
    try:
        data = request.get_json()
        telegram_id = _resolve_tg_id(data)

        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400

        if not _check_rate_limit(f"typing:{telegram_id}", max_per_minute=10):
            return jsonify({"error": "Rate limited", "retry_after": 60}), 429

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
            await shared.bot.send_chat_action(chat_id=telegram_id, action=action)

        run_async(send_action())

        return jsonify({"success": True})

    except Exception as e:
        logger.error(f"Typing error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== MEDIA ENDPOINTS ==============

@flask_app.route('/api/media/download', methods=['POST'])
def api_media_download():
    """Download media by Telegram file_id and save to cache."""
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

        os.makedirs(save_dir, exist_ok=True)

        async def download():
            file = await shared.bot.get_file(file_id)
            file_bytes = await shared.bot.download_file(file.file_path)
            ext = os.path.splitext(file.file_path)[1] if file.file_path else '.bin'
            if not ext:
                ext = '.bin'
            filename = f"media_{file_id[:20]}{ext}"
            file_path = os.path.join(save_dir, filename)
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
    """Get media file contents (base64) from cache or download on-demand."""
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
                file = await shared.bot.get_file(file_id)
                file_bytes = await shared.bot.download_file(file.file_path)
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
    """Serve media file directly from cache."""
    cache_dir = os.environ.get('MEDIA_CACHE_DIR', '/tmp/media_cache')
    file_path = os.path.join(cache_dir, str(history_id), filename)

    if not os.path.exists(file_path):
        return jsonify({"error": "File not found"}), 404

    return send_file(file_path)


# ============== REACTION / STICKER / VIDEO NOTE / DOCUMENT ==============

@flask_app.route('/telegram/reaction', methods=['POST'])
def api_set_reaction():
    """Set reaction on a message (queued)"""
    try:
        data = request.get_json()
        telegram_id = _resolve_tg_id(data)

        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400

        message_id = data.get('message_id')
        if not message_id:
            return jsonify({"error": "message_id required"}), 400

        _queue_task(telegram_id, 'reaction', {
            'reaction_emoji': data.get('emoji', '\U0001f44d'),
            'reply_to_message_id': message_id,
        })
        return jsonify({"queued": True})

    except Exception as e:
        logger.error(f"Reaction queue error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/sticker', methods=['POST'])
def api_send_sticker():
    """Send a sticker (queued)"""
    try:
        data = request.get_json()
        telegram_id = _resolve_tg_id(data)
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400

        _queue_task(telegram_id, 'sticker', {
            'sticker_short_name': data.get('sticker_set', 'AnimatedEmojies'),
            'sticker_emoji': data.get('emoji'),
            'reply_to_message_id': data.get('reply_to_message_id'),
        })
        return jsonify({"queued": True})
    except Exception as e:
        logger.error(f"Sticker queue error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/video_note', methods=['POST'])
def api_send_video_note():
    """Send a video note (circle) -- queued"""
    try:
        data = request.get_json()
        telegram_id = _resolve_tg_id(data)
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400

        _queue_task(telegram_id, 'video_note', {
            'video_url': data.get('video_url'),
            'video_data': data.get('video_data'),
            'reply_to_message_id': data.get('reply_to_message_id'),
        })
        return jsonify({"queued": True})
    except Exception as e:
        logger.error(f"Video note queue error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/voice/download', methods=['GET', 'POST'])
def api_download_voice():
    """Download voice message by file_id"""
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
            file = await shared.bot.get_file(file_id)
            file_bytes = await shared.bot.download_file(file.file_path)
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
        logger.error(f"Voice download error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/document', methods=['POST'])
def api_send_document():
    """Send a document (queued)"""
    try:
        data = request.get_json()
        telegram_id = _resolve_tg_id(data)
        if not telegram_id:
            return jsonify({"error": "telegram_id or chat_id required"}), 400

        _queue_task(telegram_id, 'document', {
            'document_url': data.get('document_url'),
            'document_data': data.get('document_data'),
            'filename': data.get('filename', 'document.pdf'),
            'caption': data.get('caption', ''),
            'message': data.get('message', ''),
            'file_format': data.get('file_format', 'docx'),
            'reply_to_message_id': data.get('reply_to_message_id'),
        })
        return jsonify({"queued": True})
    except Exception as e:
        logger.error(f"Document queue error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== SENDER STATS API ==============

@flask_app.route('/telegram/sender/stats', methods=['GET'])
def api_sender_stats():
    """Get sender statistics"""
    if shared.sender_bot:
        return jsonify({
            "success": True,
            "stats": shared.sender_bot.get_stats()
        })
    return jsonify({"error": "Sender not initialized"}), 500


@flask_app.route('/telegram/sender/start', methods=['POST'])
def api_sender_start():
    """Start the sender if stopped"""
    from sender_bot import SenderBot

    if shared.sender_bot and shared.sender_bot.is_running:
        return jsonify({"message": "Sender already running"})

    if not shared.sender_bot:
        if shared.REDIS_URL:
            shared.sender_bot = SenderBot(shared.bot, shared.REDIS_URL, shared.DATABASE_URL)

    if shared.sender_bot:
        shared.sender_bot.start()
        return jsonify({"success": True, "message": "Sender started"})

    return jsonify({"error": "Cannot create sender"}), 500


@flask_app.route('/telegram/sender/stop', methods=['POST'])
def api_sender_stop():
    """Stop the sender"""
    if shared.sender_bot:
        shared.sender_bot.stop()
        return jsonify({"success": True, "message": "Sender stopped"})
    return jsonify({"error": "Sender not initialized"}), 500


# ============== MEDIA DOWNLOAD ENDPOINT (for n8n) ==============

@flask_app.route('/telegram/message/<telegram_id>/<int:message_id>/media/download', methods=['GET'])
def api_download_media(telegram_id, message_id):
    """Download document by message"""
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

        file_id = files_path[0]

        async def download():
            file = await shared.bot.get_file(file_id)
            file_bytes = await shared.bot.download_file(file.file_path)
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
    """Download any file from Telegram by file_id"""
    try:
        data = request.get_json()
        file_id = data.get('file_id')
        output_format = data.get('format', 'base64')
        filename = data.get('filename', 'file')
        mime_type = data.get('mime', 'application/octet-stream')

        if not file_id:
            return jsonify({"error": "file_id required"}), 400

        async def download():
            file = await shared.bot.get_file(file_id)
            file_bytes = await shared.bot.download_file(file.file_path)
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
        logger.error(f"File download error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== TEMP FILES ENDPOINT ==============

@flask_app.route('/files/temp/<filename>', methods=['GET'])
def serve_temp_file(filename):
    """Serve temporary files (voice, video, etc.)"""
    try:
        filepath = os.path.join(TEMP_FILES_DIR, filename)
        if os.path.exists(filepath):
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
    """Set reaction on a message (queued)"""
    try:
        tg_id = int(telegram_id)
        data = request.get_json() or {}
        emoji = data.get('emoji', '\U0001f44d')

        _queue_task(tg_id, 'reaction', {
            'reaction_emoji': emoji,
            'reply_to_message_id': message_id,
        })
        return jsonify({"queued": True})

    except Exception as e:
        logger.error(f"Reaction queue error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/message/<telegram_id>/<int:message_id>/reaction', methods=['DELETE'])
def api_remove_message_reaction(telegram_id, message_id):
    """Remove reaction from a message (rate limited)"""
    try:
        tg_id = int(telegram_id)

        if not _check_rate_limit(f"reaction_del:{tg_id}", max_per_minute=20):
            return jsonify({"error": "Rate limited", "retry_after": 60}), 429

        async def remove_reaction():
            await shared.bot.set_message_reaction(
                chat_id=tg_id,
                message_id=message_id,
                reaction=[]
            )

        run_async(remove_reaction())

        return jsonify({"success": True})

    except Exception as e:
        logger.error(f"Remove reaction error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== PIN/UNPIN MESSAGE ENDPOINTS ==============

@flask_app.route('/telegram/chat/<chat_id>/pin', methods=['POST'])
def api_pin_message(chat_id):
    """Pin a message in chat (queued)"""
    try:
        tg_chat_id = int(chat_id)
        data = request.get_json() or {}
        message_id = data.get('message_id')
        if not message_id:
            return jsonify({"error": "message_id required"}), 400

        _queue_task(tg_chat_id, 'pin', {
            'message_id': message_id,
            'notify': data.get('notify', True),
        })
        return jsonify({"queued": True})
    except Exception as e:
        logger.error(f"Pin queue error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/chat/<chat_id>/unpin', methods=['POST'])
def api_unpin_message(chat_id):
    """Unpin a message in chat (queued)"""
    try:
        tg_chat_id = int(chat_id)
        data = request.get_json() or {}

        _queue_task(tg_chat_id, 'unpin', {
            'message_id': data.get('message_id'),
        })
        return jsonify({"queued": True})
    except Exception as e:
        logger.error(f"Unpin queue error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/telegram/chat/<chat_id>/unpin_all', methods=['POST'])
def api_unpin_all_messages(chat_id):
    """Unpin all messages in chat (queued)"""
    try:
        tg_chat_id = int(chat_id)
        _queue_task(tg_chat_id, 'unpin_all', {})
        return jsonify({"queued": True})
    except Exception as e:
        logger.error(f"Unpin all queue error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== FORWARD MESSAGE ENDPOINT ==============

@flask_app.route('/telegram/forward', methods=['POST'])
def api_forward_message():
    """Forward a message from one chat to another (queued)."""
    try:
        data = request.get_json()
        chat_id = data.get('chat_id')
        from_chat_id = data.get('from_chat_id')
        message_id = data.get('message_id')

        if not all([chat_id, from_chat_id, message_id]):
            return jsonify({"error": "chat_id, from_chat_id, message_id required"}), 400

        _queue_task(chat_id, 'forward', {
            'from_chat_id': from_chat_id,
            'message_id': message_id,
        })
        return jsonify({"queued": True})
    except Exception as e:
        logger.error(f"Forward queue error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== INTERNAL UPDATE ENDPOINT ==============

@flask_app.route('/internal/update_message', methods=['POST'])
def api_internal_update_message():
    """Internal endpoint for updating message status from triggers"""
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
        logger.error(f"Internal update error: {e}")
        return jsonify({"error": str(e)}), 500


# ============== AVATAR ENDPOINTS ==============

@flask_app.route('/api/user/<int:telegram_id>/avatar', methods=['GET'])
def api_get_user_avatar(telegram_id):
    """Get user avatar from Telegram."""
    try:
        async def get_avatar():
            photos = await shared.bot.get_user_profile_photos(user_id=telegram_id, limit=1)
            if not photos.photos:
                return None
            largest = photos.photos[0][-1]
            file = await shared.bot.get_file(largest.file_id)
            file_bytes = await shared.bot.download_file(file.file_path)
            return file_bytes.read()

        avatar_bytes = run_async(get_avatar())
        if avatar_bytes is None:
            return jsonify({"error": "No photo"}), 404

        return send_file(
            BytesIO(avatar_bytes),
            mimetype='image/jpeg',
            max_age=3600
        )

    except Exception as e:
        logger.error(f"Get user avatar error: {e}")
        return jsonify({"error": str(e)}), 500


@flask_app.route('/api/chat/<int:chat_id>/avatar', methods=['GET'])
def api_get_chat_avatar(chat_id):
    """Get chat avatar from Telegram."""
    try:
        async def get_avatar():
            chat = await shared.bot.get_chat(chat_id)
            if not chat.photo:
                return None
            file = await shared.bot.get_file(chat.photo.big_file_id)
            file_bytes = await shared.bot.download_file(file.file_path)
            return file_bytes.read()

        avatar_bytes = run_async(get_avatar())
        if avatar_bytes is None:
            return jsonify({"error": "No photo"}), 404

        return send_file(
            BytesIO(avatar_bytes),
            mimetype='image/jpeg',
            max_age=3600
        )

    except Exception as e:
        logger.error(f"Get chat avatar error: {e}")
        return jsonify({"error": str(e)}), 500
