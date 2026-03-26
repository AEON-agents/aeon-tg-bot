"""Message type handlers for SenderBot"""
from typing import Dict, Any, Optional, Callable, Awaitable, TYPE_CHECKING
from dataclasses import dataclass
import logging
import base64
import os

if TYPE_CHECKING:
    from sender_bot import SenderBot

logger = logging.getLogger(__name__)


@dataclass
class HandlerResult:
    """Result of message handler execution"""
    success: bool
    tg_message_id: Optional[int] = None
    error: Optional[str] = None
    stat_key: Optional[str] = None  # Key to increment in stats


# Type alias for handler function
HandlerFunc = Callable[['SenderBot', int, Dict[str, Any]], Awaitable[HandlerResult]]


class MessageHandlers:
    """Registry of message type handlers"""

    _handlers: Dict[str, HandlerFunc] = {}

    @classmethod
    def register(cls, msg_type: str):
        """Decorator to register handler for message type"""
        def decorator(func: HandlerFunc):
            cls._handlers[msg_type] = func
            return func
        return decorator

    @classmethod
    def get(cls, msg_type: str) -> Optional[HandlerFunc]:
        """Get handler for message type"""
        return cls._handlers.get(msg_type)

    @classmethod
    async def handle(cls, sender: 'SenderBot', chat_id: int,
                     msg_type: str, body: Dict) -> HandlerResult:
        """Execute handler for message type"""
        handler = cls.get(msg_type)
        if handler:
            return await handler(sender, chat_id, body)
        # Default to text
        return await cls._handlers.get('text')(sender, chat_id, body)


# ============== HELPER FUNCTIONS ==============

async def _get_media_data(body: Dict, media_type: str) -> Optional[bytes]:
    """Get media data from URL or base64.

    Checks multiple URL fields: {type}_url, video_url, media_url
    Uses aiohttp for async download.
    """
    import aiohttp

    # Check multiple URL keys
    url = (
        body.get(f'{media_type}_url') or
        body.get('file_url') or
        body.get('video_url') or
        body.get('media_url')
    )

    if url:
        # Skip if it's a Telegram file_id (not a URL)
        # file_ids start with specific prefixes and don't contain '/'
        if '/' not in url and url.startswith(('AgAC', 'AQAD', 'BAA', 'CAA', 'DAA', 'DQA', 'CgA')):
            logger.warning(f"⚠️ Got file_id instead of URL: {url[:20]}... - need to download via bot")
            return None

        # Auto-expand relative paths to full URL
        if not url.startswith(('http://', 'https://')):
            base_url = os.environ.get('BASE_URL', 'https://your-app.up.railway.app')
            url = f"{base_url.rstrip('/')}/workspace/{url.lstrip('/')}"
            logger.info(f"📎 Expanded relative path to: {url}")

        # Download from URL using aiohttp
        try:
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        logger.info(f"✅ Downloaded media from URL: {len(data)} bytes")
                        return data
                    else:
                        logger.error(f"❌ Failed to download media: HTTP {resp.status}")
                        return None
        except Exception as e:
            logger.error(f"❌ Error downloading media from URL: {e}")
            return None

    # Check base64 data
    data_key = f'{media_type}_data'
    if body.get(data_key):
        data = body[data_key]
        if isinstance(data, str):
            return base64.b64decode(data)
        return data

    # Also check 'base64' field directly
    if body.get('base64'):
        return base64.b64decode(body['base64'])

    return None


def _get_reply_to(body: Dict) -> Optional[int]:
    """Get reply_to from body"""
    return body.get('reply_to_message_id') or body.get('reply_to')


def _get_thread_id(body: Dict) -> Optional[int]:
    """Get message_thread_id from body (for forum topics)"""
    return body.get('message_thread_id')


# ============== HANDLERS ==============

@MessageHandlers.register('text')
async def handle_text(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle text message"""
    text = body.get('message', '')
    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    if not text:
        return HandlerResult(success=False, error="Empty text message")

    tg_msg_id = await sender._send_text(chat_id, text, reply_to, message_thread_id=thread_id)
    return HandlerResult(success=True, tg_message_id=tg_msg_id)


@MessageHandlers.register('reaction')
async def handle_reaction(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle reaction"""
    emoji = body.get('reaction_emoji')
    reply_to_msg_id = body.get('reply_to_message_id')

    if not reply_to_msg_id:
        return HandlerResult(success=False, error="reply_to_message_id required for reaction")

    logger.info(f"Processing reaction: chat={chat_id}, msg_id={reply_to_msg_id}, emoji='{emoji}'")

    await sender._send_reaction(chat_id, reply_to_msg_id, emoji)

    stat_key = 'reactions_sent' if emoji and emoji.strip() else 'reactions_removed'
    return HandlerResult(success=True, stat_key=stat_key)


@MessageHandlers.register('delete')
async def handle_delete(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle message deletion"""
    tg_message_id = body.get('tg_message_id')

    if not tg_message_id:
        return HandlerResult(success=False, error="tg_message_id required for delete")

    logger.info(f"Processing delete: chat={chat_id}, msg_id={tg_message_id}")

    deleted = await sender._delete_message(chat_id, int(tg_message_id))

    if deleted:
        return HandlerResult(success=True, stat_key='messages_deleted')
    else:
        logger.warning(f"Could not delete message: chat={chat_id}, msg_id={tg_message_id}")
        return HandlerResult(success=True)  # Don't retry


@MessageHandlers.register('sticker')
async def handle_sticker(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle sticker"""
    sticker_set = body.get('sticker_short_name')
    sticker_emoji = body.get('sticker_emoji')
    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    logger.info(f"Processing sticker: set={sticker_set}, emoji={sticker_emoji}")

    tg_msg_id = await sender._send_sticker(
        chat_id,
        sticker_set_name=sticker_set,
        emoji=sticker_emoji,
        reply_to=reply_to,
        message_thread_id=thread_id
    )
    return HandlerResult(success=True, tg_message_id=tg_msg_id, stat_key='stickers_sent')


@MessageHandlers.register('video_note')
async def handle_video_note(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle video note (circle)"""
    video_data = await _get_media_data(body, 'video')
    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    if not video_data:
        return HandlerResult(success=False, error="video_url or video_data required")

    tg_msg_id = await sender._send_video_note(chat_id, video_data, reply_to, message_thread_id=thread_id)
    return HandlerResult(success=True, tg_message_id=tg_msg_id, stat_key='video_notes_sent')


@MessageHandlers.register('document')
async def handle_document(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle document - either from URL or generated from markdown"""
    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    # Try to get document from URL first
    doc_data = await _get_media_data(body, 'document')
    if doc_data:
        caption = body.get('caption', '')
        # Extract filename from URL if not provided
        doc_url = body.get('document_url') or body.get('video_url')
        filename = None
        if doc_url and '/' in doc_url:
            filename = doc_url.split('/')[-1].split('?')[0]

        tg_msg_id = await sender._send_document_file(
            chat_id, doc_data, filename=filename, caption=caption, reply_to=reply_to,
            message_thread_id=thread_id
        )
        return HandlerResult(success=True, tg_message_id=tg_msg_id, stat_key='documents_sent')

    # Fallback: generate document from markdown
    markdown_text = body.get('message', '')
    if not markdown_text:
        return HandlerResult(success=False, error="document_url or message required")

    file_format = body.get('file_format', 'docx')
    filename = body.get('filename')

    tg_msg_id = await sender._send_document_generated(
        chat_id, markdown_text, file_format, filename, reply_to,
        message_thread_id=thread_id
    )
    return HandlerResult(success=True, tg_message_id=tg_msg_id, stat_key='documents_sent')


@MessageHandlers.register('typing')
async def handle_typing(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle typing indicator"""
    action = body.get('action', 'typing')
    await sender._send_typing(chat_id, action)
    return HandlerResult(success=True)


@MessageHandlers.register('photo')
async def handle_photo(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle photo"""
    photo_data = await _get_media_data(body, 'photo')
    if not photo_data:
        return HandlerResult(success=False, error="photo_url or photo_data required")

    caption = body.get('caption', '')
    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    tg_msg_id = await sender._send_photo(chat_id, photo_data, caption, reply_to, message_thread_id=thread_id)
    return HandlerResult(success=True, tg_message_id=tg_msg_id)


@MessageHandlers.register('voice')
async def handle_voice(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle voice message"""
    voice_data = await _get_media_data(body, 'voice')
    if not voice_data:
        return HandlerResult(success=False, error="voice_url or voice_data required")

    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    tg_msg_id = await sender._send_voice(chat_id, voice_data, reply_to, message_thread_id=thread_id)
    return HandlerResult(success=True, tg_message_id=tg_msg_id)


@MessageHandlers.register('video')
async def handle_video(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle video"""
    video_data = await _get_media_data(body, 'video')
    if not video_data:
        return HandlerResult(success=False, error="video_url or video_data required")

    caption = body.get('caption', '')
    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    tg_msg_id = await sender._send_video(chat_id, video_data, caption, reply_to, message_thread_id=thread_id)
    return HandlerResult(success=True, tg_message_id=tg_msg_id)


@MessageHandlers.register('media_group')
async def handle_media_group(sender: 'SenderBot', chat_id: int, body: Dict) -> HandlerResult:
    """Handle media group (album) - up to 10 photos/videos"""
    media = body.get('media')  # [{type: "photo", url: "..."}, ...]
    if not media or not isinstance(media, list):
        return HandlerResult(success=False, error="media array required")

    if len(media) < 2:
        return HandlerResult(success=False, error="media_group requires at least 2 items")

    if len(media) > 10:
        return HandlerResult(success=False, error="media_group max 10 items")

    caption = body.get('caption', '')
    reply_to = _get_reply_to(body)
    thread_id = _get_thread_id(body)

    tg_msg_ids = await sender._send_media_group(chat_id, media, caption, reply_to, message_thread_id=thread_id)
    # Return first message ID
    first_id = tg_msg_ids[0] if tg_msg_ids else None
    return HandlerResult(success=True, tg_message_id=first_id)
