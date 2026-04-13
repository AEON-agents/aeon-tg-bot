"""Database helper functions -- CRUD for users, chats, groups, messages."""

__all__ = [
    'get_or_create_user', 'get_or_create_chat', 'get_or_create_group',
    'add_user_to_group', '_mark_group_as_forum', 'save_forum_topic',
    'update_forum_topic_name', 'update_forum_topic_closed',
    'save_message_to_db', 'resolve_telegram_chat_id',
    'get_chat_info_by_telegram_id', '_update_media_group_file',
    '_update_message_files_url', '_send_n8n_webhook',
]

import json
import logging
from typing import Optional, Dict

from db import db_cursor
from shared import N8N_WEBHOOK_URL

logger = logging.getLogger(__name__)


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
    """Get or create group in groups_tg.

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
    """Add user to group in groups_users_tg (if not exists)."""
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


def _mark_group_as_forum(telegram_group_id: int):
    """Mark a group as forum (is_forum = true) in groups_tg."""
    try:
        with db_cursor(commit=True) as cur:
            cur.execute(
                "UPDATE groups_tg SET is_forum = true WHERE telegram_group_id = %s AND (is_forum IS NULL OR is_forum = false)",
                (telegram_group_id,)
            )
    except Exception as e:
        logger.warning(f"Failed to mark group {telegram_group_id} as forum: {e}")


def save_forum_topic(chat_id: int, topic_id: int, name: str, icon_color: int = None):
    """Insert or update a forum topic in forum_topics_tg."""
    with db_cursor(commit=True) as cur:
        cur.execute("""
            INSERT INTO public.forum_topics_tg (chat_id, topic_id, name, icon_color)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (chat_id, topic_id) DO UPDATE SET
                name = EXCLUDED.name,
                icon_color = EXCLUDED.icon_color,
                updated_at = NOW()
        """, (chat_id, topic_id, name, icon_color))


def update_forum_topic_name(chat_id: int, topic_id: int, name: str = None, icon_color: int = None):
    """Update forum topic name/icon on edit event."""
    with db_cursor(commit=True) as cur:
        sets = ["updated_at = NOW()"]
        params = []
        if name is not None:
            sets.append("name = %s")
            params.append(name)
        if icon_color is not None:
            sets.append("icon_color = %s")
            params.append(icon_color)
        params.extend([chat_id, topic_id])
        cur.execute(
            f"UPDATE public.forum_topics_tg SET {', '.join(sets)} WHERE chat_id = %s AND topic_id = %s",
            tuple(params)
        )


def update_forum_topic_closed(chat_id: int, topic_id: int, is_closed: bool):
    """Update forum topic closed/reopened status."""
    with db_cursor(commit=True) as cur:
        cur.execute("""
            UPDATE public.forum_topics_tg SET is_closed = %s, updated_at = NOW()
            WHERE chat_id = %s AND topic_id = %s
        """, (is_closed, chat_id, topic_id))


def save_message_to_db(
    chat_id: int,
    message_text: str,
    msg_type: str,
    tg_id: int,
    type_of_message: str = 'text',
    group_sender_id: int = None,
    reply_to: int = None,
    files_path: list = None,
    message_thread_id: int = None,
) -> int:
    """Save message to chat_history_tg, returns chat_history_tg.id"""
    with db_cursor(commit=True) as cur:
        cur.execute("""
            INSERT INTO chat_history_tg
            (chat_id, message, type, tg_id, type_of_message, group_sender_id, reply_to, files_path, status, message_thread_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'unread', %s)
            ON CONFLICT (chat_id, tg_id) DO UPDATE SET
                message = EXCLUDED.message,
                type_of_message = EXCLUDED.type_of_message,
                group_sender_id = EXCLUDED.group_sender_id,
                reply_to = EXCLUDED.reply_to,
                files_path = EXCLUDED.files_path,
                message_thread_id = EXCLUDED.message_thread_id
            RETURNING id
        """, (
            chat_id, message_text, msg_type, tg_id, type_of_message,
            group_sender_id, reply_to, files_path, message_thread_id
        ))

        history_id = cur.fetchone()[0]
        files_count = len(files_path) if files_path else 0
        logger.info(f"Saved message: chat={chat_id}, tg_id={tg_id}, type={type_of_message}, files={files_count}")

    # Publish to Redis for real-time delivery to aeon-main
    try:
        from redis_client import get_redis
        r = get_redis()
        if r:
            r.publish('aeon:new_tg_message', json.dumps({
                'id': history_id,
                'chat_id': chat_id,
                'tg_id': tg_id
            }))
    except Exception as e:
        logger.debug(f"Redis PUBLISH failed (non-critical): {e}")

    return history_id


def resolve_telegram_chat_id(internal_chat_id: int) -> Optional[int]:
    """Resolve internal chat_id to Telegram chat_id.
    Returns telegram_id for users or -group_id for groups.
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


def _update_media_group_file(files_path, history_id):
    """Sync helper for updating media group files in DB"""
    with db_cursor(commit=True) as cur:
        cur.execute("""
            UPDATE chat_history_tg
            SET files_path = %s
            WHERE id = %s
        """, (files_path, history_id))


def _update_message_files_url(history_id, files_url):
    """Sync helper: set files_url (array) for a chat_history_tg row."""
    with db_cursor(commit=True) as cur:
        cur.execute("""
            UPDATE chat_history_tg
            SET files_url = %s
            WHERE id = %s
        """, (files_url, history_id))


def _send_n8n_webhook(webhook_data):
    """Sync helper for sending n8n webhook"""
    import requests
    requests.post(N8N_WEBHOOK_URL, json=webhook_data, timeout=5)
