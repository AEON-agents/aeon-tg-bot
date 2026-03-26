"""Tests for supergroup/forum support: _get_thread_id, save_forum_topic,
update_forum_topic_name, update_forum_topic_closed, _send_text with thread id."""
import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock


# ---------------------------------------------------------------------------
# _get_thread_id() tests
# ---------------------------------------------------------------------------

class TestGetThreadId:
    """Tests for _get_thread_id() in handlers.py"""

    def test_get_thread_id_present(self):
        """message_thread_id=42 in body returns 42."""
        from handlers import _get_thread_id

        result = _get_thread_id({'message_thread_id': 42})

        assert result == 42

    def test_get_thread_id_absent(self):
        """Body without message_thread_id returns None."""
        from handlers import _get_thread_id

        result = _get_thread_id({})

        assert result is None

    def test_get_thread_id_zero(self):
        """message_thread_id=0 is falsy but a valid topic ID — must return 0, not None."""
        from handlers import _get_thread_id

        result = _get_thread_id({'message_thread_id': 0})

        # dict.get() returns 0 here, not None — the function must not coerce to None
        assert result == 0
        assert result is not None, "0 is a valid thread id and must not be lost"


# ---------------------------------------------------------------------------
# save_forum_topic() tests
# ---------------------------------------------------------------------------

class TestSaveForumTopic:
    """Tests for save_forum_topic() in bot.py"""

    def test_save_forum_topic_sql(self, mock_cursor):
        """INSERT INTO forum_topics_tg with ON CONFLICT; params are (chat_id, topic_id, name, icon_color)."""
        from bot import save_forum_topic

        save_forum_topic(chat_id=100, topic_id=42, name="Test Topic", icon_color=123)

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]

        assert "INSERT INTO" in sql
        assert "forum_topics_tg" in sql
        assert "ON CONFLICT" in sql
        assert params == (100, 42, "Test Topic", 123), (
            "params must be (chat_id, topic_id, name, icon_color) in that order"
        )

    def test_save_forum_topic_without_icon_color(self, mock_cursor):
        """icon_color=None is passed through as None, not dropped."""
        from bot import save_forum_topic

        save_forum_topic(chat_id=200, topic_id=7, name="General", icon_color=None)

        mock_cursor.execute.assert_called_once()
        _, params = mock_cursor.execute.call_args[0]

        assert params == (200, 7, "General", None), (
            "None icon_color must still be forwarded as the 4th param"
        )


# ---------------------------------------------------------------------------
# update_forum_topic_name() tests
# ---------------------------------------------------------------------------

class TestUpdateForumTopicName:
    """Tests for update_forum_topic_name() in bot.py"""

    def test_update_name_only(self, mock_cursor):
        """When only name is supplied, SET clause includes 'name' but NOT 'icon_color'."""
        from bot import update_forum_topic_name

        update_forum_topic_name(100, 42, name="New Name")

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]

        assert "name" in sql, "SQL SET clause must reference the name column"
        assert "icon_color" not in sql, (
            "icon_color must be absent from SET when not supplied"
        )
        # params: name, chat_id, topic_id  (icon_color omitted)
        assert "New Name" in params
        assert 100 in params
        assert 42 in params

    def test_update_name_and_icon(self, mock_cursor):
        """When both name and icon_color are supplied, SET clause includes both columns."""
        from bot import update_forum_topic_name

        update_forum_topic_name(100, 42, name="New Name", icon_color=456)

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]

        assert "name" in sql, "SQL SET clause must reference the name column"
        assert "icon_color" in sql, "SQL SET clause must reference icon_color when supplied"
        assert "New Name" in params
        assert 456 in params
        assert 100 in params
        assert 42 in params


# ---------------------------------------------------------------------------
# update_forum_topic_closed() tests
# ---------------------------------------------------------------------------

class TestUpdateForumTopicClosed:
    """Tests for update_forum_topic_closed() in bot.py"""

    def test_close_topic(self, mock_cursor):
        """is_closed=True is the first positional param passed to the query."""
        from bot import update_forum_topic_closed

        update_forum_topic_closed(100, 42, is_closed=True)

        mock_cursor.execute.assert_called_once()
        _, params = mock_cursor.execute.call_args[0]

        assert params[0] is True, "is_closed=True must be the first query param"
        assert 100 in params
        assert 42 in params

    def test_reopen_topic(self, mock_cursor):
        """is_closed=False is the first positional param passed to the query."""
        from bot import update_forum_topic_closed

        update_forum_topic_closed(100, 42, is_closed=False)

        mock_cursor.execute.assert_called_once()
        _, params = mock_cursor.execute.call_args[0]

        assert params[0] is False, "is_closed=False must be the first query param"
        assert 100 in params
        assert 42 in params


# ---------------------------------------------------------------------------
# SenderBot._send_text() with message_thread_id
# ---------------------------------------------------------------------------

class TestSendTextWithThreadId:
    """_send_text() forwards message_thread_id to bot.send_message."""

    def test_send_text_with_thread_id(self):
        """message_thread_id=42 must be forwarded to worker_bot.send_message()."""
        import fakeredis
        from aiogram import Bot as AiogramBot

        mock_bot = MagicMock(spec=AiogramBot)
        fake_redis_client = fakeredis.FakeRedis(decode_responses=True)

        with patch('sender_bot.get_redis', return_value=fake_redis_client), \
             patch('sender_bot.get_db_pool', return_value=None):
            from sender_bot import SenderBot
            sender = SenderBot(bot=mock_bot)

        # worker_bot is set by the async worker loop; inject a mock directly
        mock_worker_bot = AsyncMock()
        mock_msg = MagicMock()
        mock_msg.message_id = 999
        mock_worker_bot.send_message = AsyncMock(return_value=mock_msg)
        sender.worker_bot = mock_worker_bot

        asyncio.run(
            sender._send_text(chat_id=100, text="hello", reply_to=None, message_thread_id=42)
        )

        mock_worker_bot.send_message.assert_called_once()
        call_kwargs = mock_worker_bot.send_message.call_args[1]
        assert call_kwargs.get('message_thread_id') == 42, (
            "_send_text must pass message_thread_id=42 to send_message"
        )
        assert call_kwargs.get('chat_id') == 100
