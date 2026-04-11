"""Tests for bot.py — DB CRUD, health endpoint, media group flush, graceful shutdown"""
import pytest
import asyncio
import signal
import time
from unittest.mock import patch, MagicMock, AsyncMock


# ---------------------------------------------------------------------------
# DB CRUD tests (get_or_create_user, get_or_create_chat, save_message_to_db)
# ---------------------------------------------------------------------------

class TestGetOrCreateUser:
    """Tests for get_or_create_user()"""

    def test_new_user_inserts_and_returns_id(self, mock_cursor):
        """When user doesn't exist, INSERT RETURNING id"""
        # First SELECT returns None (user not found)
        mock_cursor.fetchone.side_effect = [None, (42,)]

        from bot import get_or_create_user
        result = get_or_create_user(
            telegram_id=123456,
            first_name='John',
            last_name='Doe',
            username='johndoe',
            is_bot=False,
        )

        assert result == 42
        # Two execute calls: SELECT + INSERT
        assert mock_cursor.execute.call_count == 2
        # INSERT call should contain telegram_id and name
        insert_call = mock_cursor.execute.call_args_list[1]
        insert_args = insert_call[0][1]
        assert insert_args[0] == 123456  # telegram_id
        assert insert_args[1] == 'John'  # first_name

    def test_existing_user_updates_and_returns_id(self, mock_cursor):
        """When user exists, UPDATE name/username fields"""
        # SELECT returns existing user id
        mock_cursor.fetchone.return_value = (5,)

        from bot import get_or_create_user
        result = get_or_create_user(
            telegram_id=123456,
            first_name='Jane',
            last_name='Smith',
            username='janesmith',
        )

        assert result == 5
        # Two execute calls: SELECT + UPDATE
        assert mock_cursor.execute.call_count == 2
        update_call = mock_cursor.execute.call_args_list[1]
        update_args = update_call[0][1]
        assert update_args[0] == 'Jane'       # first_name
        assert update_args[4] == 123456        # WHERE telegram_id


class TestGetOrCreateChat:
    """Tests for get_or_create_chat()"""

    def test_new_user_chat(self, mock_cursor):
        """type='user': SELECT miss -> INSERT RETURNING id"""
        mock_cursor.fetchone.side_effect = [None, (10,)]

        from bot import get_or_create_chat
        result = get_or_create_chat(chat_type='user', user_id=5)

        assert result == 10
        assert mock_cursor.execute.call_count == 2
        # SELECT should filter by type='user' AND user_id
        select_sql = mock_cursor.execute.call_args_list[0][0][0]
        assert "type = 'user'" in select_sql
        # INSERT should use type='user'
        insert_sql = mock_cursor.execute.call_args_list[1][0][0]
        assert "'user'" in insert_sql

    def test_existing_group_chat(self, mock_cursor):
        """type='group': SELECT hit -> returns existing id"""
        mock_cursor.fetchone.return_value = (20,)

        from bot import get_or_create_chat
        result = get_or_create_chat(chat_type='group', group_id=999)

        assert result == 20
        # Only SELECT, no INSERT
        assert mock_cursor.execute.call_count == 1
        select_sql = mock_cursor.execute.call_args_list[0][0][0]
        assert "type = 'group'" in select_sql


class TestSaveMessageToDb:
    """Tests for save_message_to_db()"""

    def test_happy_path(self, mock_cursor):
        """INSERT with correct params, returns history id"""
        mock_cursor.fetchone.return_value = (100,)

        from bot import save_message_to_db
        result = save_message_to_db(
            chat_id=10,
            message_text='Hello world',
            msg_type='user',
            tg_id=555,
            type_of_message='text',
        )

        assert result == 100
        assert mock_cursor.execute.call_count == 1
        sql = mock_cursor.execute.call_args[0][0]
        assert 'INSERT INTO chat_history_tg' in sql
        assert 'ON CONFLICT' in sql
        params = mock_cursor.execute.call_args[0][1]
        assert params[0] == 10          # chat_id
        assert params[1] == 'Hello world'  # message_text
        assert params[2] == 'user'      # msg_type
        assert params[3] == 555         # tg_id

    def test_upsert_with_files(self, mock_cursor):
        """ON CONFLICT updates fields, files_path passed correctly"""
        mock_cursor.fetchone.return_value = (101,)

        from bot import save_message_to_db
        files = ['AgACfile1', 'AgACfile2']
        result = save_message_to_db(
            chat_id=10,
            message_text='Photo album',
            msg_type='user',
            tg_id=556,
            type_of_message='photo',
            group_sender_id=789,
            reply_to=554,
            files_path=files,
        )

        assert result == 101
        params = mock_cursor.execute.call_args[0][1]
        assert params[5] == 789         # group_sender_id
        assert params[6] == 554         # reply_to
        assert params[7] == files       # files_path


# ---------------------------------------------------------------------------
# Health endpoint test
# ---------------------------------------------------------------------------

class TestHealthEndpoint:

    def test_health_returns_200_ok(self, flask_client):
        """GET /health returns 200 with expected JSON keys"""
        resp = flask_client.get('/health')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] in ('ok', 'degraded')
        expected_keys = [
            'bot_initialized', 'sender_running', 'redis_connected',
            'db_connected', 'db_pool', 'pg_listener_alive',
            'queue_length', 'ffmpeg_available',
        ]
        for key in expected_keys:
            assert key in data, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# Media group flush test
# ---------------------------------------------------------------------------

class TestMediaGroupFlush:

    def test_flush_collects_and_sorts_by_message_id(self):
        """flush_media_group sorts by message_id and calls save_message_to_db"""
        import bot as bot_module

        # Seed the buffer with unsorted messages
        bot_module.media_group_buffer['grp_123'] = {
            'messages': [
                {
                    'message_id': 3,
                    'file_id': 'file_c',
                    'media_type': 'photo',
                    'chat_id': 10,
                    'caption': '',
                    'sender_telegram_id': 111,
                    'reply_to': None,
                },
                {
                    'message_id': 1,
                    'file_id': 'file_a',
                    'media_type': 'photo',
                    'chat_id': 10,
                    'caption': 'Album caption',
                    'sender_telegram_id': 111,
                    'reply_to': None,
                },
                {
                    'message_id': 2,
                    'file_id': 'file_b',
                    'media_type': 'photo',
                    'chat_id': 10,
                    'caption': '',
                    'sender_telegram_id': 111,
                    'reply_to': None,
                },
            ]
        }

        import app as app_module
        with patch.object(app_module, 'save_message_to_db', return_value=42) as mock_save:
            # Run the coroutine but patch out the sleep
            with patch('asyncio.sleep', new_callable=AsyncMock):
                asyncio.run(bot_module.flush_media_group('grp_123'))

            mock_save.assert_called_once()
            call_kwargs = mock_save.call_args[1]
            # files should be sorted by message_id: file_a, file_b, file_c
            assert call_kwargs['files_path'] == ['file_a', 'file_b', 'file_c']
            assert call_kwargs['message_text'] == 'Album caption'
            assert call_kwargs['tg_id'] == 1  # first message_id after sort
            assert call_kwargs['type_of_message'] == 'photo'  # all same type

        # Buffer should be empty after flush
        assert 'grp_123' not in bot_module.media_group_buffer
        # Flushed should be recorded
        assert 'grp_123' in bot_module.media_group_flushed
        # Cleanup
        bot_module.media_group_flushed.pop('grp_123', None)


# ---------------------------------------------------------------------------
# Graceful shutdown tests
# ---------------------------------------------------------------------------

class TestGracefulShutdown:
    """Tests for _graceful_exit and watchdog improvements"""

    def _reset(self, shared_module):
        """Reset shared state and one-shot latch between tests."""
        import watchdog as watchdog_module
        watchdog_module._reset_graceful_exit_state()
        shared_module._shutdown_event.clear()

    def test_graceful_exit_sets_shutdown_event(self):
        """_graceful_exit sets _shutdown_event before exiting"""
        import shared as shared_module
        import bot as bot_module
        self._reset(shared_module)

        with patch.object(shared_module, 'sender_bot', None), \
             patch('db.close_db_pool'), \
             patch('redis_client.close_redis_pool'), \
             patch('watchdog.threading.Thread'), \
             patch('watchdog.os.kill') as mock_kill:
            bot_module._graceful_exit(exit_code=42)

        # SIGTERM sent to self instead of sys.exit — daemon threads can't exit the process
        assert mock_kill.called
        assert shared_module._shutdown_event.is_set()
        self._reset(shared_module)

    def test_graceful_exit_stops_sender(self):
        """_graceful_exit calls sender_bot.stop()"""
        import shared as shared_module
        import bot as bot_module
        self._reset(shared_module)
        mock_sender = MagicMock()

        with patch.object(shared_module, 'sender_bot', mock_sender), \
             patch('db.close_db_pool'), \
             patch('redis_client.close_redis_pool'), \
             patch('watchdog.threading.Thread'), \
             patch('watchdog.os.kill'):
            bot_module._graceful_exit(1)

        mock_sender.stop.assert_called_once()
        self._reset(shared_module)

    def test_graceful_exit_closes_db_and_redis(self):
        """_graceful_exit closes DB pool and Redis pool"""
        import shared as shared_module
        import bot as bot_module
        self._reset(shared_module)

        with patch.object(shared_module, 'sender_bot', None), \
             patch('db.close_db_pool') as mock_db, \
             patch('redis_client.close_redis_pool') as mock_redis, \
             patch('watchdog.threading.Thread'), \
             patch('watchdog.os.kill'):
            bot_module._graceful_exit(1)

        mock_db.assert_called_once()
        mock_redis.assert_called_once()
        self._reset(shared_module)

    def test_graceful_exit_joins_threads(self):
        """_graceful_exit attempts to join background threads"""
        import shared as shared_module
        import bot as bot_module
        self._reset(shared_module)
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True

        with patch.object(shared_module, 'sender_bot', None), \
             patch.object(shared_module, 'pg_listener_thread', mock_thread), \
             patch.object(shared_module, 'retry_worker_thread', None), \
             patch.object(shared_module, 'incoming_consumer_thread', None), \
             patch('db.close_db_pool'), \
             patch('redis_client.close_redis_pool'), \
             patch('watchdog.threading.Thread'), \
             patch('watchdog.os.kill'):
            bot_module._graceful_exit(1, timeout=9)

        mock_thread.join.assert_called_once_with(timeout=3.0)  # min(9/3, 5)
        self._reset(shared_module)

    def test_graceful_exit_survives_sender_error(self):
        """_graceful_exit continues even if sender_bot.stop() raises"""
        import shared as shared_module
        import bot as bot_module
        self._reset(shared_module)
        mock_sender = MagicMock()
        mock_sender.stop.side_effect = RuntimeError("boom")

        with patch.object(shared_module, 'sender_bot', mock_sender), \
             patch('db.close_db_pool'), \
             patch('redis_client.close_redis_pool'), \
             patch('watchdog.threading.Thread'), \
             patch('watchdog.os.kill') as mock_kill:
            bot_module._graceful_exit(1)

        # Should still terminate despite error
        assert mock_kill.called
        self._reset(shared_module)

    def test_graceful_exit_uses_os_exit_when_sigterm_fails(self):
        """If os.kill raises, fall back to os._exit (bypasses daemon-thread sys.exit trap)"""
        import shared as shared_module
        import bot as bot_module
        self._reset(shared_module)

        with patch.object(shared_module, 'sender_bot', None), \
             patch('db.close_db_pool'), \
             patch('redis_client.close_redis_pool'), \
             patch('watchdog.threading.Thread'), \
             patch('watchdog.os.kill', side_effect=OSError("no such process")), \
             patch('watchdog.os._exit') as mock_exit:
            bot_module._graceful_exit(1)

        mock_exit.assert_called_once_with(1)
        self._reset(shared_module)

    def test_graceful_exit_is_one_shot(self):
        """Second call to _graceful_exit while first is in progress is a no-op"""
        import shared as shared_module
        import bot as bot_module
        self._reset(shared_module)
        mock_sender = MagicMock()

        with patch.object(shared_module, 'sender_bot', mock_sender), \
             patch('db.close_db_pool'), \
             patch('redis_client.close_redis_pool'), \
             patch('watchdog.threading.Thread'), \
             patch('watchdog.os.kill'):
            bot_module._graceful_exit(1)
            bot_module._graceful_exit(1)  # should be ignored

        # Sender should have been stopped exactly once
        mock_sender.stop.assert_called_once()
        self._reset(shared_module)

    def test_check_cooldown_allows_within_limit(self):
        """_check_cooldown returns True when under limit"""
        import shared as shared_module
        import bot as bot_module
        # Reset cooldown tracker
        shared_module._thread_restart_times = {
            'pg_listener': [], 'retry_worker': [],
            'incoming_consumer': [], 'sender': [],
        }
        assert bot_module._check_cooldown('pg_listener') is True
        assert bot_module._check_cooldown('pg_listener') is True

    def test_check_cooldown_blocks_after_limit(self):
        """_check_cooldown returns False after MAX_RESTARTS_IN_WINDOW"""
        import shared as shared_module
        import bot as bot_module
        shared_module._thread_restart_times = {
            'pg_listener': [time.time(), time.time(), time.time()],
            'retry_worker': [], 'incoming_consumer': [], 'sender': [],
        }
        assert bot_module._check_cooldown('pg_listener') is False

    def test_check_cooldown_expires_old_entries(self):
        """_check_cooldown removes entries older than RESTART_COOLDOWN_WINDOW"""
        import shared as shared_module
        import bot as bot_module
        old_time = time.time() - bot_module.RESTART_COOLDOWN_WINDOW - 10
        shared_module._thread_restart_times = {
            'pg_listener': [old_time, old_time, old_time],
            'retry_worker': [], 'incoming_consumer': [], 'sender': [],
        }
        # Old entries should be pruned, so this is allowed
        assert bot_module._check_cooldown('pg_listener') is True

    def test_handle_sigterm_calls_graceful_exit(self):
        """SIGTERM handler calls _graceful_exit(0)"""
        import shared as shared_module
        import app as app_module
        import bot as bot_module

        with patch.object(app_module, '_graceful_exit') as mock_exit:
            bot_module._handle_sigterm(signal.SIGTERM, None)

        mock_exit.assert_called_once_with(0, timeout=15)
        shared_module._shutdown_event.clear()
