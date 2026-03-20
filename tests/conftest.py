"""Shared test fixtures for aeon-tg-bot tests"""
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
import fakeredis


@pytest.fixture
def mock_cursor():
    """Mock db_cursor context manager — yields a MagicMock cursor.

    Patches bot.db_cursor (since bot.py does `from db import db_cursor`).
    """
    cursor = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cursor)
    ctx.__exit__ = MagicMock(return_value=False)

    with patch('bot.db_cursor', return_value=ctx):
        yield cursor


@pytest.fixture
def fake_redis():
    """FakeRedis instance with decode_responses=True.

    Patches redis_client.get_redis so any code calling get_redis()
    receives this fake client.
    """
    client = fakeredis.FakeRedis(decode_responses=True)
    with patch('redis_client.get_redis', return_value=client), \
         patch('redis_client._redis_client', client):
        yield client


@pytest.fixture
def flask_client():
    """Flask test client with all heavy globals mocked out.

    Patches: bot, redis_client, sender_bot, pg_listener_thread,
    db_cursor, get_pool_stats — so /health and other endpoints
    can run without real infra.
    """
    # Import lazily to avoid module-level side effects
    import bot as bot_module

    mock_bot = MagicMock()
    mock_redis = fakeredis.FakeRedis(decode_responses=True)
    mock_sender = MagicMock()
    mock_sender.is_running = True
    mock_pg_thread = MagicMock()
    mock_pg_thread.is_alive.return_value = True
    mock_retry_thread = MagicMock()
    mock_retry_thread.is_alive.return_value = True
    mock_consumer_thread = MagicMock()
    mock_consumer_thread.is_alive.return_value = True

    patches = {
        'bot': mock_bot,
        'redis_client': mock_redis,
        'sender_bot': mock_sender,
        'pg_listener_thread': mock_pg_thread,
        'retry_worker_thread': mock_retry_thread,
        'incoming_consumer_thread': mock_consumer_thread,
        '_listener_degraded': False,
    }

    with patch.multiple(bot_module, **patches), \
         patch('bot.db_cursor') as mock_db_ctx, \
         patch('db.get_pool_stats', return_value={
             'used': 1, 'free': 9, 'total': 10,
             'maxconn': 10, 'minconn': 2,
         }):
        # db_cursor for health check SELECT 1
        cur = MagicMock()
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=cur)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        bot_module.flask_app.config['TESTING'] = True
        yield bot_module.flask_app.test_client()
