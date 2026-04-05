"""Shared test fixtures for aeon-tg-bot tests"""
import pytest
from unittest.mock import patch, MagicMock, PropertyMock
import fakeredis


@pytest.fixture
def mock_cursor():
    """Mock db_cursor context manager -- yields a MagicMock cursor.

    Patches db_cursor in all modules that import it:
    - db_helpers.db_cursor (db CRUD functions)
    - health.db_cursor (health check SELECT 1)
    - endpoints.db_cursor (internal update endpoint, media download)
    - bot.db_cursor (backward compat re-export)
    """
    cursor = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cursor)
    ctx.__exit__ = MagicMock(return_value=False)

    with patch('db_helpers.db_cursor', return_value=ctx), \
         patch('health.db_cursor', return_value=ctx), \
         patch('endpoints.db_cursor', return_value=ctx):
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

    Patches shared module globals (bot, redis_client, sender_bot, etc.)
    so /health and other endpoints can run without real infra.
    """
    import shared as shared_module
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

    shared_patches = {
        'bot': mock_bot,
        'redis_client': mock_redis,
        'sender_bot': mock_sender,
        'pg_listener_thread': mock_pg_thread,
        'retry_worker_thread': mock_retry_thread,
        'incoming_consumer_thread': mock_consumer_thread,
        '_listener_degraded': False,
    }

    with patch.multiple(shared_module, **shared_patches), \
         patch('health.db_cursor') as mock_db_ctx, \
         patch('db.get_pool_stats', return_value={
             'used': 1, 'free': 9, 'total': 10,
             'maxconn': 10, 'minconn': 2,
         }):
        # db_cursor for health check SELECT 1
        cur = MagicMock()
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=cur)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        shared_module.flask_app.config['TESTING'] = True
        yield shared_module.flask_app.test_client()
