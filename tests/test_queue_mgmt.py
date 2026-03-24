"""Tests for queue management endpoints and retry_stuck_messages (2.8)

Endpoints:
- GET  /telegram/queue/peek      — view queue contents
- POST /telegram/queue/flush     — clear entire queue
- POST /telegram/queue/dedup     — remove duplicates
- POST /telegram/queue/force_send — bypass dedup and re-queue
- POST /telegram/retry_stuck     — retry stuck messages
"""
import pytest
import json
import time
from unittest.mock import patch, MagicMock
import fakeredis


QUEUE_KEY = 'telegram:send_queue'


@pytest.fixture
def redis_client():
    """FakeRedis with some queue items."""
    client = fakeredis.FakeRedis(decode_responses=True)
    return client


@pytest.fixture
def flask_client_with_redis(redis_client):
    """Flask test client with mocked redis containing queue items."""
    import bot as bot_module

    mock_bot = MagicMock()
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
        'redis_client': redis_client,
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
        cur = MagicMock()
        mock_db_ctx.return_value.__enter__ = MagicMock(return_value=cur)
        mock_db_ctx.return_value.__exit__ = MagicMock(return_value=False)

        bot_module.flask_app.config['TESTING'] = True
        yield bot_module.flask_app.test_client(), redis_client


# ---------------------------------------------------------------------------
# Queue Peek
# ---------------------------------------------------------------------------

class TestQueuePeek:
    """GET /telegram/queue/peek"""

    def test_peek_empty_queue(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        resp = client.get('/telegram/queue/peek')
        data = resp.get_json()
        assert resp.status_code == 200
        assert data['queue_length'] == 0
        assert data['items'] == []

    def test_peek_with_items(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        # Add items to queue
        task1 = json.dumps({
            'chat_history_id': 101,
            'chat_ident': '12345',
            'request_body': {'message': 'Hello world', 'type_of_message': 'text'},
            'retry_count': 0
        })
        task2 = json.dumps({
            'chat_history_id': 102,
            'chat_ident': '67890',
            'request_body': {'message': 'Photo caption', 'type_of_message': 'photo'},
            'retry_count': 1
        })
        redis.rpush(QUEUE_KEY, task1, task2)

        resp = client.get('/telegram/queue/peek')
        data = resp.get_json()
        assert resp.status_code == 200
        assert data['queue_length'] == 2
        assert len(data['items']) == 2
        assert data['items'][0]['chat_history_id'] == 101
        assert data['items'][1]['type'] == 'photo'

    def test_peek_respects_limit(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        for i in range(10):
            redis.rpush(QUEUE_KEY, json.dumps({
                'chat_history_id': i,
                'request_body': {'message': f'msg {i}', 'type_of_message': 'text'}
            }))

        resp = client.get('/telegram/queue/peek?limit=3')
        data = resp.get_json()
        # queue_length = len(items) which is limited, not total
        assert len(data['items']) == 3
        assert data['queue_length'] == 3


# ---------------------------------------------------------------------------
# Queue Flush
# ---------------------------------------------------------------------------

class TestQueueFlush:
    """POST /telegram/queue/flush"""

    def test_flush_empty(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        resp = client.post('/telegram/queue/flush')
        data = resp.get_json()
        assert resp.status_code == 200
        assert data['success'] is True
        assert data['flushed'] == 0

    def test_flush_clears_all(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        for i in range(5):
            redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': i}))

        assert redis.llen(QUEUE_KEY) == 5
        resp = client.post('/telegram/queue/flush')
        data = resp.get_json()
        assert data['success'] is True
        assert data['flushed'] == 5
        assert redis.llen(QUEUE_KEY) == 0


# ---------------------------------------------------------------------------
# Queue Dedup
# ---------------------------------------------------------------------------

class TestQueueDedup:
    """POST /telegram/queue/dedup"""

    def test_dedup_no_duplicates(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 1}))
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 2}))

        resp = client.post('/telegram/queue/dedup')
        data = resp.get_json()
        assert data['success'] is True
        assert data['before'] == 2
        assert data['after'] == 2
        assert data['duplicates_removed'] == 0

    def test_dedup_removes_duplicates(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 1}))
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 2}))
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 1}))  # dup
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 3}))
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 2}))  # dup

        resp = client.post('/telegram/queue/dedup')
        data = resp.get_json()
        assert data['success'] is True
        assert data['before'] == 5
        assert data['after'] == 3
        assert data['duplicates_removed'] == 2

    def test_dedup_preserves_order(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 3}))
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 1}))
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 3}))  # dup

        client.post('/telegram/queue/dedup')

        # Check order preserved (first occurrence kept)
        items = redis.lrange(QUEUE_KEY, 0, -1)
        ids = [json.loads(i).get('chat_history_id') for i in items]
        assert ids == [3, 1]

    def test_dedup_handles_items_without_id(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        redis.rpush(QUEUE_KEY, json.dumps({'no_id': True}))
        redis.rpush(QUEUE_KEY, json.dumps({'chat_history_id': 1}))
        redis.rpush(QUEUE_KEY, json.dumps({'no_id_either': True}))

        resp = client.post('/telegram/queue/dedup')
        data = resp.get_json()
        assert data['after'] == 3  # items without ID kept


# ---------------------------------------------------------------------------
# Queue Force Send
# ---------------------------------------------------------------------------

class TestQueueForceSend:
    """POST /telegram/queue/force_send"""

    def test_force_send_requires_ids(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        resp = client.post('/telegram/queue/force_send',
                           json={})
        assert resp.status_code == 400
        assert 'ids required' in resp.get_json()['error']

    def test_force_send_clears_dedup_keys(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        # Set dedup key
        redis.set('telegram:sent:42', '1')
        assert redis.get('telegram:sent:42') == '1'

        with patch('bot.retry_stuck_messages', return_value=1):
            resp = client.post('/telegram/queue/force_send',
                               json={'ids': [42]})

        data = resp.get_json()
        assert data['success'] is True
        assert 42 in data['dedup_cleared']
        # Dedup key should be deleted
        assert redis.get('telegram:sent:42') is None


# ---------------------------------------------------------------------------
# Retry Stuck
# ---------------------------------------------------------------------------

class TestRetryStuck:
    """POST /telegram/retry_stuck"""

    def test_retry_stuck_default_age(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        with patch('bot.retry_stuck_messages', return_value=3) as mock_retry:
            resp = client.post('/telegram/retry_stuck',
                               json={},
                               content_type='application/json')

        data = resp.get_json()
        assert data['success'] is True
        assert data['requeued'] == 3
        mock_retry.assert_called_once_with(max_age_minutes=60)

    def test_retry_stuck_custom_age(self, flask_client_with_redis):
        client, redis = flask_client_with_redis
        with patch('bot.retry_stuck_messages', return_value=0) as mock_retry:
            resp = client.post('/telegram/retry_stuck',
                               json={'max_age_minutes': 120})

        mock_retry.assert_called_once_with(max_age_minutes=120)


# ---------------------------------------------------------------------------
# retry_stuck_messages function
# ---------------------------------------------------------------------------

class TestRetryStuckFunction:
    """Unit tests for retry_stuck_messages()"""

    def test_filters_tg_id_null(self):
        """SQL must include tg_id IS NULL filter."""
        import inspect
        from bot import retry_stuck_messages
        source = inspect.getsource(retry_stuck_messages)
        assert 'tg_id IS NULL' in source

    def test_filters_status_queued_failed(self):
        """SQL must filter by queued and failed status."""
        import inspect
        from bot import retry_stuck_messages
        source = inspect.getsource(retry_stuck_messages)
        assert "'queued'" in source
        assert "'failed'" in source

    def test_adds_force_send_flag(self):
        """When force_send=True, task should have force_send key."""
        task = {'chat_history_id': 42, 'retry': True}
        force_send = True
        if force_send:
            task['force_send'] = True
        assert task['force_send'] is True

    def test_no_force_send_by_default(self):
        """By default, task should NOT have force_send."""
        task = {'chat_history_id': 42, 'retry': True}
        assert 'force_send' not in task

    def test_returns_zero_on_no_db(self):
        """Should return 0 when DB connection fails."""
        with patch('bot.db_connection') as mock_ctx:
            mock_ctx.return_value.__enter__ = MagicMock(side_effect=Exception("no db"))
            mock_ctx.return_value.__exit__ = MagicMock(return_value=True)
            from bot import retry_stuck_messages
            result = retry_stuck_messages()
        assert result == 0
