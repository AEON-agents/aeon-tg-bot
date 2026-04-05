"""Tests for rate limiting and queue routing"""
import pytest
import json
import time
from unittest.mock import patch, MagicMock


class TestRateLimiter:
    def test_allows_within_limit(self):
        import bot as bot_module
        bot_module._rate_limit_counters.clear()
        for i in range(30):
            assert bot_module._check_rate_limit("test_key", 30) is True

    def test_blocks_over_limit(self):
        import bot as bot_module
        bot_module._rate_limit_counters.clear()
        for i in range(30):
            bot_module._check_rate_limit("test_key", 30)
        assert bot_module._check_rate_limit("test_key", 30) is False

    def test_different_keys_independent(self):
        import bot as bot_module
        bot_module._rate_limit_counters.clear()
        for i in range(30):
            bot_module._check_rate_limit("key_a", 30)
        assert bot_module._check_rate_limit("key_b", 30) is True

    def test_expires_old_entries(self):
        import bot as bot_module
        bot_module._rate_limit_counters.clear()
        # Insert timestamps 61 seconds in the past
        old_time = time.time() - 61
        bot_module._rate_limit_counters["expire_key"] = [old_time] * 30
        # Should be allowed because old entries are pruned
        assert bot_module._check_rate_limit("expire_key", 30) is True


class TestQueueTask:
    def test_queue_task_pushes_to_redis(self, fake_redis):
        import bot as bot_module
        bot_module._queue_task(123, 'sticker', {'sticker_emoji': 'x'})
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        assert len(items) == 1
        task = json.loads(items[0])
        assert task['chat_ident'] == '123'
        assert task['request_body']['type_of_message'] == 'sticker'
        assert task['request_body']['chat_id'] == '123'
        assert task['request_body']['sticker_emoji'] == 'x'
        assert 'queued_at' in task

    def test_queue_task_raises_without_redis(self):
        import bot as bot_module
        with patch('redis_client.get_redis', return_value=None):
            with pytest.raises(RuntimeError, match="Redis not available"):
                bot_module._queue_task(123, 'text', {'message': 'hi'})


class TestResolveTgId:
    def test_resolve_from_telegram_id(self):
        import bot as bot_module
        assert bot_module._resolve_tg_id({'telegram_id': 999}) == 999

    def test_resolve_from_chat_id(self):
        import bot as bot_module
        with patch('db_helpers.resolve_telegram_chat_id', return_value=888):
            assert bot_module._resolve_tg_id({'chat_id': 5}) == 888

    def test_resolve_returns_none(self):
        import bot as bot_module
        assert bot_module._resolve_tg_id({}) is None


class TestQueueRouting:
    def test_sticker_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/sticker', json={
            'telegram_id': 123, 'sticker_set': 'AnimatedEmojies', 'emoji': 'x'
        })
        assert resp.status_code == 200
        data = resp.get_json()
        assert data.get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        assert len(items) == 1
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'sticker'

    def test_video_note_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/video_note', json={
            'telegram_id': 123, 'video_url': 'https://example.com/vid.mp4'
        })
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'video_note'

    def test_document_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/document', json={
            'telegram_id': 123, 'document_url': 'https://example.com/doc.pdf'
        })
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'document'

    def test_reaction_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/reaction', json={
            'telegram_id': 123, 'message_id': 456, 'emoji': '\U0001f44d'
        })
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'reaction'

    def test_message_reaction_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/message/123/456/reaction', json={
            'emoji': '\U0001f44d'
        })
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'reaction'
        assert task['request_body']['reply_to_message_id'] == 456

    def test_pin_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/chat/123/pin', json={'message_id': 456})
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'pin'

    def test_pin_requires_message_id(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/chat/123/pin', json={})
        assert resp.status_code == 400

    def test_unpin_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/chat/123/unpin', json={'message_id': 456})
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'unpin'

    def test_unpin_all_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/chat/123/unpin_all')
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'unpin_all'

    def test_forward_queued(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/forward', json={
            'chat_id': 123, 'from_chat_id': 456, 'message_id': 789
        })
        assert resp.status_code == 200
        assert resp.get_json().get('queued') is True
        items = fake_redis.lrange('telegram:send_queue', 0, -1)
        task = json.loads(items[0])
        assert task['request_body']['type_of_message'] == 'forward'

    def test_forward_requires_all_fields(self, flask_client, fake_redis):
        resp = flask_client.post('/telegram/forward', json={
            'chat_id': 123, 'message_id': 789
        })
        assert resp.status_code == 400


class TestRateLimitedEndpoints:
    def test_send_rate_limited(self, flask_client):
        """After 30 sends to same chat, should get 429"""
        import shared as shared_module
        shared_module._rate_limit_counters.clear()

        with patch('endpoints.run_async', return_value=1):
            for i in range(30):
                resp = flask_client.post('/telegram/send', json={
                    'telegram_id': 999, 'message': f'msg {i}'
                })
                assert resp.status_code == 200

            # 31st should be rate limited
            resp = flask_client.post('/telegram/send', json={
                'telegram_id': 999, 'message': 'one too many'
            })
            assert resp.status_code == 429
            data = resp.get_json()
            assert 'retry_after' in data

    def test_typing_rate_limited(self, flask_client):
        """After 10 typing actions to same chat, should get 429"""
        import shared as shared_module
        shared_module._rate_limit_counters.clear()

        with patch('endpoints.run_async', return_value=None):
            for i in range(10):
                resp = flask_client.post('/telegram/typing', json={
                    'telegram_id': 888, 'action': 'typing'
                })
                assert resp.status_code == 200

            resp = flask_client.post('/telegram/typing', json={
                'telegram_id': 888, 'action': 'typing'
            })
            assert resp.status_code == 429

    def test_reaction_delete_rate_limited(self, flask_client):
        """After 20 reaction removals, should get 429"""
        import shared as shared_module
        shared_module._rate_limit_counters.clear()

        with patch('endpoints.run_async', return_value=None):
            for i in range(20):
                resp = flask_client.delete('/telegram/message/777/100/reaction')
                assert resp.status_code == 200

            resp = flask_client.delete('/telegram/message/777/101/reaction')
            assert resp.status_code == 429

    def test_different_chats_independent_limits(self, flask_client):
        """Rate limits are per-chat, not global"""
        import shared as shared_module
        shared_module._rate_limit_counters.clear()

        with patch('endpoints.run_async', return_value=1):
            # Fill up chat 111
            for i in range(30):
                flask_client.post('/telegram/send', json={
                    'telegram_id': 111, 'message': f'msg {i}'
                })

            # Chat 222 should still work
            resp = flask_client.post('/telegram/send', json={
                'telegram_id': 222, 'message': 'still works'
            })
            assert resp.status_code == 200


class TestNewHandlers:
    """Test that new handler types are registered"""

    def test_pin_handler_registered(self):
        from handlers import MessageHandlers
        assert MessageHandlers.get('pin') is not None

    def test_unpin_handler_registered(self):
        from handlers import MessageHandlers
        assert MessageHandlers.get('unpin') is not None

    def test_unpin_all_handler_registered(self):
        from handlers import MessageHandlers
        assert MessageHandlers.get('unpin_all') is not None

    def test_forward_handler_registered(self):
        from handlers import MessageHandlers
        assert MessageHandlers.get('forward') is not None
