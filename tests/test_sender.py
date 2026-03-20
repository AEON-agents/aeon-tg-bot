"""Tests for sender_bot.py — rate limiter, dedup, db reconnect"""
import pytest
import time
import random
from unittest.mock import patch, MagicMock
import fakeredis

from sender_bot import RedisRateLimiter, RateLimitConfig


# ---------------------------------------------------------------------------
# Rate limiter tests
# ---------------------------------------------------------------------------

class TestRateLimiterPerChat:
    """Per-chat sliding window: 1 msg/sec"""

    def test_second_message_within_1s_is_throttled(self):
        """Two messages to same chat in <1s -> second one waits"""
        client = fakeredis.FakeRedis(decode_responses=True)
        limiter = RedisRateLimiter(client)

        now = time.time()
        chat_id = 12345

        # Record first message
        limiter._record_message(chat_id, now)

        # Check 100ms later — should need to wait
        wait = limiter._sliding_window_check(
            f"{limiter.CHAT_KEY}{chat_id}",
            now + 0.1,
            window=1.0 / limiter.config.chat_messages_per_second,
            limit=1,
        )
        assert wait > 0, "Expected throttle for second message within window"

    def test_message_after_window_passes(self):
        """Message after 1s window passes -> no wait"""
        client = fakeredis.FakeRedis(decode_responses=True)
        limiter = RedisRateLimiter(client)

        now = time.time()
        chat_id = 12345

        limiter._record_message(chat_id, now)

        # Check 1.5s later — should pass
        wait = limiter._sliding_window_check(
            f"{limiter.CHAT_KEY}{chat_id}",
            now + 1.5,
            window=1.0 / limiter.config.chat_messages_per_second,
            limit=1,
        )
        assert wait == 0.0, "Expected no throttle after window expires"


class TestRateLimiterGlobal:
    """Global message limit: 25 * 0.85 = 21 msg/sec"""

    def test_exceeding_global_limit_throttles(self):
        """More messages than global limit -> throttle"""
        client = fakeredis.FakeRedis(decode_responses=True)
        config = RateLimitConfig(
            global_messages_per_second=10,
            safety_factor=1.0,  # exact limit = 10
        )
        limiter = RedisRateLimiter(client, config)

        now = time.time()
        # Record 10 global messages
        for i in range(10):
            ts = now + i * 0.01  # spread within 1s window
            member = f"{ts}:{random.random()}"
            client.zadd(limiter.GLOBAL_MSG_KEY, {member: ts})

        # 11th message should be throttled
        wait = limiter._sliding_window_check(
            limiter.GLOBAL_MSG_KEY,
            now + 0.5,
            window=1.0,
            limit=10,
        )
        assert wait > 0, "Expected throttle when global limit exceeded"


# ---------------------------------------------------------------------------
# Deduplication tests
# ---------------------------------------------------------------------------

class TestDedupById:
    """ID-based dedup: telegram:sent:{chat_history_id}"""

    def test_first_set_returns_true(self):
        """First SET NX -> True (message not seen before)"""
        client = fakeredis.FakeRedis(decode_responses=True)
        key = "telegram:sent:42"
        result = client.set(key, "1", nx=True, ex=600)
        assert result is True

    def test_second_set_returns_false(self):
        """Second SET NX -> None/False (duplicate)"""
        client = fakeredis.FakeRedis(decode_responses=True)
        key = "telegram:sent:42"
        client.set(key, "1", nx=True, ex=600)
        result = client.set(key, "1", nx=True, ex=600)
        assert result is None  # fakeredis returns None for failed NX


class TestDedupByContent:
    """Content-based dedup using hash+session+5min slot"""

    def test_same_content_same_slot_is_duplicate(self):
        """Same md5 + session_id + 5-min slot -> duplicate detected"""
        import hashlib
        client = fakeredis.FakeRedis(decode_responses=True)

        chat_id = 12345
        msg_type = "text"
        session_id = "sess_abc"
        message = "Hello world"
        md5 = hashlib.md5(message.encode()).hexdigest()[:8]
        slot = int(time.time()) // 300  # 5-min slot

        key = f"tg:dedup:{chat_id}:AEON:{msg_type}:{session_id}:{md5}:{slot}"

        first = client.set(key, "1", nx=True, ex=600)
        assert first is True

        second = client.set(key, "1", nx=True, ex=600)
        assert second is None  # duplicate

    def test_different_content_not_duplicate(self):
        """Different message hash -> not duplicate"""
        import hashlib
        client = fakeredis.FakeRedis(decode_responses=True)

        chat_id = 12345
        slot = int(time.time()) // 300

        key1 = f"tg:dedup:{chat_id}:AEON:text:sess:{hashlib.md5(b'msg1').hexdigest()[:8]}:{slot}"
        key2 = f"tg:dedup:{chat_id}:AEON:text:sess:{hashlib.md5(b'msg2').hexdigest()[:8]}:{slot}"

        assert client.set(key1, "1", nx=True, ex=600) is True
        assert client.set(key2, "1", nx=True, ex=600) is True  # different key -> OK


# ---------------------------------------------------------------------------
# DB connection reconnect tests
# ---------------------------------------------------------------------------

class TestDbConnectionReconnect:
    """db_connection() retries on OperationalError"""

    def test_reconnect_on_connection_reset(self):
        """OperationalError 'connection reset' -> retry, second attempt succeeds"""
        import psycopg2
        from db import db_connection

        mock_pool = MagicMock()
        good_conn = MagicMock()

        # First getconn raises, second succeeds
        mock_pool.getconn.side_effect = [
            psycopg2.OperationalError("SSL connection has been closed unexpectedly"),
            good_conn,
        ]

        with patch('db.get_db_pool', return_value=mock_pool), \
             patch('db._reset_pool') as mock_reset, \
             patch('db._track_conn'), \
             patch('db._is_conn_stale', return_value=False), \
             patch('db._untrack_conn'), \
             patch('time.sleep'):
            with db_connection(max_retries=2) as conn:
                assert conn is good_conn

        # Pool should have been reset after first failure
        mock_reset.assert_called_once()

    def test_non_connection_error_raises_immediately(self):
        """Non-connection OperationalError raises without retry"""
        import psycopg2
        from db import db_connection

        mock_pool = MagicMock()
        mock_pool.getconn.return_value = MagicMock()

        # Simulate error during yield (query execution)
        with patch('db.get_db_pool', return_value=mock_pool), \
             patch('db._track_conn'), \
             patch('db._is_conn_stale', return_value=False):
            with pytest.raises(psycopg2.OperationalError, match="some other error"):
                with db_connection(max_retries=3) as conn:
                    raise psycopg2.OperationalError("some other error")


class TestDbConnectionAgeRecycling:
    """Stale connections (>300s) are recycled"""

    def test_stale_conn_is_recycled(self):
        """Connection older than MAX_CONN_AGE -> putconn(close=True), new getconn"""
        from db import db_connection

        mock_pool = MagicMock()
        stale_conn = MagicMock()
        fresh_conn = MagicMock()
        mock_pool.getconn.side_effect = [stale_conn, fresh_conn]

        with patch('db.get_db_pool', return_value=mock_pool), \
             patch('db._track_conn'), \
             patch('db._untrack_conn') as mock_untrack, \
             patch('db._is_conn_stale', side_effect=[True, False]):
            with db_connection() as conn:
                assert conn is fresh_conn

        # Stale conn should have been returned with close=True
        mock_pool.putconn.assert_any_call(stale_conn, close=True)
        mock_untrack.assert_called_with(stale_conn)
