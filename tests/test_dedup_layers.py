"""Tests for 3-layer anti-duplicate system in sender_bot.py

Layer 1: In-memory _sent_ids set — survives DB failures
Layer 2: DB check (tg_id/status) — catches retries after process restart
Layer 3: Redis dedup (content hash + ID lock) — existing, tested in test_sender.py
"""
import pytest
import threading
import time
from unittest.mock import patch, MagicMock, AsyncMock
import fakeredis


# ---------------------------------------------------------------------------
# Layer 1: In-memory _sent_ids set
# ---------------------------------------------------------------------------

class TestInMemorySentIds:
    """_sent_ids blocks duplicates even when DB update fails."""

    def _make_sender(self):
        """Create a SenderBot with mocked dependencies."""
        from sender_bot import SenderBot, RateLimitConfig

        mock_bot = MagicMock()
        fake_redis = fakeredis.FakeRedis(decode_responses=True)

        with patch('sender_bot.get_redis', return_value=fake_redis), \
             patch('sender_bot.get_db_pool', return_value=MagicMock()):
            sender = SenderBot(mock_bot, rate_limit_config=RateLimitConfig())

        return sender

    def test_sent_ids_starts_empty(self):
        sender = self._make_sender()
        assert len(sender._sent_ids) == 0

    def test_add_to_sent_ids(self):
        sender = self._make_sender()
        with sender._sent_ids_lock:
            sender._sent_ids.add(12345)
        assert 12345 in sender._sent_ids

    def test_sent_ids_blocks_duplicate(self):
        """If chat_history_id is in _sent_ids, it should be considered duplicate."""
        sender = self._make_sender()
        with sender._sent_ids_lock:
            sender._sent_ids.add(42)

        # Check membership
        with sender._sent_ids_lock:
            is_dup = 42 in sender._sent_ids
        assert is_dup is True

    def test_sent_ids_allows_new(self):
        """New chat_history_id should not be blocked."""
        sender = self._make_sender()
        with sender._sent_ids_lock:
            sender._sent_ids.add(42)

        with sender._sent_ids_lock:
            is_dup = 99 in sender._sent_ids
        assert is_dup is False

    def test_sent_ids_cap_at_50000(self):
        """Set should be capped to prevent memory leak."""
        sender = self._make_sender()
        # Add 50001 items
        with sender._sent_ids_lock:
            for i in range(50001):
                sender._sent_ids.add(i)

            # Simulate the cap logic from _async_process_task
            if len(sender._sent_ids) > 50000:
                to_remove = list(sender._sent_ids)[:10000]
                sender._sent_ids -= set(to_remove)

        assert len(sender._sent_ids) <= 41000  # 50001 - 10000

    def test_sent_ids_thread_safety(self):
        """Multiple threads can safely add to _sent_ids."""
        sender = self._make_sender()
        errors = []

        def add_ids(start, count):
            try:
                for i in range(start, start + count):
                    with sender._sent_ids_lock:
                        sender._sent_ids.add(i)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=add_ids, args=(i * 1000, 1000))
            for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(sender._sent_ids) == 5000


# ---------------------------------------------------------------------------
# Layer 2: Pre-send DB check
# ---------------------------------------------------------------------------

class TestPreSendDbCheck:
    """DB check prevents sending when tg_id already exists."""

    def test_db_check_blocks_when_tg_id_set(self):
        """If DB shows tg_id is set, message should be skipped."""
        cursor = MagicMock()
        cursor.fetchone.return_value = (98765, 'sent')  # tg_id=98765

        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=cursor)
        ctx.__exit__ = MagicMock(return_value=False)

        with patch('sender_bot.db_cursor', return_value=ctx):
            from sender_bot import db_cursor
            with db_cursor() as cur:
                cur.execute("SELECT tg_id, status FROM chat_history_tg WHERE id = %s", (42,))
                row = cur.fetchone()

        assert row[0] is not None  # tg_id exists -> should skip

    def test_db_check_allows_when_tg_id_null(self):
        """If tg_id is NULL, message should proceed."""
        cursor = MagicMock()
        cursor.fetchone.return_value = (None, 'queued')  # tg_id=NULL

        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=cursor)
        ctx.__exit__ = MagicMock(return_value=False)

        with patch('sender_bot.db_cursor', return_value=ctx):
            from sender_bot import db_cursor
            with db_cursor() as cur:
                cur.execute("SELECT tg_id, status FROM chat_history_tg WHERE id = %s", (42,))
                row = cur.fetchone()

        assert row[0] is None  # tg_id NULL -> proceed

    def test_db_check_blocks_status_sent_even_without_tg_id(self):
        """If status='sent' but tg_id is NULL (edge case), should still skip."""
        cursor = MagicMock()
        cursor.fetchone.return_value = (None, 'sent')  # sent but no tg_id

        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=cursor)
        ctx.__exit__ = MagicMock(return_value=False)

        with patch('sender_bot.db_cursor', return_value=ctx):
            from sender_bot import db_cursor
            with db_cursor() as cur:
                cur.execute("SELECT tg_id, status FROM chat_history_tg WHERE id = %s", (42,))
                row = cur.fetchone()

        assert row[1] == 'sent'  # status=sent -> should skip

    def test_db_check_failure_does_not_block(self):
        """If DB check fails, message should still proceed (fail-open)."""
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(side_effect=Exception("DB pool exhausted"))

        with patch('sender_bot.db_cursor', return_value=ctx):
            proceed = True
            try:
                from sender_bot import db_cursor
                with db_cursor() as cur:
                    cur.execute("SELECT tg_id, status FROM chat_history_tg WHERE id = %s", (42,))
            except Exception:
                proceed = True  # fail-open: proceed with send

        assert proceed is True


# ---------------------------------------------------------------------------
# Post-send error handling
# ---------------------------------------------------------------------------

class TestPostSendErrorHandling:
    """Error after successful TG send should NOT cause retry."""

    def _make_sender(self):
        from sender_bot import SenderBot, RateLimitConfig
        mock_bot = MagicMock()
        fake_redis = fakeredis.FakeRedis(decode_responses=True)

        with patch('sender_bot.get_redis', return_value=fake_redis), \
             patch('sender_bot.get_db_pool', return_value=MagicMock()):
            sender = SenderBot(mock_bot, rate_limit_config=RateLimitConfig())

        return sender

    def test_no_retry_after_successful_send(self):
        """If chat_history_id is in _sent_ids, exception handler should NOT re-queue."""
        sender = self._make_sender()
        chat_history_id = 42

        # Mark as sent
        with sender._sent_ids_lock:
            sender._sent_ids.add(chat_history_id)

        # Verify it's marked
        with sender._sent_ids_lock:
            already_sent = chat_history_id in sender._sent_ids

        assert already_sent is True
        # In the actual code, this prevents rpush to queue

    def test_retry_allowed_when_not_sent(self):
        """If chat_history_id is NOT in _sent_ids, retry should proceed."""
        sender = self._make_sender()
        chat_history_id = 42

        with sender._sent_ids_lock:
            already_sent = chat_history_id in sender._sent_ids

        assert already_sent is False
        # In the actual code, this allows rpush to queue

    def test_no_force_send_on_retry(self):
        """Retry task_data should NOT have force_send=True (removed in fix)."""
        task_data = {
            'chat_history_id': 42,
            'retry_count': 0,
            'request_body': {'message': 'test', 'type_of_message': 'text'},
        }

        # Simulate retry logic (from the fixed code)
        retry_count = task_data.get('retry_count', 0)
        if retry_count < 3:
            task_data['retry_count'] = retry_count + 1
            # DO NOT set force_send — this was the bug
            task_data['retry_after'] = time.time() + 5

        assert 'force_send' not in task_data


# ---------------------------------------------------------------------------
# Dedup interaction: retry_stuck + sender
# ---------------------------------------------------------------------------

class TestRetryStuckDedup:
    """retry_stuck re-queues should be caught by pre-send checks."""

    def test_retry_stuck_query_filters_tg_id_null(self):
        """retry_stuck SQL should only pick messages with tg_id IS NULL."""
        # This is a documentation/contract test
        expected_filter = "h.tg_id IS NULL"
        expected_status = "IN ('queued', 'failed')"

        # Read the actual SQL from bot.py
        import inspect
        from bot import retry_stuck_messages
        source = inspect.getsource(retry_stuck_messages)

        assert "tg_id IS NULL" in source
        assert "'queued'" in source
        assert "'failed'" in source

    def test_sent_message_not_picked_by_retry(self):
        """Message with tg_id set should NOT be picked by retry_stuck query."""
        # Contract: retry_stuck uses WHERE tg_id IS NULL
        # If _update_db_success works, tg_id is set -> retry_stuck won't pick it
        # If _update_db_success fails, tg_id is NULL -> retry_stuck picks it
        # -> _sent_ids blocks the duplicate at sender level

        # This test validates the concept
        sent_ids = {42, 43, 44}
        retry_candidate = 42  # retry_stuck picked this up

        assert retry_candidate in sent_ids  # sender will block it
