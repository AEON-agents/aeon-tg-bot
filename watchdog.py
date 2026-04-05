"""Graceful shutdown and thread watchdog."""

__all__ = ['_graceful_exit', 'thread_watchdog']

import sys
import time
import logging
import threading

import shared
from shared import (_shutdown_event, _health_state, _check_cooldown,
                    _consumer_stop_event, _retry_stop_event,
                    REDIS_URL, DATABASE_URL_DIRECT)
from pg_listener import pg_notify_listener_worker
from stuck_retry import stuck_messages_retry_worker
from incoming_consumer import incoming_message_consumer

logger = logging.getLogger(__name__)


def _graceful_exit(exit_code=1, timeout=15):
    """Graceful shutdown: stop all threads, close connections, then exit.

    Replaces os._exit() to prevent resource leaks and lost data.
    """
    logger.warning(f"[GRACEFUL_EXIT] Initiating graceful exit (code={exit_code}, timeout={timeout}s)")

    # Signal all threads to stop
    _shutdown_event.set()

    # Stop sender first (flushes queue)
    try:
        if shared.sender_bot:
            shared.sender_bot.stop()
            logger.info("[GRACEFUL_EXIT] SenderBot stopped")
    except Exception as e:
        logger.error(f"[GRACEFUL_EXIT] SenderBot stop error: {e}")

    # Join background threads with timeout
    threads_to_join = [
        ('PgListener', shared.pg_listener_thread),
        ('RetryWorker', shared.retry_worker_thread),
        ('IncomingConsumer', shared.incoming_consumer_thread),
    ]
    for name, thread in threads_to_join:
        if thread and thread.is_alive():
            thread.join(timeout=min(timeout / 3, 5))
            if thread.is_alive():
                logger.warning(f"[GRACEFUL_EXIT] {name} still alive after join")

    # Close connections
    try:
        from db import close_db_pool
        close_db_pool()
        logger.info("[GRACEFUL_EXIT] DB pool closed")
    except Exception:
        pass

    try:
        from redis_client import close_redis_pool
        close_redis_pool()
        logger.info("[GRACEFUL_EXIT] Redis pool closed")
    except Exception:
        pass

    logger.warning(f"[GRACEFUL_EXIT] Exiting with code {exit_code}")
    sys.exit(exit_code)


def thread_watchdog():
    """Monitor daemon threads and restart them if they die.

    Checks every 60 seconds. Uses per-thread stop events for safe restart.
    """
    CHECK_INTERVAL = 60
    HEALTH_LOG_INTERVAL = 300
    last_health_log = time.time()
    restart_counter = 0
    consecutive_pg_restarts = 0
    MAX_PG_RESTARTS = 3

    _shutdown_event.wait(10)
    logger.info("[WATCHDOG] Thread watchdog started")

    while not _shutdown_event.is_set():
        try:
            # Check PgNotifyListener
            pg_stale = (time.time() - _health_state.get('pg_listener_last_activity', 0)) > 120
            pg_dead = shared.pg_listener_thread and not shared.pg_listener_thread.is_alive()

            if pg_dead or (shared.pg_listener_thread and pg_stale):
                reason = "DEAD" if pg_dead else "STALE (no activity >120s)"
                logger.error(f"[WATCHDOG] PgNotifyListener {reason} -- restarting!")
                consecutive_pg_restarts += 1

                if consecutive_pg_restarts >= MAX_PG_RESTARTS:
                    logger.critical(f"[WATCHDOG] PG listener restarted {consecutive_pg_restarts}x -- initiating graceful exit")
                    _graceful_exit(1)

                if not _check_cooldown('pg_listener'):
                    logger.critical("[WATCHDOG] PG listener restart cooldown exceeded -- initiating graceful exit")
                    _graceful_exit(1)

                if shared.pg_listener_thread and shared.pg_listener_thread.is_alive():
                    shared.pg_listener_thread.join(timeout=2)
                    if shared.pg_listener_thread.is_alive():
                        logger.warning("[WATCHDOG] Old PgListener still alive -- skipping restart")
                        continue
                restart_counter += 1
                shared.pg_listener_thread = threading.Thread(
                    target=pg_notify_listener_worker,
                    args=(REDIS_URL, DATABASE_URL_DIRECT),
                    daemon=True,
                    name=f"PgNotifyListener-{restart_counter}"
                )
                shared.pg_listener_thread.start()
                logger.info(f"[WATCHDOG] PgNotifyListener restarted (#{restart_counter}, consecutive={consecutive_pg_restarts})")
            else:
                pg_last_act = _health_state.get('pg_listener_last_activity', 0)
                if pg_last_act and (time.time() - pg_last_act) < 60 and consecutive_pg_restarts > 0:
                    consecutive_pg_restarts = 0

            # Check StuckMessagesRetry
            if shared.retry_worker_thread and not shared.retry_worker_thread.is_alive():
                logger.error("[WATCHDOG] StuckMessagesRetry thread DEAD -- restarting!")
                if not _check_cooldown('retry_worker'):
                    logger.critical("[WATCHDOG] RetryWorker restart cooldown exceeded -- initiating graceful exit")
                    _graceful_exit(1)
                _retry_stop_event.set()
                shared.retry_worker_thread.join(timeout=2)
                _retry_stop_event.clear()
                restart_counter += 1
                shared.retry_worker_thread = threading.Thread(
                    target=stuck_messages_retry_worker,
                    daemon=True,
                    name=f"StuckMessagesRetry-{restart_counter}"
                )
                shared.retry_worker_thread.start()
                logger.info(f"[WATCHDOG] StuckMessagesRetry restarted (#{restart_counter})")

            # Check IncomingConsumer
            consumer_dead = shared.incoming_consumer_thread and not shared.incoming_consumer_thread.is_alive()
            consumer_stuck = False
            if shared.incoming_consumer_thread and shared.incoming_consumer_thread.is_alive():
                last_hb = _health_state.get('consumer_heartbeat', time.time())
                consumer_stuck = (time.time() - last_hb) > 30

            if consumer_dead or consumer_stuck:
                reason = "DEAD" if consumer_dead else f"STUCK (no heartbeat {time.time() - _health_state.get('consumer_heartbeat', 0):.0f}s)"
                logger.error(f"[WATCHDOG] IncomingConsumer {reason} -- restarting!")
                if not _check_cooldown('incoming_consumer'):
                    logger.critical("[WATCHDOG] IncomingConsumer restart cooldown exceeded -- initiating graceful exit")
                    _graceful_exit(1)
                _consumer_stop_event.set()
                if shared.incoming_consumer_thread and shared.incoming_consumer_thread.is_alive():
                    shared.incoming_consumer_thread.join(timeout=10)
                    if shared.incoming_consumer_thread.is_alive():
                        logger.warning("[WATCHDOG] Old IncomingConsumer still alive after 10s -- skipping restart")
                        _consumer_stop_event.clear()
                    else:
                        _consumer_stop_event.clear()
                        restart_counter += 1
                        shared.incoming_consumer_thread = threading.Thread(
                            target=incoming_message_consumer, daemon=True,
                            name=f"IncomingConsumer-{restart_counter}")
                        shared.incoming_consumer_thread.start()
                        _health_state['consumer_last_activity'] = time.time()
                        _health_state['consumer_heartbeat'] = time.time()
                        logger.info(f"[WATCHDOG] IncomingConsumer restarted (#{restart_counter})")
                else:
                    _consumer_stop_event.clear()
                    restart_counter += 1
                    shared.incoming_consumer_thread = threading.Thread(
                        target=incoming_message_consumer, daemon=True,
                        name=f"IncomingConsumer-{restart_counter}")
                    shared.incoming_consumer_thread.start()
                    _health_state['consumer_last_activity'] = time.time()
                    _health_state['consumer_heartbeat'] = time.time()
                    logger.info(f"[WATCHDOG] IncomingConsumer restarted (#{restart_counter})")

            # Check SenderBot worker
            if shared.sender_bot:
                thread_dead = shared.sender_bot.worker_thread and not shared.sender_bot.worker_thread.is_alive()
                flag_says_running = shared.sender_bot.is_running
                if thread_dead or (not flag_says_running and shared.sender_bot.worker_thread):
                    logger.error(f"[WATCHDOG] SenderBot worker DEAD (is_running={flag_says_running}, thread_alive={not thread_dead}) -- restarting!")
                    if not _check_cooldown('sender'):
                        logger.critical("[WATCHDOG] SenderBot restart cooldown exceeded -- initiating graceful exit")
                        _graceful_exit(1)
                    shared.sender_bot.is_running = False
                    shared.sender_bot.start()
                    logger.info("[WATCHDOG] SenderBot restarted")

            # Count dead critical threads
            critical_dead = 0
            if shared.pg_listener_thread and not shared.pg_listener_thread.is_alive():
                critical_dead += 1
            if shared.incoming_consumer_thread and not shared.incoming_consumer_thread.is_alive():
                critical_dead += 1
            if shared.sender_bot and shared.sender_bot.worker_thread and not shared.sender_bot.worker_thread.is_alive():
                critical_dead += 1

            if critical_dead >= 2:
                logger.critical(f"[WATCHDOG] {critical_dead} critical threads dead -- initiating graceful exit")
                _graceful_exit(1)

            if restart_counter >= 15:
                logger.critical(f"[WATCHDOG] {restart_counter} total restarts -- initiating graceful exit")
                _graceful_exit(1)

            # Periodic health status log
            now = time.time()
            if now - last_health_log >= HEALTH_LOG_INTERVAL:
                pg_alive = shared.pg_listener_thread.is_alive() if shared.pg_listener_thread else False
                retry_alive = shared.retry_worker_thread.is_alive() if shared.retry_worker_thread else False
                consumer_alive = shared.incoming_consumer_thread.is_alive() if shared.incoming_consumer_thread else False
                sender_alive = (shared.sender_bot.worker_thread.is_alive() if shared.sender_bot and shared.sender_bot.worker_thread else False)
                queue_len = 0
                sender_stats = {}
                try:
                    if shared.sender_bot:
                        sender_stats = shared.sender_bot.get_stats()
                        queue_len = sender_stats.get('queue_length', 0)
                except Exception:
                    pass
                degraded_str = " DEGRADED(polling)" if shared._listener_degraded else ""
                logger.info(
                    f"[WATCHDOG] Status: pg_listener={'alive' if pg_alive else 'DEAD'}{degraded_str} "
                    f"sender={'alive' if sender_alive else 'DEAD'} "
                    f"consumer={'alive' if consumer_alive else 'DEAD'} "
                    f"retry={'alive' if retry_alive else 'DEAD'} "
                    f"queue={queue_len} "
                    f"sent={sender_stats.get('sent', 0)} errors={sender_stats.get('failed', 0)}"
                )
                last_health_log = now

        except Exception as e:
            logger.error(f"[WATCHDOG] Error: {e}")

        _shutdown_event.wait(CHECK_INTERVAL)

    logger.info("[WATCHDOG] Stopped (shutdown)")
