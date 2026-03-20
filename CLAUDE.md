# AEON Telegram Bot API (aeon-tg-bot)

## What This Service Does

Telegram bot service for the AEON platform. Bridges Telegram and the rest of the AEON ecosystem through two data flows:

1. **Incoming:** Receives messages from Telegram users via webhook (and from `aeon-tg-receiver` via Redis queue), saves them to PostgreSQL (`chat_history_tg`). A PG trigger fires `NOTIFY`, which `aeon-main` agents pick up via LISTEN.
2. **Outgoing:** Agents write to `chat_history_tg` with `type='AEON'` and `status='queued'`. A PG trigger fires `NOTIFY telegram_send` with the task payload. This service's PG listener catches it, pushes to Redis `telegram:send_queue`, and the SenderBot worker delivers to Telegram via Bot API with rate limiting.

**Service name on Railway:** `aeon-tg-bot`
**Port:** 8080 (gunicorn, 1 worker, 4 threads)

---

## Architecture

```
                          ┌──────────────────────────┐
                          │    aeon-tg-receiver       │
                          │  (separate service,       │
                          │   Telegram polling)       │
                          └─────────┬────────────────┘
                                    │ Redis: telegram:incoming_queue
                                    ▼
┌─────────────┐           ┌──────────────────────────────────────┐
│  Telegram   │──webhook──│              aeon-tg-bot              │
│  Bot API    │◄──send────│                                      │
└─────────────┘           │  Flask (gunicorn -w 1 --threads 4)   │
                          │  ├── Webhook handler                 │
                          │  ├── REST API endpoints              │
                          │  │                                   │
                          │  Daemon threads:                     │
                          │  ├── Aiogram event loop              │
                          │  ├── SenderBot (Redis queue worker)  │
                          │  ├── PgNotifyListener (LISTEN)       │
                          │  ├── IncomingConsumer (Redis BLPOP)  │
                          │  ├── StuckMessagesRetry (safety net) │
                          │  └── ThreadWatchdog (restarts dead)  │
                          └───────────┬──────────────────────────┘
                                      │
                          ┌───────────┼───────────┐
                          │           │           │
                          ▼           ▼           ▼
                     PostgreSQL     Redis     aeon-main
                     (chat_history)  (queues)  (LISTEN/NOTIFY
                                               for new messages)
```

### Thread Model (single process, 6+ threads)

| Thread | Purpose | Recovery |
|--------|---------|----------|
| Main | Flask/gunicorn request handling | gunicorn restarts |
| Aiogram loop | `asyncio.new_event_loop()` for async Telegram calls | Created on first use |
| SenderBotWorker | Processes `telegram:send_queue`, rate limits, sends to TG | Watchdog restarts |
| PgNotifyListener | `LISTEN telegram_send` on direct PG connection, pushes to Redis | Watchdog restarts, `os._exit(1)` after 5 consecutive failures |
| IncomingConsumer | `BLPOP telegram:incoming_queue`, feeds updates to aiogram dispatcher | Watchdog restarts |
| StuckMessagesRetry | Polls DB for stuck `queued` messages, re-queues them | Watchdog restarts |
| ThreadWatchdog | Monitors all threads, restarts dead ones every 60s | Runs indefinitely |

**Why `-w 1`:** Multiple workers would create duplicate bot instances, duplicate LISTEN connections, and double-process the same Redis queues.

---

## Files

| File | Lines | What it does |
|------|-------|--------------|
| `bot.py` | ~2700 | Core: Flask app, webhook handler, DB CRUD (user/chat/group), message saving, aiogram handlers, PG listener, incoming consumer, retry worker, watchdog, all REST API endpoints, startup/shutdown |
| `sender_bot.py` | ~1600 | Outgoing: SenderBot class, RedisRateLimiter (sliding window), ChatActionManager (typing indicators with dedup), document generation (docx/pdf/txt from markdown), all send methods (text/photo/video/voice/sticker/video_note/media_group/reaction/delete), task processing with dedup and retry |
| `handlers.py` | ~307 | Message type handler registry pattern. Each type (text, photo, video, voice, document, sticker, video_note, media_group, reaction, delete, typing) registers a handler via decorator |
| `db.py` | ~255 | DB connection pool (`ThreadedConnectionPool`, minconn=2, maxconn=10) with auto-reconnect, connection age recycling (5 min), statement timeout (10s), context managers `db_connection()` and `db_cursor()` |
| `redis_client.py` | ~99 | Shared Redis connection pool (max 20), lazy init, `get_redis()`, `create_redis_client()`, `wait_for_redis()` with exponential backoff |
| `Dockerfile` | 31 | Python 3.11-slim + ffmpeg + DejaVu fonts + gunicorn |
| `requirements.txt` | 22 | aiogram 3.13.1, Flask 3.0.3, gunicorn, psycopg2-binary, redis, aiohttp, requests, python-docx, reportlab |

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `BOT_TOKEN` | Yes | Telegram Bot API token |
| `DATABASE_URL` | Yes | PostgreSQL connection string (through Supavisor pooler) |
| `REDIS_URL` | Yes | Redis connection string |
| `BASE_URL` | Yes* | Public URL of this service (for webhook registration and media URL expansion). *Required for webhook to work |
| `WEBHOOK_PATH` | No | Webhook endpoint path (default: `/webhook/telegram`) |
| `WEBHOOK_SECRET` | No | Secret token for webhook verification (`X-Telegram-Bot-Api-Secret-Token` header) |
| `N8N_WEBHOOK_URL` | No | If set, incoming messages are forwarded to this n8n webhook URL |
| `DATABASE_URL_DIRECT` | No | Direct PG connection (port 5432, not through Supavisor). **Required for LISTEN/NOTIFY** — Supavisor does not support LISTEN. Falls back to `DATABASE_URL` if not set |
| `MEDIA_CACHE_DIR` | No | Directory for downloaded media cache (default: `/tmp/media_cache`) |
| `PORT` | No | Server port (default: `8080`) |

---

## API Endpoints

### Webhook
| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/webhook/telegram` | Telegram webhook handler. Verifies `WEBHOOK_SECRET` if set. Feeds update to aiogram dispatcher |

### Health
| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Returns JSON with status of all components (bot, sender, redis, db, pg_listener, consumer, retry worker, queue length, ffmpeg, db pool stats). Returns 503 if bot or Redis down |

### Send Messages (Direct API — bypasses queue)
| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/telegram/send` | Send text message. Accepts `telegram_id` or `chat_id` (resolved to telegram_id). Tries Markdown parse mode, falls back to plain text |
| `POST` | `/telegram/typing` | Send chat action (typing, upload_photo, record_video, etc.) |
| `POST` | `/telegram/reaction` | Set reaction on message. Body: `{telegram_id, message_id, emoji}` |
| `POST` | `/telegram/sticker` | Send sticker by set name + emoji |
| `POST` | `/telegram/video_note` | Send video note (circle). Accepts URL or base64. Uses ffmpeg to crop to 384x384 square |
| `POST` | `/telegram/document` | Send document from URL or base64 |
| `POST` | `/telegram/forward` | Forward message between chats |

### Reactions & Pins
| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/telegram/message/{telegram_id}/{message_id}/reaction` | Set reaction on message |
| `DELETE` | `/telegram/message/{telegram_id}/{message_id}/reaction` | Remove reaction |
| `POST` | `/telegram/chat/{chat_id}/pin` | Pin message. Body: `{message_id, notify}` |
| `POST` | `/telegram/chat/{chat_id}/unpin` | Unpin message (or most recent if no message_id) |
| `POST` | `/telegram/chat/{chat_id}/unpin_all` | Unpin all messages |

### Media Download
| Method | Path | Purpose |
|--------|------|---------|
| `GET/POST` | `/telegram/voice/download` | Download voice by file_id (returns file or base64) |
| `POST` | `/telegram/file/download` | Download any file by file_id |
| `GET` | `/telegram/message/{telegram_id}/{message_id}/media/download` | Download media attached to a message (looks up file_id from DB) |
| `POST` | `/api/media/download` | Download media by file_id, save to cache |
| `POST` | `/api/media/get` | Get media as base64 (from cache or download on-demand) |
| `GET` | `/api/media/serve/{history_id}/{filename}` | Serve cached media file directly |

### Avatars
| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/user/{telegram_id}/avatar` | Get user profile photo (JPEG, cached 1h) |
| `GET` | `/api/chat/{chat_id}/avatar` | Get chat/group photo (JPEG, cached 1h) |

### Sender Control
| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/telegram/sender/stats` | Get sender statistics (sent, failed, retried, duplicates, rate limiter stats, chat action stats) |
| `POST` | `/telegram/sender/start` | Start sender if stopped |
| `POST` | `/telegram/sender/stop` | Stop sender |

### Retry
| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/telegram/retry_stuck` | Manually retry stuck messages. Body: `{max_age_minutes}` |

### Temp Files
| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/files/temp/{filename}` | Serve ephemeral temp files |

### Internal
| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/internal/update_message` | Update message status from PG triggers. Not for external use |

---

## Database Tables

All tables are in `public` schema.

### `users_tg`
Stores Telegram users.
```
id              SERIAL PRIMARY KEY
telegram_id     BIGINT UNIQUE
name            TEXT (first_name)
last_name       TEXT
username        TEXT
is_bot          BOOLEAN
status          TEXT (default 'unblocked')
```
**Operations:** `get_or_create_user()` — upsert by `telegram_id`, updates name/username on every message.

### `groups_tg`
Stores Telegram groups.
```
id                  SERIAL PRIMARY KEY
telegram_group_id   BIGINT UNIQUE  -- positive, without minus sign
title               TEXT
```
**Operations:** `get_or_create_group()` — upsert by `telegram_group_id`. Returns `telegram_group_id` (NOT internal `id`).

### `chats_tg`
Links users/groups to internal chat IDs used throughout the system.
```
id          SERIAL PRIMARY KEY
type        TEXT ('user' or 'group')
user_id     INTEGER REFERENCES users_tg(id)   -- for type='user'
group_id    INTEGER                            -- telegram_group_id for type='group'
```
**Operations:** `get_or_create_chat()` — lookup by user_id or group_id. One chat per user, one chat per group.

### `groups_users_tg`
Group membership.
```
id          SERIAL PRIMARY KEY
chat_id     INTEGER  -- telegram_group_id (NOT groups_tg.id)
user_id     INTEGER  -- users_tg.id
role        TEXT (default 'member')
status      TEXT (default 'active')
```

### `chat_history_tg`
All messages (incoming and outgoing).
```
id                  SERIAL PRIMARY KEY
chat_id             INTEGER REFERENCES chats_tg(id)
message             TEXT
type                TEXT ('user' for incoming, 'AEON' for outgoing)
tg_id               INTEGER  -- Telegram message_id (NULL until sent for outgoing)
type_of_message     TEXT (text/photo/video/voice/document/sticker/video_note/media_group/reaction)
group_sender_id     BIGINT   -- sender's telegram_id in groups
reply_to            INTEGER  -- tg_id of replied message
files_path          TEXT[]   -- array of Telegram file_id strings
status              TEXT (unread/queued/sent/failed)
created_at          TIMESTAMP
```
**Unique constraint:** `(chat_id, tg_id)` — upsert on conflict updates message/type_of_message/group_sender_id/files_path.

**PG triggers on this table:**
- `notify_new_message()` — fires on INSERT, sends `NOTIFY` for `aeon-main` agents to pick up new incoming messages
- Outgoing trigger — fires on INSERT where `type='AEON'`, sends `NOTIFY telegram_send` with task JSON payload for the PG listener in this service

### `update_message_status()` (PG function)
Called by SenderBot after successful/failed send to update `tg_id` and `status`.

---

## Redis Queues and Keys

### Queues
| Key | Direction | Format |
|-----|-----------|--------|
| `telegram:send_queue` | Outgoing | JSON task: `{chat_history_id, telegram_chat_id, chat_ident, request_body: {type_of_message, message, ...}, retry_count, retry_after, queued_at}` |
| `telegram:incoming_queue` | Incoming | Raw Telegram Update JSON (from `aeon-tg-receiver` service) |

### Rate Limiting Keys (sorted sets, sliding window)
| Key Pattern | TTL | Purpose |
|-------------|-----|---------|
| `ratelimit:chat:{chat_id}` | 5s | Per-chat: 1 msg/sec |
| `ratelimit:group:{chat_id}` | 120s | Per-group: 20 msg/min (x0.85 safety) |
| `ratelimit:global:msg` | 5s | Global: 25 msg/sec (x0.85 safety) |
| `ratelimit:global:api` | 5s | Global API: 25 req/sec (x0.85 safety) |

### Deduplication Keys
| Key Pattern | TTL | Purpose |
|-------------|-----|---------|
| `telegram:sent:{chat_history_id}` | 600s | Prevent re-sending same chat_history_tg row |
| `tg:dedup:{chat_id}:AEON:{msg_type}:{session_id}:{md5}:{5min_slot}` | 600s | Content-based dedup (same message text in 5-min window) |

### Chat Action Dedup
| Key Pattern | TTL | Purpose |
|-------------|-----|---------|
| `chataction:{chat_id}:{action}` | 4s | Prevent spamming same typing indicator |

---

## Key Patterns and Conventions

### Async-Sync Bridge
The codebase mixes sync (Flask, DB operations) and async (aiogram, Telegram API). The bridge is:
- `get_main_loop()` — creates a dedicated asyncio event loop in a daemon thread
- `run_async(coro, timeout=30)` — submits coroutine to that loop via `run_coroutine_threadsafe()`, waits for result with timeout, cancels on timeout to prevent zombie coroutines
- Inside aiogram handlers, DB calls use `await asyncio.to_thread()` to avoid blocking the event loop

### Error Handling Pattern
All DB operations use the `db_cursor(commit=True)` or `db_connection()` context managers which handle:
- Auto-retry up to 3 times on connection/SSL errors
- Pool exhaustion recovery (resets entire pool)
- Connection age recycling (max 5 minutes per connection)
- Statement timeout (10 seconds)

### Media Group (Album) Handling
Media groups arrive as separate messages with the same `media_group_id`. The system:
1. Buffers messages in `media_group_buffer` dict
2. Starts a 1.5-second flush timer on first message
3. After timer, saves as single `chat_history_tg` row with `files_path` array containing all file_ids
4. Tracks flushed groups in `media_group_flushed` for 60 seconds to handle late arrivals (updates the DB row)

### Outgoing Message Flow (PG Trigger -> Redis -> Telegram)
1. Agent writes to `chat_history_tg` with `type='AEON'`, `status='queued'`
2. PG trigger fires `NOTIFY telegram_send` with JSON payload containing `chat_history_id`, `telegram_chat_id`, `request_body`
3. `PgNotifyListener` thread catches the NOTIFY, pushes JSON to `telegram:send_queue` in Redis
4. `SenderBot._async_worker()` does `BLPOP` on the queue
5. Deduplication check (content-based + id-based)
6. Rate limit check (per-chat, per-group, global)
7. Chat action sent (typing indicator with cooldown dedup)
8. Message dispatched through `MessageHandlers` registry
9. DB updated with `tg_id` and `status='sent'` on success, or `status='failed'` on failure

### Handler Registry Pattern (`handlers.py`)
```python
@MessageHandlers.register('photo')
async def handle_photo(sender, chat_id, body):
    ...
    return HandlerResult(success=True, tg_message_id=msg_id)
```
Each handler receives the SenderBot instance, telegram chat_id, and request body dict. Returns `HandlerResult` with success flag, optional tg_message_id, error, and stat_key.

### Text Formatting
- Outgoing text: Markdown converted to HTML via `_markdown_to_html()` (supports bold, italic, code, links, strikethrough). Falls back to plain text if HTML parsing fails.
- Direct API (`/telegram/send`): Tries Markdown parse mode first, falls back to plain text.

### Listener Degradation
When the PG LISTEN connection breaks:
- `_listener_degraded` flag is set to `True`
- `StuckMessagesRetry` worker switches from 60s polling interval to 10s
- Stuck messages are picked up after just 5 seconds instead of 60 seconds
- When LISTEN reconnects, flag is cleared and normal intervals resume

### Graceful Shutdown
- `_shutdown_event` (threading.Event) signals all daemon threads to stop
- SIGTERM/SIGINT handlers call `on_shutdown()` which closes bot session, DB pool, Redis pool
- Per-thread stop events (`_consumer_stop_event`, `_retry_stop_event`) allow watchdog to restart individual threads without killing all

### Watchdog Self-Healing
The `thread_watchdog` (60s interval) monitors:
- **PgNotifyListener**: Restarts if thread is dead OR no activity for >120s. After 5 consecutive restarts, kills the process (`os._exit(1)`) for clean Railway restart
- **StuckMessagesRetry**: Restarts if thread is dead
- **IncomingConsumer**: Restarts if thread is dead OR stuck for >60s
- **SenderBot worker**: Restarts if thread is dead but `is_running` flag still True

Logs health status every 5 minutes (thread states, queue length, sent/failed counters).

---

## Document Generation

SenderBot generates documents from markdown text:

| Format | Library | Cyrillic Support | Features |
|--------|---------|------------------|----------|
| `.docx` | python-docx | Native | Headers, bold, italic, strikethrough, code (Courier New, red), bullet/numbered lists |
| `.pdf` | reportlab | DejaVu fonts (installed in Docker) | Same formatting, font family registration for bold/italic mapping |
| `.txt` | Built-in | UTF-8 | Strips markdown formatting |

Triggered via Redis queue when `type_of_message='document'` and no `document_url` provided (falls back to generating from `message` field).

---

## Integration with AEON Ecosystem

### How Agents Send Messages
Agents (in `aeon-main`) use the MCP tool `telegram_send` which inserts into `chat_history_tg` with `type='AEON'`. The PG trigger `NOTIFY telegram_send` pushes the task to this service.

### How Agents Receive Messages
This service saves incoming messages to `chat_history_tg`. A separate PG trigger fires `NOTIFY new_message` which `aeon-main`'s agent loop picks up via its own LISTEN connection.

### Receiver Service
`aeon-tg-receiver` is a separate service that uses Telegram polling (not webhook) and pushes raw Update JSON to `telegram:incoming_queue` in Redis. This service's `IncomingConsumer` thread processes those updates identically to webhook updates.

### Data Flow Diagram
```
User sends TG message
    → Webhook/Receiver → bot.py saves to chat_history_tg
    → PG trigger: NOTIFY new_message
    → aeon-main (agent_loop.py) picks up via LISTEN
    → Agent processes, calls telegram_send MCP tool
    → Inserts into chat_history_tg (type='AEON', status='queued')
    → PG trigger: NOTIFY telegram_send
    → PgNotifyListener pushes to Redis telegram:send_queue
    → SenderBot processes, sends via Bot API
    → Updates chat_history_tg (tg_id=X, status='sent')
```

### ID Resolution
- `telegram_id` = Telegram user ID (positive integer)
- `telegram_group_id` = Telegram group ID stored as positive integer in `groups_tg`
- Telegram chat_id for groups = negative (`-telegram_group_id`)
- Internal `chat_id` = `chats_tg.id` (auto-increment, used in `chat_history_tg`)
- `resolve_telegram_chat_id(internal_chat_id)` converts internal to Telegram format

---

## Testing Approach

### No Tests Exist Yet
The codebase currently has zero tests. Here is what should be tested and how:

### What to Mock
- **Redis:** Use `fakeredis` or mock `redis.Redis` — all Redis operations are through `get_redis()` singleton
- **PostgreSQL:** Mock `db_cursor()` and `db_connection()` context managers from `db.py`
- **Telegram Bot API:** Mock `aiogram.Bot` methods (`send_message`, `send_photo`, `get_file`, etc.)
- **aiohttp:** Mock for media download tests in handlers
- **threading:** Use `unittest.mock.patch` for thread creation in watchdog tests

### What to Test (Priority Order)
1. **Rate limiter** — `RedisRateLimiter` sliding window logic (uses only Redis sorted sets, easy to test with fakeredis)
2. **Deduplication** — Content-based dedup in `_async_process_task()`, id-based dedup
3. **Handler registry** — Each handler in `handlers.py` processes correct fields from body
4. **DB CRUD** — `get_or_create_user/chat/group`, `save_message_to_db` with upsert
5. **Media group buffering** — `flush_media_group`, late arrival handling
6. **Retry worker** — `retry_stuck_messages` query and re-queue logic
7. **ID resolution** — `resolve_telegram_chat_id` for users vs groups
8. **Document generation** — docx/pdf/txt from markdown
9. **Connection pool** — Reconnect, age recycling, pool reset
10. **Watchdog** — Thread restart logic, degradation detection

### Test Setup Pattern
```python
import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_db():
    with patch('db.get_db_pool') as mock_pool:
        # ... setup mock cursor
        yield mock_pool

@pytest.fixture
def mock_redis():
    import fakeredis
    client = fakeredis.FakeRedis(decode_responses=True)
    with patch('redis_client.get_redis', return_value=client):
        yield client
```

---

## Known Issues

### Critical
1. **bot.py is ~2700 lines** — monolithic file handles DB CRUD, API endpoints, PG listener, consumer, retry worker, watchdog. Should be split into modules (e.g., `api_endpoints.py`, `pg_listener.py`, `workers.py`, `db_helpers.py`)
2. **No `_getconn_with_timeout`** — unlike `aeon-main`'s `db_pool.py`, `pool.getconn()` here can block forever if Supavisor hangs. The `db_connection()` context manager retries on errors but does not timeout on the initial `getconn()` call
3. **PG listener connection timeout** — `_pg_connect_with_timeout()` wraps connection in a thread with timeout, but if `conn.poll()` hangs, there is no timeout (it runs in the main listener thread)

### Moderate
4. **Media group race condition** — If messages arrive >1.5s apart (slow network), some may be saved as late arrivals (updates existing row) but the order in `files_path` array may not match the original Telegram order
5. **Direct API endpoints bypass queue** — `/telegram/send`, `/telegram/sticker`, etc. call Bot API directly from Flask thread, bypassing rate limiting and deduplication. Only SenderBot queue path has these protections
6. **gunicorn 4 threads share DB pool** — maxconn=10 pool is shared across 4 Flask threads + SenderBot + consumer + retry worker. Under load, `PoolError` is possible (handled by retry but adds latency)
7. **No retry for direct API endpoints** — If `/telegram/send` fails, it returns error immediately with no retry

### Minor
8. **Markdown-to-HTML conversion** — `_markdown_to_html()` uses regex-based conversion which may break on nested formatting or edge cases
9. **`run_async` 30s default timeout** — Some operations (large media download) may exceed 30s
10. **Temp file cleanup** — `/files/temp/` serves from system temp dir but there is no cleanup mechanism for old files

---

## Deploy

```bash
# Railway CLI (preferred)
railway up

# Or git push (if GitHub connected to Railway)
git push origin main
```

### Docker (local testing)
```bash
docker build -t aeon-tg-bot .
docker run -p 8080:8080 \
  -e BOT_TOKEN=... \
  -e DATABASE_URL=... \
  -e REDIS_URL=... \
  -e BASE_URL=https://your-url.railway.app \
  aeon-tg-bot
```

### Important Deploy Notes
- **Single worker required:** `gunicorn -w 1` is mandatory. Multiple workers create duplicate bot instances and PG listeners
- **Webhook registration:** On startup, bot calls `bot.set_webhook()` with `BASE_URL + WEBHOOK_PATH`. If `BASE_URL` is not set, webhook is not registered
- **PG LISTEN requires direct connection:** Set `DATABASE_URL_DIRECT` to a direct PG connection (port 5432), not through Supavisor. Without it, the PG listener and retry worker will not start
- **ffmpeg required:** Installed in Docker image. Needed for video note cropping (square format)
- **DejaVu fonts required:** Installed in Docker image. Needed for Cyrillic text in PDF generation

---

## Code Conventions

- **Logging:** Uses Python `logging` module. Emoji prefixes in log messages for visual scanning (but not required in new code)
- **Sync-async bridge:** Never call `await` from sync code — always use `run_async()` or `asyncio.to_thread()`
- **DB access:** Always use `db_cursor()` or `db_connection()` context managers — never call `pool.getconn()` directly
- **Redis access:** Use `get_redis()` for shared client, `create_redis_client()` for isolated contexts
- **Error handling:** Catch specific exceptions (`psycopg2.OperationalError`, `TelegramRetryAfter`, etc.) before generic `Exception`
- **Thread safety:** DB pool uses `ThreadedConnectionPool`, Redis uses connection pool. Global state protected by locks (`_init_lock`, `_pool_lock`)
- **Shutdown:** All background threads check `_shutdown_event.is_set()` in their loops and exit cleanly
