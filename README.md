# aeon-tg-bot

The Telegram I/O service for **AEON** — an AI "employee" that works inside Telegram for businesses. This service is the bridge between Telegram and the agent system: it ingests every incoming message and delivers every agent reply — reliably, under rate limits, without duplicates.

One service in a multi-service system: `aeon-tg-receiver` → **`aeon-tg-bot`** → PostgreSQL → `aeon-main` (the agents).

## What it does

Two flows, fully decoupled through PostgreSQL + Redis:

**Incoming.** A Telegram update (via webhook, or from `aeon-tg-receiver`'s Redis queue) is saved to PostgreSQL (`chat_history_tg`). A PG trigger fires `NOTIFY`, and `aeon-main` agents pick it up over a `LISTEN` connection.

**Outgoing.** An agent inserts a row (`type='AEON'`, `status='queued'`). A PG trigger fires `NOTIFY telegram_send`; this service's listener pushes the task to a Redis queue, and the SenderBot worker delivers it via the Bot API.

Decoupling delivery through `LISTEN/NOTIFY` + a Redis queue means agents never block on Telegram, and the send path has exactly one place to enforce rate limits, deduplication, and retries.

## Reliability engineering

This is the hard part of running a Telegram bot in production, and where most of the work went:

- **Single process, 6 cooperating threads** — Flask request handler, an asyncio loop for aiogram, the Redis-queue SenderBot worker, a PostgreSQL `LISTEN` listener, an incoming-queue consumer, and a stuck-message retry net.
- **A watchdog thread** monitors all of them every 60s and restarts any that die or stall; after repeated listener failures it exits the process for a clean restart.
- **Three-layer send dedup** — id-based, content-based (md5 + time-slot), and a sent-marker key — so a message is never delivered twice, even across retries and restarts.
- **Sliding-window rate limiting in Redis** — per-chat, per-group, and two global limits, each kept under Telegram's caps with a safety margin.
- **Self-degrading retry** — if the `LISTEN` connection drops, the stuck-message poller tightens from a 60s to a 10s interval so nothing sits in `queued` for long.
- **Connection-pool discipline** — auto-reconnect, age recycling, statement timeouts, pool-exhaustion recovery.

Media handling covers the full Telegram surface: text, photos, video, voice, video notes (ffmpeg-cropped to a circle), stickers, reactions, albums (buffered and flushed as one row), and on-the-fly `.docx`/`.pdf`/`.txt` generation from Markdown with Cyrillic support.

## Stack

Python · Flask + gunicorn · aiogram 3 · PostgreSQL (`LISTEN/NOTIFY`, triggers) · Redis (queues, rate-limit sorted sets, dedup keys) · Docker · Railway

## Layout

```
bot.py            Flask app, webhook, DB CRUD, PG listener, consumer, retry worker, watchdog, REST API
sender_bot.py     SenderBot worker, Redis rate limiter, dedup, all send methods, document generation
handlers.py       message-type handler registry (text/photo/video/voice/document/sticker/…)
db.py             threaded connection pool with auto-reconnect + recycling
redis_client.py   shared Redis pool
tests/            rate limiter, dedup layers, queue management, sender, supergroups
```

Full architecture, schema, and env-var reference: see [`CLAUDE.md`](CLAUDE.md).

## Notes

Internal service of a closed product, published as a work sample. All secrets are read from the environment (`.env` is ignored).

## License

All rights reserved. Published for review only — not licensed for use, copying, or distribution. See [LICENSE](LICENSE).
