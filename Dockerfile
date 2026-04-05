# AEON Telegram Bot - Bot API version (Production)
FROM python:3.11-slim

# Install ffmpeg (video notes) + fonts (PDF Cyrillic) + system deps
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    ffmpeg \
    fonts-dejavu-core \
    libpq-dev \
    gcc \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

# Use gunicorn for production
# --workers 1: single worker to avoid multiple bot instances
# --threads 4: use threads for concurrent requests
# --timeout 120: long timeout for async operations
# NO --preload: initialization must happen in worker process, not master
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--timeout", "120", "app:flask_app"]