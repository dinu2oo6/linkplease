FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DB_PATH=/data/linkplease.db

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY scripts ./scripts

EXPOSE 8080

# Exactly one worker, on purpose. The rate governor and the outbox claim are
# correct within a single writer; a second worker would independently think it
# owned 10 sends per minute and breach the limit. See FAILURES.md #2.
#
# Shell form so $PORT expands -- Render injects its own port and ignores EXPOSE.
CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1
