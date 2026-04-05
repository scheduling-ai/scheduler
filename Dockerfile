FROM ghcr.io/astral-sh/uv:python3.13-trixie-slim

RUN apt-get update && apt-get install -y --no-install-recommends nodejs npm \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN useradd --create-home appuser && chown -R appuser:appuser /app \
    && mkdir -p /data/live-state && chown appuser:appuser /data/live-state
USER appuser

COPY --chown=appuser:appuser py-scheduler/scheduler/ui/package.json py-scheduler/scheduler/ui/package-lock.json py-scheduler/scheduler/ui/
RUN cd py-scheduler/scheduler/ui && npm ci
COPY --chown=appuser:appuser py-scheduler/scheduler/ui/ py-scheduler/scheduler/ui/
RUN cd py-scheduler/scheduler/ui && npm run build

COPY pyproject.toml uv.lock ./
RUN mkdir -p py-scheduler/scheduler && touch py-scheduler/scheduler/__init__.py \
    && uv sync --frozen --no-dev
COPY --chown=appuser:appuser py-scheduler/ py-scheduler/
RUN uv sync --frozen --no-dev
