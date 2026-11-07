# Telegram copy trader. Build: docker compose build
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# MetaApi SDK (MT4/MT5 on Linux) is optional: --build-arg WITH_METAAPI=false to skip it.
ARG WITH_METAAPI=true

WORKDIR /app

# Dependencies first (cached layer).
COPY requirements.txt ./
RUN pip install -r requirements.txt \
    && if [ "$WITH_METAAPI" = "true" ]; then pip install "metaapi-cloud-sdk>=29"; fi

# Non-root runtime user; code stays root-owned (read-only for the app).
RUN groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --no-create-home --home-dir /app --shell /usr/sbin/nologin app

COPY copytrader/ ./copytrader/
COPY examples/ ./examples/
RUN chmod -R a+rX /app/copytrader /app/examples \
    && mkdir -p /app/data \
    && chown app:app /app/data

USER app

# Sessions, database, images, logs. config.yaml is mounted at /app/config.yaml.
VOLUME ["/app/data"]

CMD ["python", "-m", "copytrader", "run", "--config", "/app/config.yaml"]
