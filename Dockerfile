# ── Stage 1: build wheels (has gcc + dev headers) ───────────────────────────
FROM python:3.12-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# build-only toolchain; exact apt pins rot on bookworm point releases — rebuilds break before they help
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libc6-dev libffi-dev libssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# always-latest build frontend for wheel building; app deps are pinned in requirements.txt
# hadolint ignore=DL3013
RUN pip install --upgrade pip setuptools wheel \
    && pip wheel --no-cache-dir --wheel-dir /wheels -r requirements.txt


# ── Stage 2: runtime (slim, no compilers) ───────────────────────────────────
FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# curl is the only runtime apt dep — used by HEALTHCHECK and ad-hoc shells.
# single runtime dep; exact apt pin rots on bookworm point releases — rebuilds break before they help
# hadolint ignore=DL3008
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps from pre-built wheels — no compiler in the runtime image.
COPY --from=builder /wheels /wheels
COPY requirements.txt .
# DL3003: the `cd` below is scoped inside a subshell for a single find —
# WORKDIR would leak the directory change into every later layer.
# hadolint ignore=DL3003
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels requirements.txt \
    # Slim botocore: it bundles API specs for ~350 AWS services (~70MB+). This
    # image only calls ecr + ce (best-effort infra_report). Keep those + sts and
    # the shared root specs (endpoints/partitions/etc); drop the rest.
    && ( cd /usr/local/lib/python3.12/site-packages/botocore/data \
         && find . -maxdepth 1 -type d ! -name . ! -name ecr ! -name ce ! -name sts -exec rm -rf {} + ) \
    # Drop byte-caches (runtime also disables them via PYTHONDONTWRITEBYTECODE).
    && find /usr/local/lib/python3.12/site-packages -depth -type d -name __pycache__ -exec rm -rf {} + \
    && find /usr/local/lib/python3.12/site-packages -name '*.pyc' -delete

# Non-root user. uid/gid 1000 matches the host `ubuntu` user on the EC2 VM
# so bind-mounted data/ and logs/ are directly writable from /home/ubuntu.
RUN groupadd --system --gid 1000 relaybot && \
    useradd  --system --uid 1000 --gid relaybot --home-dir /app --shell /usr/sbin/nologin relaybot && \
    mkdir -p /app/logs /app/data && \
    chown -R relaybot:relaybot /app

# Application code (.dockerignore excludes .env, keys/, config.ini, vendor/,
# graphify-out/, tests/, scripts/, *.db, *.log, .git/).
COPY --chown=relaybot:relaybot . .

USER relaybot

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/api/health || exit 1

CMD ["python", "-m", "uvicorn", "app.core.app:app", "--host", "0.0.0.0", "--port", "8000"]
