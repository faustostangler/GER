# --- SOTA DOCKERFILE: Multi-Stage Modular Monolith ---

# Stage 1: Base - Dependências de Sistema OS
FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PLAYWRIGHT_BROWSERS_PATH=/app/pw-browsers

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    nginx \
    # Dependências do Playwright (Chromium)
    libglib2.0-0 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxext6 \
    libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Stage 2: Builder - Resolução rápida com UV
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PLAYWRIGHT_BROWSERS_PATH=/app/pw-browsers
WORKDIR /app

COPY pyproject.toml uv.lock ./
# Instala Dependências Primeiro
RUN uv sync --frozen --no-install-project --no-dev
# Baixa os browsers do Playwright ainda no builder (agora com binário disponível)
RUN .venv/bin/python -m playwright install chromium

# Stage 3: Runtime - Imagem de Produção SecOps
FROM base AS runtime

# Criar usuário sem privilégios para mitigar vulnerabilidades
RUN useradd -m appuser && \
    chown -R appuser:appuser /app && \
    chown -R appuser:appuser /var/lib/nginx /var/log/nginx && \
    touch /run/nginx.pid && chown appuser:appuser /run/nginx.pid

# Copiar artefatos do builder com as permissões corretas
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=builder --chown=appuser:appuser /app/pw-browsers /app/pw-browsers
COPY --chown=appuser:appuser . /app/

# Setup Nginx
COPY static/index.html /usr/share/nginx/html/index.html
COPY nginx.conf /etc/nginx/sites-available/default
COPY --chown=appuser:appuser entrypoint.sh /entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"

USER appuser

EXPOSE 80 8501

ENTRYPOINT ["/entrypoint.sh"]
