# Stage 1: Base com Ubuntu e Dependências de GUI/RDE
# --- SRE FIX: Atualizado para 3.12 para bater com o pyproject.toml ---
FROM python:3.12-slim-bookworm AS base

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"
ENV DEBIAN_FRONTEND=noninteractive
ENV PLAYWRIGHT_BROWSERS_PATH=/app/pw-browsers
ENV DISPLAY=:1

# Instalação de dependências de sistema e GUI para noVNC
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl gpg ca-certificates \
    xvfb x11vnc novnc websockify \
    python3 python3-pip \
    libpq5 \
    # Dependências do Playwright
    libglib2.0-0 libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
    libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxext6 \
    libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 libcairo2 libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Instalação do OpenGravity (Antigravity)
RUN mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://us-central1-apt.pkg.dev/doc/repo-signing-key.gpg | gpg --dearmor --yes -o /etc/apt/keyrings/antigravity-repo-key.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/antigravity-repo-key.gpg] https://us-central1-apt.pkg.dev/projects/antigravity-auto-updater-dev/ antigravity-debian main" | tee /etc/apt/sources.list.d/antigravity.list > /dev/null \
    && apt-get update && apt-get install -y antigravity

# Stage 2: Builder (Gestão de Dependências com uv)
# --- SRE FIX: Atualizado para 3.12 para que o .venv gerado seja portável ---
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
WORKDIR /app
ENV PLAYWRIGHT_BROWSERS_PATH=/app/pw-browsers
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev
RUN .venv/bin/python -m playwright install chromium

# Stage 3: Runtime Imutável (Sem Bind Mounts)
FROM base AS runtime

# WHY: Build-args injetados pelo CI para rastreabilidade commit-level (Sentry + Grafana)
ARG GIT_SHA=local-dev
ARG BUILD_TIMESTAMP=unknown
ENV GIT_SHA=${GIT_SHA}
ENV BUILD_TIMESTAMP=${BUILD_TIMESTAMP}

# Copia o código para dentro da imagem (Imutabilidade)
COPY src/ /app/src/
COPY app_analytics.py /app/app_analytics.py
COPY infra/ /app/infra/
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/pw-browsers /app/pw-browsers

# Configuração de Segurança e Entrypoint (SRE FIX: Path amigável na raiz)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8501 6080 50051 

ENTRYPOINT ["/entrypoint.sh"]
