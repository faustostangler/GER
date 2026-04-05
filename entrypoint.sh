#!/bin/bash
set -e

# --- SRE FIX: Força o Linux a usar o Virtual Environment do uv ---
export PATH="/app/.venv/bin:$PATH"

# 1. Fail-fast: Validação de variáveis via Python/Pydantic
python -c "from infrastructure.config import settings; print('Config Validada')"

# --- SRE: GUI Stack Condicional (Skip no Cloud Run / Serverless) ---
# K_SERVICE é injetado automaticamente pelo Cloud Run; se existir, estamos em serverless.
if [ -z "$K_SERVICE" ]; then
    echo "🖥️  RDE Mode: Iniciando GUI Stack (Xvfb + VNC + noVNC)..."

    # --- SRE FIX 1: Self-Healing do Framebuffer ---
    rm -f /tmp/.X1-lock /tmp/.X11-unix/X1

    # 2. Inicializa o Framebuffer Virtual (Xvfb) para o Playwright
    Xvfb :1 -screen 0 1280x1024x24 &
    sleep 2

    # 3. Inicia o noVNC usando websockify diretamente
    websockify --web /usr/share/novnc/ 6080 127.0.0.1:5901 &

    # 4. Inicia o Servidor VNC
    x11vnc -display :1 -forever -nopw -listen 127.0.0.1 -rfbport 5901 &
else
    echo "☁️  Cloud Run Mode: Skipping GUI Stack (K_SERVICE=$K_SERVICE)"
fi

# 5. Executa o Processo Principal (Analytics ou Worker)
if [ "$ROLE" = "analytics" ]; then
    echo "Starting Analytics App on port ${PORT:-8501}..."
    exec streamlit run app_analytics.py \
         --server.port="${PORT:-8501}" \
         --server.address=0.0.0.0 \
         --server.headless=true \
         --server.baseUrlPath="/dashboard" \
         --server.enableCORS=false \
         --server.enableXsrfProtection=false
elif [ "$ROLE" = "worker" ]; then
    echo "Starting ARQ Worker..."
    exec arq src.infrastructure.queue.worker_settings.WorkerConfig
else
    echo "Error: Unknown ROLE '$ROLE'"
    exit 1
fi