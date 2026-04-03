#!/bin/bash
set -e
export PATH="/app/.venv/bin:$PATH"

# 1. Fail-fast: Validação de variáveis via Python/Pydantic
python -c "from src.infrastructure.config import settings; print('Config Validada')"

# --- SRE FIX 1: Self-Healing do Framebuffer ---
# Remove arquivos de lock órfãos de crashs anteriores antes de iniciar o Xvfb
rm -f /tmp/.X1-lock /tmp/.X11-unix/X1

# 2. Inicializa o Framebuffer Virtual (Xvfb) para o Playwright
Xvfb :1 -screen 0 1280x1024x24 &
sleep 2

# --- SRE FIX 2: Modernização do path noVNC para Debian Bookworm ---
# 3. Inicia o noVNC usando websockify diretamente, que é mais estável no Debian
websockify --web /usr/share/novnc/ 6080 127.0.0.1:5901 &

# 4. Inicia o Servidor VNC (usando IP padrão para evitar problemas de DNS interno com nip.io)
x11vnc -display :1 -forever -nopw -listen 127.0.0.1 -rfbport 5901 &

# 5. Executa o Processo Principal (API ou Worker)
if [ "$SERVICE_TYPE" = "worker" ]; then
    exec arq src.infrastructure.queue.worker_settings.WorkerConfig
else
    # SRE FIX 3: Adicionado headless=true para o Streamlit não tentar abrir o navegador interno
    exec streamlit run app_analytics.py \
        --server.port=8501 \
        --server.address=0.0.0.0 \
        --server.baseUrlPath=dashboard \
        --server.headless=true
fi