#!/bin/bash
set -e

# 1. Fail-fast: Validação de variáveis via Python/Pydantic
python -c "from src.infrastructure.config import settings; print('Config Validada')"

# 2. Inicializa o Framebuffer Virtual (Xvfb) para o Playwright
Xvfb :1 -screen 0 1280x1024x24 &
sleep 2

# 3. Inicia o noVNC (BFF de Apresentação)
/usr/share/novnc/utils/launch.sh --vnc 127.0.0.1.nip.io:5901 --listen 6080 &

# 4. Inicia o Servidor VNC
x11vnc -display :1 -forever -nopw -listen 127.0.0.1.nip.io -rfbport 5901 &

# 5. Executa o Processo Principal (API ou Worker)
if [ "$SERVICE_TYPE" = "worker" ]; then
    exec arq src.infrastructure.queue.worker_settings.WorkerConfig
else
    # SRE FIX: Servindo sob /dashboard para compatibilidade com Reverse Proxy e IAP
    exec streamlit run app_analytics.py --server.port=8501 --server.address=0.0.0.0 --server.baseUrlPath=dashboard
fi
