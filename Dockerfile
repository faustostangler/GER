# --- SOTA DOCKERFILE: Multi-Role Modular Monolith (Web + Analytics) ---
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

# Install basic requirements + nginx
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    nginx \
    && rm -rf /var/lib/apt/lists/*

# Install uv (The ultra-fast Rust-based manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install dependencies
COPY pyproject.toml .
RUN uv pip install --no-cache-dir -r pyproject.toml

# Install Playwright and its system dependencies
RUN playwright install --with-deps chromium

# Copy the rest of the application
COPY . .

# Setup static files for Nginx
COPY static/index.html /usr/share/nginx/html/index.html
COPY nginx.conf /etc/nginx/sites-available/default

# Entrypoint script to run Nginx and Streamlit
# Movemos para /entrypoint.sh (fora de /app) para evitar que o volume mount (.:/app) oculte o arquivo na VM
RUN echo '#!/bin/bash\nnginx -g "daemon off;" & streamlit run app_analytics.py --server.port=8501 --server.address=0.0.0.0 --server.baseUrlPath=dashboard' > /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Role-specific entrypoints
EXPOSE 80 8501

ENTRYPOINT ["/entrypoint.sh"]
