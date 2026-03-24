# --- SOTA DOCKERFILE: Multi-Role Modular Monolith ---
FROM python:3.11-slim

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

# Install basic requirements
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv (The ultra-fast Rust-based manager)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install dependencies first for better caching
COPY pyproject.toml .
RUN uv pip install --no-cache-dir -r pyproject.toml

# Install Playwright and its system dependencies
RUN playwright install --with-deps chromium

# Copy the rest of the application
COPY . .

# Ensure env directory exists
RUN mkdir -p env

# Role-specific entrypoints defined in docker-compose
EXPOSE 8501

CMD ["streamlit", "run", "app_analytics.py", "--server.port=8501", "--server.address=0.0.0.0"]
