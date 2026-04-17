# 001 - SOTA Development Orchestration & Performance

This document solidifies the knowledge generated regarding development workflow optimization for the GER project, focusing on eliminating development latency and ensuring Host-to-Container synchronization.

## 1. Environment Diagnosis & Resilience
- **Symptom**: Syntax errors (e.g., `IndentationError`) appearing on the dashboard that do not exist in the host source file.
- **Cause**: Docker Bind Mount desynchronization on Linux or stale Python `.pyc` compilation caches.
- **SRE Solution**: 
    1. Recreate the primary container: `make up`.
    2. Restart the edge router: `docker compose restart router` (to clear 502/504 Bad Gateways).

## 2. Ultra-Fast Build Strategy (BuildKit)
To prevent `--build` from taking excessive time, we implemented direct cache mounts in the `Dockerfile`:
- **uv Cache**: Stores Python wheels in `/root/.cache/uv`.
- **Playwright Cache**: Prevents repetitive downloads of Chromium binaries.
- **Implementation**: 
  ```dockerfile
  RUN --mount=type=cache,target=/root/.cache/uv uv sync ...
  ```

## 3. Definitive Live-Reload (Polling Strategy)
Due to `inotify` inefficiencies in Docker volumes on Linux, Streamlit has been configured for active monitoring:
- **File**: `.streamlit/config.toml`
- **Parameter**: `fileWatcherType = "poll"`
- **Result**: Streamlit actively checks files at fixed intervals, ensuring code saved on the host is executed in the container within ~1s.

## 4. Command Protocol (Makefile)

| Command | Primary Function | Impact |
| :--- | :--- | :--- |
| **`make refresh`** | **Forces instant reload** of Streamlit via `touch`. | Milliseconds |
| **`make up-iam`** | Starts the full stack (App + Keycloak + Proxy). | Fast (Polling active) |
| **`make up`** | Rebuilds images and updates dependencies. | Medium (Cached) |
| **`make restart`** | Restarts processes without recreating containers. | 2-3 seconds |

## 5. Golden Rules for Developers
1. **Trust the Volume**: Most changes in `app_analytics.py` and `src/` are automatic.
2. **Use Refresh**: If the UI hasn't updated within 2 seconds of saving, run `make refresh`.
3. **Build only on Infra-Change**: Only use `--build` when changing the `Dockerfile`, `pyproject.toml`, or Entrypoint scripts.
4. **DNS Resolution**: Always use `127.0.0.1.nip.io` to avoid CORS and OIDC redirect issues.
