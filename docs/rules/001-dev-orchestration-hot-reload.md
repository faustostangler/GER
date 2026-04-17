# 001 - Development Orchestration and Performance (SOTA)

This document consolidates the knowledge generated for optimizing the workflow in the GER project, focusing on eliminating development latency and ensuring synchronization between Host and Container.

## 1. Environment Diagnosis and Resilience
- **Symptom**: Syntax errors (e.g., `IndentationError`) that appear in the dashboard but do not exist in the original file.
- **Cause**: Docker Bind Mount desynchronization on Linux or Python compilation cache (.pyc) issues.
- **SRE Solution**: 
    1. Recreate the primary container: `make up`.
    2. Restart the edge router: `docker compose restart router` (to clear 502/504 Bad Gateways).

## 2. Ultra-Fast Build Strategy (BuildKit)
To prevent `--build` from taking forever, we implemented cache mounts directly in the `Dockerfile`:
- **uv Cache**: Stores Python wheels in `/root/.cache/uv`.
- **Playwright Cache**: Prevents repetitive downloads of Chromium binaries.
- **Implementation**: 
  ```dockerfile
  RUN --mount=type=cache,target=/root/.cache/uv uv sync ...
  ```

## 3. Definitive Live-Reload (Polling Strategy)
Due to the inefficiency of `inotify` in Docker volumes on Linux, Streamlit was configured for active monitoring:
- **File**: `.streamlit/config.toml`
- **Parameter**: `fileWatcherType = "poll"`
- **Result**: Streamlit actively checks the file at fixed intervals, ensuring that code saved on the host is executed in the container in ~1s.

## 4. Command Protocol (Makefile)

| Command | Primary Function | Impact |
| :--- | :--- | :--- |
| **`make refresh`** | **Forces instant reload** of Streamlit via `touch`. | Milliseconds |
| **`make up-iam`** | Starts the full stack (App + Keycloak + Proxy). | Fast (Polling active) |
| **`make up`** | Rebuilds images and updates dependencies. | Medium (With cache) |
| **`make restart`** | Restarts processes without recreating containers. | 2-3 seconds |

## 5. Developer Golden Rules
1. **Trust the Volume**: Most changes in `app_analytics.py` and `src/` are automatic.
2. **Use Refresh**: If the screen doesn't update within 2 seconds after saving, run `make refresh`.
3. **Build Only on Infra-Change**: Only use `--build` when changing the `Dockerfile`, `pyproject.toml`, or Entrypoint scripts.
4. **DNS Resolution**: Always use `127.0.0.1.nip.io` to avoid CORS and OIDC redirect issues.
