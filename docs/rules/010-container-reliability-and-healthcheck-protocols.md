# 010: Container Reliability and Healthcheck Protocols

## Status
✅ Active (2026-04-20)

## Context
Transient `502 Bad Gateway` errors were occurring during cold starts and post-commit rebuilds. Nginx (the edge router) was becoming ready before the upstream service (Streamlit) had finished its internal initialization and port binding (port 8501).

Current `docker-compose` orchestration relies on `service_healthy` conditions, but default healthcheck timings created a race condition where the container was "starting" but not yet "healthy" despite the process being up, or nginx failing its upstream connection before the first health poll finished.

## Decisions

### 1. Robust Start Period
- **Action**: Increase `start_period` for heavyweight services (e.g., Streamlit, Keycloak) to at least 90s.
- **Reason**: Rebuilding layers or initializing large Parquet files in memory can take significant time on local machines. 30s was causing premature health failures.

### 2. Fast Polling during Startup (`start_interval`)
- **Action**: Add `start_interval: 2s` to `5s` in the healthcheck block.
- **Requirement**: Requires Docker Compose V2 / Docker 25+.
- **Reason**: During the `start_period`, Docker normally waits the full `interval` (e.g., 30s) between tests. `start_interval` allows the container to be promoted to `healthy` as soon as it passes its first check, dramatically reducing wait time for dependent services (like the router) without inflating normal runtime resource usage.

### 3. Self-Healing Edge (On-Failure Restart)
- **Action**: Add `restart: on-failure` to the `router` service.
- **Reason**: If the upstream service restarts (OOM, crash, redeploy), Nginx might need a reset to clear its "dead upstream" state unless fine-tuned. A restart policy ensures the edge layer aligns with the state of the backend.

## Orchestration Patterns

### SOTA Healthcheck Block
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://127.0.0.1:8501/dashboard/_stcore/health"]
  interval: 30s        # Long-term polling
  timeout: 10s
  retries: 3
  start_period: 90s    # Cold-boot buffer
  start_interval: 5s   # Fast-track to Healthy status
```

### Dependency Chain
```yaml
router:
  depends_on:
    analytics:
      condition: service_healthy
```

## Diagnostic Protocol (502 Debugging)

1. **Check Status**: `docker ps` — Is the backend `healthy` or still `starting`?
2. **Check Logs**: `docker logs ger_router` — What is the upstream error? (ECONNREFUSED vs Timing out).
3. **Verify Port**: `docker exec ger_analytics netstat -tulpn` — Is Streamlit actually listening on 8501?
4. **Trigger Poll**: `docker inspect --format='{{json .State.Health}}' ger_analytics` — Check the history of healthcheck results.
