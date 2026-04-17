# 🏢 Rule 002: Dashboard Modernization & SRE-Grade Telemetry

This document summarizes the architectural and operational knowledge established during the GERCON Analytics Dashboard modernization session.

## 1. UI/UX: The "Digital Surgeon" Design System
The dashboard has been refactored to align with high-end, editorial-standard visual principles.

- **Aesthetics**: High-contrast dark theme with glassmorphism effects.
- **Primary Accent**: Neon Orange (#FF4B00) used for critical data points and active state markers.
- **Typography**: Editorial-scale fonts (Inter/Outfit) replacing browser defaults.
- **Custom Components**:
    - **KPI Cards**: Refactored `render_kpi` to use custom HTML/CSS for glassmorphism headers, dynamic icons, and high-readability metrics.
    - **Amber Alert**: A premium notification system for Data Freshness SLA violations, replacing native Streamlit warnings with custom neon-bordered HTML containers.

## 2. Data Pipeline: SQLite to Parquet Consolidation
Transitioned the data lake from raw SQLite to optimized Parquet for extreme dashboard performance.

- **Engine**: Used `PyArrow` for high-performance memory-efficient chunked processing (~5000 records per batch).
- **Scale**: Successfully processed 123,000+ clinical records.
- **Data Integrity**: 
    - Forced strict schema typing (Date, Category, Boolean, Float) to ensure DuckDB performance.
    - Resolved "Module Identity Mismatch" by strictly avoiding `src.` prefixes in internal imports, ensuring `isinstance()` and `match/case` operations work across service boundaries.
- **Cleanup**: Implemented `pd.set_option('future.no_silent_downcasting', True)` and explicit `infer_objects(copy=False)` to silence Pandas FutureWarnings during type casting.

## 3. Observability: SRE-Grade Telemetry
Metrics are no longer ephemeral; they are part of the long-running infrastructure.

- **Exporter**: Integrated the Prometheus HTTP server into the **Arq Worker** boot cycle.
- **Implementation**: Used the `on_startup` hook in `WorkerConfig` to call a centralized `init_prometheus()` helper.
- **Endpoint**: Metrics are now consistently available on port **8000**, even when the worker is idle.
- **Categories**:
    - **RED**: (Rate, Errors, Duration) for scrapers.
    - **USE**: (Utilization, Saturation, Errors) for data ingestion jobs.
    - **Business SLIs**: Tracking data freshness hours and clinical risk surges.

## 4. Identity & Security: Zero-Trust Resilience
Enhanced the security layer to handle enterprise-level identity constraints.

- **Large JWT Management**: Configured `oauth2-proxy` with a **Redis session store**. This bypasses the 4KB HTTP header limit by storing the full JWT in Redis and using a short session ID cookie.
- **Split-Horizon DNS**: Managed identity resolution via internal network aliases (`iam.127.0.0.1.nip.io`) to ensure seamless OIDC Discovery between services.
- **Domain Hardening**: Moved `OAUTH2_PROXY_EMAIL_DOMAINS` to environment configuration (`env/config.env`) to allow surgeons and staff to be restricted to specific hospital domains.

## 5. Execution Protocol (Akita Method)
- **Fail-Fast**: Configuration validation via Pydantic is performed at the entrypoint level.
- **Immutability**: Python modules are copied into the image during multi-stage builds, while bind mounts are reserved for development and data persistence (Volumes).
- **Graceful Degradation**: Infrastructure adapters (Redis/Prometheus) are designed with circuit breakers or "No-Op" fallbacks to prevent pipeline crashes during partial failures.
