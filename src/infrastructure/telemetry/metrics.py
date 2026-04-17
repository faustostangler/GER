from prometheus_client import Counter, Gauge, Histogram, REGISTRY


def _get_or_create(
    metric_cls: type,
    name: str,
    doc: str,
    labelnames: tuple = (),
    **kwargs,
):
    """
    WHY: Streamlit re-executes the entire script on every hot-reload and user
    interaction cycle. Module-level Prometheus metric definitions raise
    ValueError('Duplicated timeseries in CollectorRegistry') on the second
    registration attempt, crashing the app. This guard safely returns the
    already-registered collector when that happens, making metric initialization
    idempotent across Streamlit reruns without touching internal registry state
    at call sites.
    """
    try:
        return metric_cls(name, doc, labelnames, **kwargs)
    except ValueError:
        # Metric already registered from a prior Streamlit execution cycle.
        # Retrieve the existing collector via the base metric name (no suffix).
        return REGISTRY._names_to_collectors.get(name)


# ---------------------------------------------------------------------------
# RED METHOD — Web Scraper / API Traffic (Rate · Errors · Duration)
# ---------------------------------------------------------------------------

SCRAPER_PAGES_FETCHED: Counter = _get_or_create(
    Counter,
    "gercon_scraper_pages_total",
    "Total paginated pages fetched from Gercon per target list",
    ("target_list",),
)

SCRAPER_ITEMS_SAVED: Counter = _get_or_create(
    Counter,
    "gercon_scraper_items_total",
    "Total individual records converted and persisted",
    ("target_list",),
)

SCRAPER_ERRORS_TOTAL: Counter = _get_or_create(
    Counter,
    "gercon_scraper_errors_total",
    "Errors counted by layer (network, parser, timeout)",
    ("error_type", "target_list"),
)

SCRAPER_DURATION_SECONDS: Histogram = _get_or_create(
    Histogram,
    "gercon_scraper_duration_seconds",
    "Latency (P90) of JavaScript Promise resolution / Angular Fetch Network",
    ("target_list",),
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 240.0],
)

SCRAPER_SUCCESS_TOTAL: Counter = _get_or_create(
    Counter,
    "scraper_success_total",
    "Number of scraper execution cycles completed successfully",
)

SCRAPER_FAILURE_TOTAL: Counter = _get_or_create(
    Counter,
    "scraper_failure_total",
    "Number of scraper execution cycles that failed",
)

SCRAPER_SESSION_DURATION_SECONDS: Histogram = _get_or_create(
    Histogram,
    "scraper_session_duration_seconds",
    "Total duration of the scraper ingestion session",
    buckets=[10.0, 60.0, 300.0, 600.0, 1800.0, 3600.0],
)

# ---------------------------------------------------------------------------
# USE METHOD — Data Processing & Storage (Utilization · Saturation · Errors)
# ---------------------------------------------------------------------------

INGEST_PIPELINE_DURATION: Histogram = _get_or_create(
    Histogram,
    "gercon_pipeline_job_duration",
    "Total time of the nightly ingestion window consumed by the K8s CronJob",
)

PARQUET_SIZE_BYTES: Gauge = _get_or_create(
    Gauge,
    "gercon_parquet_size_bytes",
    "Actual size of the final post-transformation Parquet artifact in bytes",
)

PIPELINE_LAST_SUCCESS_TIMESTAMP: Gauge = _get_or_create(
    Gauge,
    "gercon_pipeline_last_success",
    "Heartbeat timestamp of the last successfully completed pipeline run",
)

# ---------------------------------------------------------------------------
# BUSINESS SUCCESS METRICS — Clinical Subdomain KPIs (Ubiquitous Measurement)
# Measure domain value, not just engineering signals.
# ---------------------------------------------------------------------------

DATA_FRESHNESS_HOURS: Gauge = _get_or_create(
    Gauge,
    "gercon_data_freshness_hours",
    "Age of the most recent data in hours (data SLA for Amber Alert)",
)

HIGH_RISK_PATIENTS_DETECTED: Gauge = _get_or_create(
    Gauge,
    "gercon_high_risk_patients_detected_total",
    "High-risk patients (Red/Orange/Yellow) detected in the last query",
)

# ---------------------------------------------------------------------------
# PRESENTATION LAYER METRICS — Streamlit Rendering Observability
# ---------------------------------------------------------------------------

RENDER_LATENCY: Histogram = _get_or_create(
    Histogram,
    "gercon_render_latency_seconds",
    "Streamlit component render latency by component name",
    ("component",),
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

SILENT_ERRORS: Counter = _get_or_create(
    Counter,
    "gercon_silent_errors_total",
    "Silently caught rendering errors by component name",
    ("component",),
)


# ---------------------------------------------------------------------------
# PROMETHEUS SERVER BOOTSTRAP
# ---------------------------------------------------------------------------


def init_prometheus(port: int = 8000) -> None:
    """Start the Prometheus HTTP scrape endpoint.

    Supports multi-process mode via the ``PROMETHEUS_MULTIPROC_DIR``
    environment variable (required for gunicorn/uvicorn deployments).

    Args:
        port: TCP port to expose the ``/metrics`` endpoint on.
    """
    import os

    from prometheus_client import CollectorRegistry, multiprocess, start_http_server

    from infrastructure.telemetry.logger import setup_structured_logger

    logger = setup_structured_logger("prometheus_init")
    multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")

    try:
        if multiproc_dir:
            registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(registry)
            start_http_server(port, registry=registry)
            logger.info(
                "Prometheus (MultiProc) listening on :%d  dir=%s", port, multiproc_dir
            )
        else:
            start_http_server(port)
            logger.info("Prometheus (SingleProc) listening on :%d", port)
    except Exception as exc:
        logger.error("Failed to start Prometheus server on :%d: %s", port, exc)
