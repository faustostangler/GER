from prometheus_client import Counter, Histogram, Gauge

# --- MÉTODO RED (Web Scraper / API Traffic) ---
# Rate, Errors, Duration
SCRAPER_PAGES_FETCHED = Counter(
    "gercon_scraper_pages_total",
    "Volume total de páginas paginadas do Gercon por lista",
    ["target_list"],
)

SCRAPER_ITEMS_SAVED = Counter(
    "gercon_scraper_items_total",
    "Volume total de registros individuais convertidos",
    ["target_list"],
)

SCRAPER_ERRORS_TOTAL = Counter(
    "gercon_scraper_errors_total",
    "Erros contabilizados por camada (network, parser, timeout)",
    ["error_type", "target_list"],
)

SCRAPER_DURATION_SECONDS = Histogram(
    "gercon_scraper_duration_seconds",
    "Latência (P90) de resolução das Promises JavaScript / Fetch Network no Angular",
    ["target_list"],
    buckets=[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 240.0],
)

# --- MÉTODO USE (Data Processing & Storage) ---
# Utilization, Saturation, Errors
INGEST_PIPELINE_DURATION = Histogram(
    "gercon_pipeline_job_duration",
    "Tempo total da janela de ingestão noturna consumida pelo K8s CronJob",
)

PARQUET_SIZE_BYTES = Gauge(
    "gercon_parquet_size_bytes", "Tamanho real do artefato final pós-transformação"
)

PIPELINE_LAST_SUCCESS_TIMESTAMP = Gauge(
    "gercon_pipeline_last_success",
    "Heartbeat Timestamp do último run completado com sucesso",
)
