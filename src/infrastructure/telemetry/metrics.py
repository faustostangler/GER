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

SCRAPER_SUCCESS_TOTAL = Counter(
    "scraper_success_total",
    "Quantidade de ciclos de execução de scraper completos com sucesso."
)

SCRAPER_FAILURE_TOTAL = Counter(
    "scraper_failure_total",
    "Quantidade de ciclos de execução de scraper que falharam."
)

SCRAPER_SESSION_DURATION_SECONDS = Histogram(
    "scraper_session_duration_seconds",
    "Tempo total da sessão de ingestão do Scraper",
    buckets=[10.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]
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

# --- BUSINESS SUCCESS METRICS (Ubiquitous Measurement) ---
# Medem o valor do subdomínio clínico, não apenas sinais de engenharia.

DATA_FRESHNESS_HOURS = Gauge(
    "gercon_data_freshness_hours",
    "Idade do dado mais recente em horas (SLA de dados para Amber Alert)",
)

HIGH_RISK_PATIENTS_DETECTED = Gauge(
    "gercon_high_risk_patients_detected_total",
    "Pacientes de risco alto (Vermelho/Laranja/Amarelo) detectados na última consulta",
)

USER_DECISION_TIME_SECONDS = Histogram(
    "gercon_user_decision_time_seconds",
    "Tempo entre filtrar e exportar/auditar (eficiência UX do médico)",
    buckets=[5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0],
)
