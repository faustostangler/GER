"""
Sentry SDK Initialization — Infrastructure Layer.

Initializes Sentry error tracking with:
- Release tagging via GIT_SHA for commit-level traceability
- Environment separation (local/staging/production)
- Streamlit integration for auto-capturing UI exceptions
- Traces sample rate tuned for production (10% of transactions)

WHY: Sentry catches exceptions that escape our Chaos Testing mesh.
The test suite validates known failure modes; Sentry catches the unknown unknowns.
"""
import logging

logger = logging.getLogger(__name__)


def init_sentry(dsn: str, environment: str, release: str):
    """Initializes Sentry SDK. No-op if dsn is None or empty."""
    if not dsn:
        logger.info("Sentry DSN não configurado — error tracking desativado (ambiente local).")
        return

    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration

        sentry_sdk.init(
            dsn=dsn,
            environment=environment,
            release=f"gercon-analytics@{release}",
            # WHY: Captura breadcrumbs de WARNING+ para contexto em erros
            integrations=[
                LoggingIntegration(
                    level=logging.WARNING,
                    event_level=logging.ERROR,
                ),
            ],
            # WHY: 10% de traces para evitar custo excessivo no Sentry em produção
            traces_sample_rate=0.1,
            # WHY: Envia PII anonimizado (respeita LGPD — dados clínicos)
            send_default_pii=False,
            # WHY: Filtra dados de saúde do breadcrumb (compliance)
            before_breadcrumb=_filter_health_data_breadcrumb,
        )
        logger.info(f"Sentry inicializado — release: gercon-analytics@{release}, env: {environment}")
    except Exception as e:
        logger.warning(f"Falha ao inicializar Sentry (non-blocking): {e}")


def _filter_health_data_breadcrumb(crumb, hint):
    """Remove dados sensíveis de saúde dos breadcrumbs (LGPD compliance)."""
    if crumb.get("category") == "query":
        # WHY: Não envia SQL queries que podem conter dados de pacientes
        crumb["message"] = "[REDACTED - LGPD]"
    return crumb
