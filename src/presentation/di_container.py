import os
import streamlit as st
from infrastructure.config import settings


@st.cache_resource
def get_use_case():
    """WHY: Builds ClinicaPolicy from AppSettings here (presentation/infra),
    injecting it into AnalyticsUseCase via DI. The Use Case and Domain remain
    agnostic of settings — only the composition layer (this function) knows both.
    Ref: ADR-005 — Business Policy Extraction.
    """
    from infrastructure.repositories.duckdb_repository import (
        DuckDBAnalyticsRepository,
    )
    from application.use_cases.analytics_use_case import AnalyticsUseCase
    from domain.models import ClinicaPolicy

    try:
        from infrastructure.telemetry.metrics import init_prometheus

        init_prometheus(port=8001)
    except Exception:
        # WHY: Streamlit reruns this block on every interaction. The Prometheus
        # HTTP server raises OSError if the port is already bound (second run).
        # Metrics themselves are guarded by _get_or_create in metrics.py.
        pass

    # WHY: Wire-up of the business policy from configurable settings.
    # Domain has safe defaults; .env only overrides if necessary.
    policy = ClinicaPolicy(
        idade_min=settings.AGE_MIN,
        idade_max=settings.AGE_MAX,
        sla_dias_vencimento=settings.SLA_DIAS_VENCIMENTO,
        mes_comercial_dias=settings.MES_COMERCIAL_DIAS,
        data_sla_threshold_horas=settings.DATA_SLA_THRESHOLD,
        cores_urgencia=settings.CORES_URGENCIA,
    )

    try:
        repo = DuckDBAnalyticsRepository(settings.OUTPUT_FILE)
    except ValueError as e:
        st.error(f"🔌 **Circuit Breaker Triggered:** {e}")
        st.stop()

    return AnalyticsUseCase(repo, policy=policy)


@st.cache_resource
def get_identity_service():
    def _is_cloud_run() -> bool:
        return bool(os.getenv("K_SERVICE"))

    def _is_dev_mock_allowed() -> bool:
        environment = os.getenv("ENVIRONMENT", "production").lower()
        allow_dev = os.getenv("ALLOW_UNAUTHENTICATED_DEV", "false").lower() == "true"
        return environment in ("local", "dev") and allow_dev

    from infrastructure.auth.adapters import (
        CloudRunIdentityAdapter,
        MockIdentityAdapter,
        IAPIdentityAdapter,
    )

    if _is_cloud_run():
        return CloudRunIdentityAdapter(settings)
    if _is_dev_mock_allowed():
        return MockIdentityAdapter()
    return IAPIdentityAdapter(settings)
