"""
TDD: Domain tests for Data Freshness SLA via last_sync_at.

Validates the AnalyticKPIs model accepts and correctly exposes
the last_sync_at timestamp for the Amber Alert system.
"""
import time
import pytest
from src.domain.models import AnalyticKPIs


@pytest.fixture
def base_kpi_kwargs():
    """Fixture de dados mínimos para construir um AnalyticKPIs válido."""
    return dict(
        pacientes=100,
        eventos=500,
        esp_mae=10,
        sub_esp=25,
        medicos=15,
        cids=30,
        origens=5,
        lead_time=45.0,
        max_lead_time=180,
        span_dias=365,
        pac_urgentes=20,
        pac_vencidos=10,
        p90_lead_time=90.0,
        p90_esquecido=60.0,
    )


class TestDataFreshnessSLA:
    def test_last_sync_at_defaults_to_zero(self, base_kpi_kwargs):
        kpi = AnalyticKPIs(**base_kpi_kwargs)
        assert kpi.last_sync_at == 0.0

    def test_last_sync_at_accepts_epoch_timestamp(self, base_kpi_kwargs):
        now = time.time()
        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=now)
        assert kpi.last_sync_at == now

    def test_stale_data_detection_logic(self, base_kpi_kwargs):
        """Simula a lógica do Amber Alert: dados mais velhos que threshold."""
        two_hours_ago = time.time() - (2.5 * 3600)  # 2.5h atrás
        threshold_hours = 2.0

        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=two_hours_ago)
        age_hours = (time.time() - kpi.last_sync_at) / 3600

        assert age_hours > threshold_hours

    def test_fresh_data_within_sla(self, base_kpi_kwargs):
        """Dados recentes não devem disparar o Amber Alert."""
        thirty_min_ago = time.time() - (0.5 * 3600)  # 30min atrás
        threshold_hours = 2.0

        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=thirty_min_ago)
        age_hours = (time.time() - kpi.last_sync_at) / 3600

        assert age_hours < threshold_hours

    def test_zero_sync_at_means_unknown_freshness(self, base_kpi_kwargs):
        """Quando last_sync_at=0, significa que o sistema não conseguiu ler mtime."""
        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=0.0)
        # A UI deve tratar 0 como "desconhecido" e não disparar alerta
        assert kpi.last_sync_at == 0.0

    def test_cdc_last_sync_at_field_exists_in_schema(self, base_kpi_kwargs):
        """CDC Guard: Garante que o campo não seja removido em refactorings."""
        fields = AnalyticKPIs.model_fields
        assert "last_sync_at" in fields
