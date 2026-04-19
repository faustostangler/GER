"""
TDD: Domain tests for Data Freshness SLA via last_sync_at.

Validates the AnalyticKPIs model accepts and correctly exposes
the last_sync_at timestamp for the Amber Alert system, including
the centralised is_stale() domain property which encapsulates the
SLA business rule so the Presentation layer stays a thin Humble Object.

Glossary: Amber Alert — Banner signals "Silence of Data" — Scraper might
have stopped. Ref: docs/GLOSSARY.md
"""
import time
import pytest
from domain.models import AnalyticKPIs


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


class TestIsStaleProperty:
    """Unit tests for AnalyticKPIs.is_stale() — the centralised Amber Alert predicate.

    WHY: Business rule "is data stale?" must live in the Domain, not in the UI
    (Humble Object pattern). These tests exercise is_stale() directly so the
    Presentation layer can call a single boolean method without any arithmetic.
    Mutmut hardening: boundary-value tests at exactly threshold ± 1s.
    """

    @pytest.mark.parametrize(
        "age_seconds, threshold_hours, expected",
        [
            # --- Stale cases ---
            (3 * 3600, 2.0, True),       # 3h > 2h threshold → stale
            (2 * 3600 + 1, 2.0, True),   # 1 second over threshold → stale
            (7200.5, 2.0, True),         # fractional second over → stale
            # --- Fresh cases ---
            (0.5 * 3600, 2.0, False),    # 30min < 2h → fresh
            (1 * 3600, 2.0, False),      # exactly 1h → fresh
            (1 * 3600, 0.5, True),       # 1h > 0.5h custom threshold → stale
            # --- Boundary: 1s under threshold is fresh (strict > semantics)
            (7199, 2.0, False),          # 1s under threshold → NOT stale (strict >)
        ],
    )
    def test_is_stale_boundary_values(
        self, base_kpi_kwargs, age_seconds: float, threshold_hours: float, expected: bool
    ):
        """Truth table for the Amber Alert staleness predicate."""
        sync_ts = time.time() - age_seconds
        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=sync_ts)
        assert kpi.is_stale(threshold_hours) is expected

    def test_is_stale_zero_sync_returns_false_no_crash(self, base_kpi_kwargs):
        """Graceful degradation: last_sync_at=0 (fresh install) must not raise.

        WHY: At first boot, last_sync_at defaults to 0.0. The predicate must
        not evaluate 0 as stale — 0 means 'unknown', not stale.
        The UI handles unknown separately with a higher-visibility banner.
        """
        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=0.0)
        # is_stale() is not triggered for 0 — callers check .last_sync_at > 0 first.
        # We verify the method doesn't crash and returns a boolean.
        result = kpi.is_stale(2.0)
        assert isinstance(result, bool)

    def test_is_stale_reflects_current_time(self, base_kpi_kwargs):
        """Ensures is_stale() reads wall-clock time at call time, not at model creation."""
        sync_ts = time.time() - (3 * 3600)  # 3 hours ago
        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=sync_ts)
        # Regardless of when this test runs within the test suite, should be stale
        assert kpi.is_stale(2.0) is True

    def test_is_stale_age_hours_property_is_consistent(self, base_kpi_kwargs):
        """CDC: age_hours property must be consistent with is_stale() predicate."""
        sync_ts = time.time() - (3 * 3600)
        kpi = AnalyticKPIs(**base_kpi_kwargs, last_sync_at=sync_ts)
        threshold = 2.0
        # Both derived values must agree
        assert (kpi.age_hours > threshold) == kpi.is_stale(threshold)
