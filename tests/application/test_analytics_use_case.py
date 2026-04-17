"""Testes do AnalyticsUseCase com ClinicaPolicy injetada via DI.

WHY: Após o ADR-005 (Business Policy Extraction), o AnalyticsUseCase recebe
ClinicaPolicy por injeção de dependência — sem acoplamento direto a settings.
Estes testes garantem que o contrato de DI é honrado e que não há regressão
no comportamento central do Use Case.
"""
import ast
import pathlib
import pandas as pd
import pytest
from application.use_cases.interfaces import IAnalyticsRepository
from domain.models import AnalyticKPIs
from domain.policies import ClinicaPolicy, DEFAULT_CLINICA_POLICY
from domain.specifications import Specification
from application.use_cases.analytics_use_case import AnalyticsUseCase
from infrastructure.auth.token_acl import ValidatedUserToken


class MockAnalyticsRepository(IAnalyticsRepository):
    def __init__(self, stub_kpis: AnalyticKPIs):
        self._stub = stub_kpis

    def get_kpis(
        self,
        spec: Specification,
        spec_urgentes: Specification,
        spec_vencidos: Specification,
        user: ValidatedUserToken,
    ) -> AnalyticKPIs:
        return self._stub

    def get_distribution_data(
        self, spec: Specification, user: ValidatedUserToken
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def get_dynamic_options(
        self, column: str, current_where: str, user: ValidatedUserToken
    ):
        return []

    def get_global_bounds(
        self, column: str, is_date: bool = False, user: ValidatedUserToken = None
    ):
        return (None, None)

    def execute_custom_query(self, sql: str, user: ValidatedUserToken) -> pd.DataFrame:
        return pd.DataFrame()


def _make_stub_kpis() -> AnalyticKPIs:
    return AnalyticKPIs(
        pacientes=500,
        eventos=1000,
        esp_mae=10,
        sub_esp=20,
        medicos=50,
        cids=30,
        origens=5,
        lead_time=10.5,
        max_lead_time=30,
        span_dias=90,
        pac_urgentes=100,
        pac_vencidos=50,
        p90_lead_time=25.0,
        p90_esquecido=15.0,
    )


def _make_user() -> ValidatedUserToken:
    return ValidatedUserToken(
        sub="123",
        email="test@test.com",
        preferred_username="test",
        roles=["admin"],
    )


def test_analytics_use_case_should_calculate_correct_kpis():
    repo = MockAnalyticsRepository(_make_stub_kpis())
    use_case = AnalyticsUseCase(repo)

    kpis = use_case.get_executive_summary(None, _make_user())

    assert kpis.pacientes == 500
    assert kpis.lead_time == 10.5
    assert kpis.mes_comercial == pytest.approx(30.416)


class TestAnalyticsUseCasePolicyInjection:
    """Verifica que ClinicaPolicy é injetada corretamente e não há coupling a settings."""

    def test_use_case_usa_default_policy_quando_nao_injetada(self):
        """WHY: DI com default seguro — sem settings como dependência implícita."""
        use_case = AnalyticsUseCase(MockAnalyticsRepository(_make_stub_kpis()))
        assert use_case._policy == DEFAULT_CLINICA_POLICY

    def test_use_case_aceita_custom_policy_via_di(self):
        """Permite override de política para multi-tenant ou testes customizados."""
        custom = ClinicaPolicy(sla_dias_vencimento=90, cores_urgencia=["VERMELHO"])
        use_case = AnalyticsUseCase(
            MockAnalyticsRepository(_make_stub_kpis()), policy=custom
        )

        assert use_case._policy.sla_dias_vencimento == 90
        assert "VERMELHO" in use_case._policy.cores_urgencia

    def test_mes_comercial_vem_da_policy_injetada(self):
        """mes_comercial nos KPIs deve refletir a ClinicaPolicy, não settings."""
        custom = ClinicaPolicy(mes_comercial_dias=21.0)  # quinzena comercial fictícia
        use_case = AnalyticsUseCase(
            MockAnalyticsRepository(_make_stub_kpis()), policy=custom
        )

        kpis = use_case.get_executive_summary(None, _make_user())
        assert kpis.mes_comercial == pytest.approx(21.0)

    def test_use_case_nao_importa_settings_diretamente(self):
        """CRÍTICO: Use Case não deve acoplar com infrastructure.config.

        WHY: A Clean Architecture proíbe que a camada Application dependa de
        Infrastructure. A ClinicaPolicy resolve isso via DI. Ref: ADR-005.
        """
        source = (
            pathlib.Path(__file__).parent.parent.parent
            / "src/application/use_cases/analytics_use_case.py"
        )
        tree = ast.parse(source.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                qualified = node.module or ""
                assert "infrastructure.config" not in qualified, (
                    "AnalyticsUseCase importa infrastructure.config diretamente! "
                    "Use ClinicaPolicy via DI. Ref: ADR-005."
                )
