import pandas as pd
from application.use_cases.interfaces import IAnalyticsRepository
from domain.models import AnalyticKPIs
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
        # Mocking filter intersection logic
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


def test_analytics_use_case_should_calculate_correct_kpis():
    stub = AnalyticKPIs(
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
    repo = MockAnalyticsRepository(stub)
    use_case = AnalyticsUseCase(repo)

    # Mock user token
    user = ValidatedUserToken(
        sub="123",
        email="test@test.com",
        preferred_username="test",
        roles=["admin"],
    )

    kpis = use_case.get_executive_summary(None, user)

    assert kpis.pacientes == 500
    assert kpis.lead_time == 10.5
    # Just to confirm config injection was correctly passed to the KPIs
    assert kpis.mes_comercial == 30.416
