import pytest
from datetime import datetime
from src.application.use_cases.interfaces import IAnalyticsRepository
from src.domain.models import AnalyticKPIs, FilterCriteria
from src.application.use_cases.analytics_use_case import AnalyticsUseCase

class MockAnalyticsRepository(IAnalyticsRepository):
    """Repositório In-Memory para Prova Matemática de Isolamento do BFF (Zero UI/I.O DB)."""
    def __init__(self, stub_kpis: AnalyticKPIs):
        self._stub = stub_kpis

    def get_kpis(self, filters: FilterCriteria) -> AnalyticKPIs:
        # Simulando uma filtragem boba apenas para provar que a chamada passa por aqui
        if filters.risk_color == "Vermelho" and self._stub.total_rows > 0:
            # Assumimos que no stub todos são vermelhos, então retornamos tudo, ou simulamos redução.
            return self._stub
        elif filters.risk_color == "Verde":
            return AnalyticKPIs(total_rows=0, p90_lead_time_days=0.0)
        return self._stub

    def get_sunburst_data(self, filters: FilterCriteria):
        return []

    def get_risk_boxplot_data(self, filters: FilterCriteria):
        return []

    def get_lead_time_history(self, filters: FilterCriteria):
        return []

def test_analytics_use_case_should_calculate_correct_kpis():
    stub = AnalyticKPIs(total_rows=500, p90_lead_time_days=15.5)
    repo = MockAnalyticsRepository(stub)
    
    use_case = AnalyticsUseCase(repo)
    
    # Executando Use Case através da fronteira arquitetural (Hexagonal Port)
    filters = FilterCriteria(risk_color="Vermelho")
    kpis = use_case.fetch_kpis(filters)
    
    # O Streamlit (UI) nunca é invocado. Teste de pura Lógica de Aplicação.
    assert kpis.total_rows == 500
    assert kpis.p90_lead_time_days == 15.5

def test_analytics_use_case_should_respect_filter_propagation():
    stub = AnalyticKPIs(total_rows=500, p90_lead_time_days=15.5)
    repo = MockAnalyticsRepository(stub)
    use_case = AnalyticsUseCase(repo)
    
    filters = FilterCriteria(risk_color="Verde")
    kpis = use_case.fetch_kpis(filters)
    
    assert kpis.total_rows == 0
    assert kpis.p90_lead_time_days == 0.0
