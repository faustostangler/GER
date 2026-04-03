import pytest
from src.domain.models import AnalyticKPIs
from src.application.use_cases.analytics_use_case import AnalyticsUseCase
from pydantic import ValidationError

def test_cdc_streamlit_requires_specific_kpis_fields():
    """
    Consumer-Driven Contract (CDC) - Presentation Layer (Streamlit)
    
    O Streamlit exige que o objeto retornado por AnalyticsUseCase.get_executive_summary()
    possua obrigatoriamente as propriedades:
    - pacientes
    - lead_time
    - p90_lead_time
    
    Se alguém (provedor) alterar a classe AnalyticKPIs ou seus nomes no backend
    e remover / renomear essas propriedades, os componentes de sumário executivo no Streamlit falharão.
    Este teste atuará como uma barreira quebrando antes do deploy caso o contrato seja violado.
    """
    # Verify that the Pydantic model itself has these fields declared
    fields = AnalyticKPIs.model_fields.keys()
    
    assert "pacientes" in fields, "CDC Quebrado: Streamlit necessita de AnalyticKPIs.pacientes para renderizar o card de volume total."
    assert "lead_time" in fields, "CDC Quebrado: Streamlit necessita de AnalyticKPIs.lead_time para compor o SLA."
    assert "p90_lead_time" in fields, "CDC Quebrado: Streamlit necessita de AnalyticKPIs.p90_lead_time para a métrica estatística de outliers."
    
    # Adicionalmente, verificamos que eles podem ser acessados via getattr
    try:
        mock_instance = AnalyticKPIs(
            pacientes=100,
            eventos=500,
            esp_mae=10,
            sub_esp=20,
            medicos=30,
            cids=40,
            origens=5,
            lead_time=15.5,
            max_lead_time=100,
            span_dias=30,
            pac_urgentes=10,
            pac_vencidos=5,
            p90_lead_time=45.2,
            p90_esquecido=60.1
        )
    except ValidationError as e:
        pytest.fail(f"O contrato estrutural inicial foi violado ao tentar instanciar o objeto com as chaves obrigatórias: {e}")
        
    assert hasattr(mock_instance, "pacientes")
    assert hasattr(mock_instance, "lead_time")
    assert hasattr(mock_instance, "p90_lead_time")
