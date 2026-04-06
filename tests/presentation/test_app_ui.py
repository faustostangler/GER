import pytest
import os
from unittest.mock import patch, MagicMock
from pathlib import Path
from streamlit.testing.v1 import AppTest
from domain.models import AnalyticKPIs
from infrastructure.config import settings

# SRE FIX: Localiza a raiz do projeto absoltamente (2 níveis acima de tests/presentation/)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
APP_PATH = str(BASE_DIR / "app_analytics.py")

# Salvar referência para não quebrar módulos internos do python
original_exists = os.path.exists

def mock_exists_side_effect(path):
    # Se for a base duckdb que o analytics exige
    if str(path) == str(settings.OUTPUT_FILE):
        return True
    return original_exists(path)

@pytest.fixture
def mock_analytics_use_case():
    # Patchamos a classe da arquitetura limpa SEM src. prefix
    with patch("application.use_cases.analytics_use_case.AnalyticsUseCase") as mock_use_case_class:
        instance = MagicMock()
        instance.get_global_bounds.return_value = (1, 100)
        instance.get_dynamic_options.return_value = ["Opção A", "Opção B"]
        
        mock_kpis = AnalyticKPIs(
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

        def mock_execute_custom_query(query, spec, current_user, **kwargs):
            import pandas as pd
            # SRE: query is available if needed for complex mocks
            
            # Base fat dataframe
            df_base = {
                "Categoria": ["A", "B"],
                "Vol": [10, 20], 
                "entidade_classificacaoRisco_cor": ["VERMELHO", "VERDE"],
                "Etapa": ["1. Solicitado", "2. Triado"],
                "entidade_situacao_descricao": ["AGENDADA", "ATENDIDO"],
                "origem_descricao": ["Origem A", "Origem B"],
                "entidade_especialidade_descricao": ["Cardio", "Orto"],
                "motivo_descricao": ["Motivo A", "Motivo B"],
                "CID": ["A01", "A02"],
                "Janela": ["2026-01-01", "2026-01-02"],
                "entidade_especialidade_especialidadeMae_descricao": ["Clinica", "Cirurgia"],
                "entidade_cidPrincipal_descricao": ["A00", "A01"],
                "num_dias_aguardando": [1, 5],
                "lead_time": [1.0, 5.0],
                "Metrica_Cor": [1, 2],
                "usuarioSUS_bairro": ["Centro", "Bairro"],
                "usuarioSUS_municipioResidencia_nome": ["SPOA", "Canoas"],
                "entidade_idade_idadeInteiro": [45, 60],
                "usuarioSUS_sexo_descricao": ["M", "F"],
                "solicitante_profissional_nome_medico": ["Dr. A", "Dra. B"],
                "numero_dias_desde_criacao": [10, 20],
                "ano_mes": ["2026-01", "2026-02"],
                "entidade_cidPrincipal_codigo": ["A00", "B01"],
                "medico_executante": ["Dr. A", "Dra. B"],
                "Idade_Int": [45, 60],
                "usuarioSUS_sexo": ["M", "F"],
                "Dia": ["2026-01-01", "2026-01-02"],
                "origem_lista": ["A", "B"],
                "solicitante_profissional_nome": ["Dr. X", "Dr. Y"],
                "CIDs_Distintos": [1, 2],
                "Volume": [10, 20],
                "medicoSolicitante": ["Dr. M", "Dra. N"],
                "CID_Curto": ["A00.0", "A01.1"],
                "DiasFila": [10, 20],
                "Origem": ["A", "B"],
                "Ofensor": ["X", "Y"],
                "Pontos": [1, 2],
                "numeroCMCE": [1001, 1002]
            }
            return pd.DataFrame(df_base)
            
        instance.execute_custom_query.side_effect = mock_execute_custom_query
        instance.get_executive_summary.return_value = mock_kpis
        mock_use_case_class.return_value = instance
        yield instance

@pytest.fixture
def mock_duckdb_repo():
    with patch("infrastructure.repositories.duckdb_repository.DuckDBAnalyticsRepository") as mock_db:
        yield mock_db

@patch("os.path.exists", side_effect=mock_exists_side_effect)
def test_app_ui_loads_and_updates_state(mock_exists, mock_duckdb_repo, mock_analytics_use_case):
    # 1. Instanciando a Simulação Streamlit (Humble Object)
    # Bypass do IAP Proxy pelo fluxo de "dev"
    with patch.dict("os.environ", {"ENVIRONMENT": "dev", "ALLOW_UNAUTHENTICATED_DEV": "true"}):
        # SRE FIX: Use path absoluto para evitar quebra no sandbox do mutmut
        at = AppTest.from_file(APP_PATH).run(timeout=10)
    
        if at.exception:
            pytest.fail(f"Streamlit AppTest Exception: {at.exception}")

        # 2. Verificar se a tela inicial e o Título carregaram corretamente (invariantes visuais)
        assert len(at.title) > 0, "Nenhum título foi renderizado"
        assert "🎯 Gercon SRE | Advanced Root Cause Analysis" in at.title[0].value
        
        # 3. Simular interação com a aplicação e verificar mutação do estado de Tracker SRE
        if len(at.toggle) > 0:
            first_toggle = at.toggle[0]
            first_toggle.set_value(True).run()
            assert not at.exception

        if len(at.button) > 0:
            clear_all_button = None
            for idx, b in enumerate(at.button):
                if "Limpar Todos os Filtros" in getattr(b, "label", getattr(b, "value", "")):
                    clear_all_button = at.button[idx]
                    break
            
            if clear_all_button:
                clear_all_button.click().run()
                assert not at.exception
