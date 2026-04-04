import sys
from pathlib import Path

# Força o Pytest a enxergar a pasta src
src_path = str(Path(__file__).resolve().parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import pytest  # noqa: E402

# SRE FIX: Força o carregamento de C-Extensions no Main Interpreter 
# antes que o Streamlit AppTest ou pytest-cov alterem os import hooks.
import numpy  # noqa: E402, F401
import pandas  # noqa: E402, F401

@pytest.fixture(autouse=True)
def prevent_streamlit_module_purge(monkeypatch):
    """
    Impede que o Streamlit expurgue pacotes do .venv da memória
    durante a execução dos testes headless.
    """
    import streamlit.source_util as source_util
    # Desativa silenciosamente o watcher de arquivos locais para testes
    monkeypatch.setattr(source_util, "_cached_pages", None, raising=False)

# --- SRE DATA SEEDING ELEVADO PARA ESCOPO GLOBAL ---
@pytest.fixture(scope="session", autouse=True)
def seed_test_database():
    import os
    import pandas as pd
    """Gera um arquivo Parquet válido para o Streamlit consumir em todos os testes de UI/E2E."""
    test_file = "gercon_consolidado.parquet"
    
    df = pd.DataFrame({
        # Chaves Identificadoras e de Triagem
        "numeroCMCE": ["E2E-001", "E2E-002"],
        "dataSolicitacao": ["2026-04-01T10:00:00Z", "2026-04-02T10:00:00Z"],
        "dataCadastro": ["2026-04-01T09:00:00Z", "2026-04-02T09:00:00Z"],
        "entidade_situacao_descricao": ["PENDENTE", "AGENDADA"],
        "entidade_classificacaoRisco_cor": ["VERMELHO", "AMARELO"],
        "entidade_classificacaoRisco_totalPontos": [50, 120], # Falta no scatter
        
        # Demografia e Rede
        "usuarioSUS_municipioResidencia_nome": ["Porto Alegre", "Canoas"],
        "usuarioSUS_bairro": ["Centro", "Mathias Velho"],
        "usuarioSUS_dataNascimento": ["1980-01-01", "1965-05-15"], # Necessário para date_diff
        "usuarioSUS_sexo": ["Masculino", "Feminino"],
        "entidade_idade_idadeInteiro": [45, 60],
        
        # Especialidades e Médicos
        "entidade_especialidade_descricao": ["Cardiologia", "Ortopedia"],
        "entidade_especialidade_especialidadeMae_descricao": ["Clínica Médica", "Cirurgia"],
        "medicoSolicitante": ["Dr. João SRE", "Dra. Maria DevOps"],
        "unidade_solicitante": ["UBS Centro", "UBS Norte"],
        "origem_lista": ["Fila A", "Fila B"],
        
        # Diagnóstico e Auditoria
        "entidade_cidPrincipal_descricao": ["Hipertensão", "Fratura"],
        "paciente_nome": ["John Doe", "Jane Doe"],
        "cpf": ["111.111.111-11", "222.222.222-22"],
        "historico_quadro_clinico": ["Paciente relata dor", "Evolução estável"], # Necessário para Tabela
        
        # SLAs
        "SLA_Lead_Time_Total_Dias": [10.0, 25.0]
    })
    
    df.to_parquet(test_file)
    
    yield # Executa todos os testes da suíte
    
    if os.path.exists(test_file):
        os.remove(test_file)
