import sys
from pathlib import Path

# Força o Pytest a enxergar a pasta src
src_path = str(Path(__file__).resolve().parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

import pytest  # noqa: E402

import numpy  # noqa: E402, F401
import pandas  # noqa: E402, F401
import os

# --- SRE: CRITICAL ENVIRONMENT INITIALIZATION ---
# WHY: These must be set BEFORE any infrastructure modules are imported during
# pytest collection. Moving them here ensures that src.infrastructure.database.session
# correctly identifies the testing environment even when imported at the top level
# of a test file.
os.environ["APP__ENVIRONMENT"] = "testing"
os.environ["ENVIRONMENT"] = "testing"
os.environ["OUTPUT_FILE"] = "test_gercon_consolidado.parquet"


@pytest.fixture(autouse=True)
def prevent_streamlit_module_purge(monkeypatch):
    """
    Impede que o Streamlit expurgue pacotes do .venv da memória
    durante a execução dos testes headless.
    """
    import streamlit.source_util as source_util

    # Desativa silenciosamente o watcher de arquivos locais para testes
    monkeypatch.setattr(source_util, "_cached_pages", None, raising=False)


# --- SRE DATA SEEDING ISOLADO EM DIRETÓRIO TEMPORÁRIO ---
@pytest.fixture(scope="session")
def test_parquet_path(tmp_path_factory):
    """Gera um arquivo Parquet válido em diretório temporário isolado.

    WHY: Escrever no CWD (gercon_consolidado.parquet) polui os dados de produção
    quando o container monta /app via bind-mount. O tmp_path_factory garante
    isolamento total: o artefato é criado em /tmp/pytest-xxx/ e destruído
    automaticamente pelo pytest ao finalizar a sessão.

    O DuckDBAnalyticsRepository e o app devem ser instrumentados via monkeypatch
    de settings.OUTPUT_FILE para apontar ao caminho retornado por esta fixture.
    """
    import pandas as pd

    tmp_dir = tmp_path_factory.mktemp("data")
    parquet_file = tmp_dir / "gercon_consolidado.parquet"

    df = pd.DataFrame(
        {
            # Chaves Identificadoras e de Triagem
            "numeroCMCE": ["E2E-001", "E2E-002"],
            "dataSolicitacao": [
                "2026-04-01T10:00:00Z",
                "2026-04-02T10:00:00Z",
            ],
            "dataCadastro": [
                "2026-04-01T09:00:00Z",
                "2026-04-02T09:00:00Z",
            ],
            # WHY: 'situacao' é a coluna raw usada diretamente em queries SQL (SELECT situacao ...)
            # 'entidade_situacao_descricao' é usada em outros contextos de renaming.
            # Ambas são necessárias no seed para não quebrar nenhum caminho de query.
            "situacao": ["PENDENTE", "AGENDADA"],
            "entidade_situacao_descricao": ["PENDENTE", "AGENDADA"],
            "entidade_classificacaoRisco_cor": ["VERMELHO", "AMARELO"],
            "entidade_classificacaoRisco_totalPontos": [50, 120],
            # Demografia e Rede
            "usuarioSUS_municipioResidencia_nome": ["Porto Alegre", "Canoas"],
            "usuarioSUS_bairro": ["Centro", "Mathias Velho"],
            "usuarioSUS_dataNascimento": ["1980-01-01", "1965-05-15"],
            "usuarioSUS_sexo": ["Masculino", "Feminino"],
            "entidade_idade_idadeInteiro": [45, 60],
            # Especialidades e Médicos
            "entidade_especialidade_descricao": ["Cardiologia", "Ortopedia"],
            "entidade_especialidade_especialidadeMae_descricao": [
                "Clínica Médica",
                "Cirurgia",
            ],
            "entidade_especialidade_cbo_descricao": ["CBO-001", "CBO-002"],
            "medicoSolicitante": ["Dr. João SRE", "Dra. Maria DevOps"],
            "unidade_solicitante": ["UBS Centro", "UBS Norte"],
            # WHY: Valores semânticos do domínio clínico — não "Fila A/B".
            # Devem refletir os valores reais de origem_lista registrados no Gercon.
            "origem_lista": ["LISTA_ESPERA", "LISTA_SUS"],
            "entidade_centralRegulacao_nome": ["Central A", "Central B"],
            # Diagnóstico e Auditoria
            "entidade_cidPrincipal_descricao": ["Hipertensão", "Fratura"],
            "entidade_cidPrincipal_codigo": ["I10", "S72"],
            "paciente_nome": ["John Doe", "Jane Doe"],
            "cpf": ["111.111.111-11", "222.222.222-22"],
            "historico_quadro_clinico": [
                "Paciente relata dor",
                "Evolução estável",
            ],
            # SLAs
            "SLA_Lead_Time_Total_Dias": [10.0, 25.0],
        }
    )

    df.to_parquet(str(parquet_file))
    return str(parquet_file)


@pytest.fixture(scope="session", autouse=True)
def seed_test_database(test_parquet_path, monkeypatch_session):
    """Aponta settings.OUTPUT_FILE para o parquet temporário de testes.

    WHY: Garante que o DuckDBAnalyticsRepository consuma o parquet mock isolado
    durante todos os testes, sem jamais tocar no arquivo de produção em /app/.
    """
    monkeypatch_session.setenv("OUTPUT_FILE", test_parquet_path)
    monkeypatch_session.setenv("APP__ENVIRONMENT", "testing")

    # Patch direto no objeto settings já instanciado (necessário pois o settings
    # é um singleton criado no import de infrastructure.config)
    from infrastructure import config as cfg

    monkeypatch_session.setattr(cfg.settings, "OUTPUT_FILE", test_parquet_path)
    monkeypatch_session.setattr(cfg.settings, "ENVIRONMENT", "testing")
    yield


@pytest.fixture(scope="session")
def monkeypatch_session():
    """Monkeypatch com escopo de sessão (não existe nativamente no pytest).

    WHY: O monkeypatch padrão é function-scoped. Para patching de session-scoped
    fixtures (como settings), precisamos de um monkeypatch de sessão manual.
    """
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    yield mp
    mp.undo()
