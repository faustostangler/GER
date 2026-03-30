"""
Tests for the Event Sourcing Mapper (solicitacao_mapper.py).
Red-Green-Refactor: Validates snapshot extraction, SLA Engine V2
(State Machine), Funnel Trackers, and schema integrity.
"""
import pytest
from src.domain.solicitacao_mapper import flatten_solicitacao, safe_bool, extract_unidade


def test_safe_bool_handles_all_types():
    assert safe_bool(True) is True
    assert safe_bool(False) is False
    assert safe_bool(None) is False
    assert safe_bool("true") is True
    assert safe_bool("sim") is True
    assert safe_bool("false") is False
    assert safe_bool(1) is True
    assert safe_bool(0) is False


def test_extract_unidade_safe_on_none():
    data = {}
    extract_unidade(None, "test", data)
    assert "test_nome" not in data

    extract_unidade({}, "test", data)
    assert data["test_nome"] == ""


def test_flatten_solicitacao_extracts_root_demographics():
    payload = {
        "numeroCMCE": 123456789012,
        "situacao": "AGENDADA",
        "dataSolicitacao": 1700000000000,
        "usuarioSUS": {
            "nomeCompleto": "JOAO DA SILVA",
            "cpf": "123.456.789-00",
            "sexo": "Masculino",
        },
        "evolucoes": [],
    }

    flat = flatten_solicitacao(payload, "agendadas")

    assert flat["numeroCMCE"] == 123456789012
    assert flat["situacao"] == "AGENDADA"
    assert flat["origem_lista"] == "agendadas"
    from src.domain.solicitacao_mapper import hash_pii
    assert flat["usuarioSUS_nomeCompleto"] == hash_pii("JOAO DA SILVA")
    assert flat["dataSolicitacao"].startswith("14/11/2023")


def test_sla_v2_state_machine_agendada_is_pong_not_desfecho():
    """V2: AGENDADA é PONG (bola com solicitante), NÃO é desfecho.
    O desfecho só ocorre em REALIZADA, CANCELADA, ENCERRADA."""
    payload = {
        "numeroCMCE": 9999,
        "evolucoes": [
            {
                "data": 1700000000000,
                "perfil": "prf-profissional-solicitante",
                "usuario": {"nome": "DR A"},
                "operacaoSolicitacao": "CRIACAO",
                "situacaoAnterior": "",
                "situacaoAtual": "AGUARDA_REGULACAO",
                "detalhes": '{"itensEvolucao": [{"label": "Anamnese", "texto": "Paciente com dor"}], "entidade": {"complexidade": "MEDIA", "cidPrincipal": {"codigo": "R51", "descricao": "CEFALEIA"}}}',
            },
            {
                "data": 1700086400000,  # +1 day
                "perfil": "prf-profissional-regulador-estadual",
                "usuario": {"nome": "REG B"},
                "operacaoSolicitacao": "TROCA_SITUACAO",
                "situacaoAnterior": "AGUARDA_REGULACAO",
                "situacaoAtual": "AGENDADA",
                "detalhes": "{}",
            },
        ],
    }

    flat = flatten_solicitacao(payload, "test")

    # Entidade Snapshot extraído da 1ª evolução
    assert flat["entidade_complexidade"] == "MEDIA"
    assert flat["entidade_cidPrincipal_codigo"] == "R51"
    assert flat["entidade_cidPrincipal_descricao"] == "CEFALEIA"

    # V2: AGENDADA é PONG, NÃO é desfecho → timer continua rodando (active ticker)
    assert flat["SLA_Desfecho_Atingido"] is False
    assert flat["SLA_Tipo_Desfecho"] == ""

    # Funil Trackers
    assert flat["SLA_Marco_Agendada"] is True
    assert flat["SLA_Marco_Realizada"] is False

    # V2: Posse determinada por estado, não perfil
    # AGUARDA_REGULACAO → REGULADOR (1 interação)
    # AGENDADA → SOLICITANTE
    assert flat["SLA_Interacoes_Regulacao"] == 1
    assert flat["SLA_Tempo_Regulador_Dias"] == 1.0  # 1 dia com regulador

    # Dual-Write
    assert "Paciente com dor" in flat["historico_quadro_clinico"]
    assert "CRIACAO" in flat["historico_evolucoes_completo"]

    # Clean Break
    assert "Protocolo" not in flat
    assert "Risco Cor" not in flat


def test_sla_v2_desfecho_positivo_realizada():
    """V2: REALIZADA congela timer com desfecho POSITIVO e marca Funil completo."""
    payload = {
        "numeroCMCE": 5555,
        "evolucoes": [
            {
                "data": 1700000000000,
                "perfil": "prf-profissional-solicitante",
                "usuario": {"nome": "DR A"},
                "operacaoSolicitacao": "CRIACAO",
                "situacaoAnterior": "",
                "situacaoAtual": "AGUARDA_REGULACAO",
                "detalhes": '{"entidade": {"complexidade": "ALTA"}}',
            },
            {
                "data": 1700086400000,  # +1 day
                "perfil": "prf-profissional-regulador",
                "usuario": {"nome": "REG X"},
                "operacaoSolicitacao": "TROCA_SITUACAO",
                "situacaoAnterior": "AGUARDA_REGULACAO",
                "situacaoAtual": "AUTORIZADA",
                "detalhes": "{}",
            },
            {
                "data": 1700172800000,  # +2 days
                "perfil": "sistema",
                "usuario": {"nome": "Rotina automática"},
                "operacaoSolicitacao": "AGENDAMENTO",
                "situacaoAnterior": "AUTORIZADA",
                "situacaoAtual": "AGENDADA",
                "detalhes": "{}",
            },
            {
                "data": 1700259200000,  # +3 days
                "perfil": "sistema",
                "usuario": {"nome": "Rotina automática"},
                "operacaoSolicitacao": "TROCA_SITUACAO",
                "situacaoAnterior": "AGENDADA",
                "situacaoAtual": "REALIZADA",
                "detalhes": "{}",
            },
        ],
    }

    flat = flatten_solicitacao(payload, "test")

    assert flat["SLA_Desfecho_Atingido"] is True
    assert flat["SLA_Tipo_Desfecho"] == "POSITIVO"

    # Funil completo: todos os marcos atingidos
    assert flat["SLA_Marco_Autorizada"] is True
    assert flat["SLA_Marco_Agendada"] is True
    assert flat["SLA_Marco_Realizada"] is True

    # Lead Time: 3 dias exatos
    assert flat["SLA_Lead_Time_Total_Dias"] == 3.0


def test_sla_v2_desfecho_negativo_cancelada():
    """V2: CANCELADA congela timer com desfecho NEGATIVO."""
    payload = {
        "numeroCMCE": 6666,
        "evolucoes": [
            {
                "data": 1700000000000,
                "perfil": "prf-profissional-solicitante",
                "usuario": {"nome": "DR A"},
                "operacaoSolicitacao": "CRIACAO",
                "situacaoAnterior": "",
                "situacaoAtual": "AGUARDA_REGULACAO",
                "detalhes": '{"entidade": {}}',
            },
            {
                "data": 1700172800000,  # +2 days
                "perfil": "prf-profissional-regulador",
                "usuario": {"nome": "REG Y"},
                "operacaoSolicitacao": "TROCA_SITUACAO",
                "situacaoAnterior": "AGUARDA_REGULACAO",
                "situacaoAtual": "CANCELADA",
                "detalhes": "{}",
            },
        ],
    }

    flat = flatten_solicitacao(payload, "test")

    assert flat["SLA_Desfecho_Atingido"] is True
    assert flat["SLA_Tipo_Desfecho"] == "NEGATIVO"
    assert flat["SLA_Lead_Time_Total_Dias"] == 2.0

    # Funil: não atingiu nenhum marco positivo
    assert flat["SLA_Marco_Autorizada"] is False
    assert flat["SLA_Marco_Agendada"] is False
    assert flat["SLA_Marco_Realizada"] is False


def test_sla_v2_desfecho_abandono_encerrada():
    """V2: ENCERRADA congela timer com desfecho ABANDONO."""
    payload = {
        "numeroCMCE": 7777,
        "evolucoes": [
            {
                "data": 1700000000000,
                "perfil": "prf-profissional-solicitante",
                "usuario": {"nome": "DR A"},
                "operacaoSolicitacao": "CRIACAO",
                "situacaoAnterior": "",
                "situacaoAtual": "AGUARDA_REGULACAO",
                "detalhes": '{"entidade": {}}',
            },
            {
                "data": 1700086400000,  # +1 day
                "perfil": "prf-profissional-regulador",
                "usuario": {"nome": "REG Z"},
                "operacaoSolicitacao": "TROCA_SITUACAO",
                "situacaoAnterior": "AGUARDA_REGULACAO",
                "situacaoAtual": "ENCERRADA",
                "detalhes": "{}",
            },
        ],
    }

    flat = flatten_solicitacao(payload, "test")

    assert flat["SLA_Desfecho_Atingido"] is True
    assert flat["SLA_Tipo_Desfecho"] == "ABANDONO"
    assert flat["SLA_Lead_Time_Total_Dias"] == 1.0


def test_sla_v2_state_driven_not_profile_driven():
    """V2: Posse é determinada pelo ESTADO, não pelo perfil.
    Um admin movendo para AGUARDA_REGULACAO deve dar a bola ao REGULADOR."""
    payload = {
        "numeroCMCE": 8888,
        "evolucoes": [
            {
                "data": 1700000000000,
                "perfil": "prf-profissional-solicitante",
                "usuario": {"nome": "DR A"},
                "operacaoSolicitacao": "CRIACAO",
                "situacaoAnterior": "",
                "situacaoAtual": "AGUARDA_REGULACAO",
                "detalhes": '{"entidade": {}}',
            },
            {
                "data": 1700086400000,  # +1 day
                # Why: perfil é admin, MAS o estado é PENDENTE (PONG → Solicitante)
                "perfil": "prf-administrador-sistema",
                "usuario": {"nome": "ADMIN SYS"},
                "operacaoSolicitacao": "TROCA_SITUACAO",
                "situacaoAnterior": "AGUARDA_REGULACAO",
                "situacaoAtual": "PENDENTE",
                "detalhes": "{}",
            },
        ],
    }

    flat = flatten_solicitacao(payload, "test")

    # V2: PENDENTE é PONG → Solicitante tem a bola, apesar do perfil ser admin
    # Na V1, o admin seria classificado como REGULADOR (errado)
    # O 1 dia inteiro foi com REGULADOR (AGUARDA_REGULACAO)
    assert flat["SLA_Tempo_Regulador_Dias"] == 1.0

    # AGUARDA_REGULACAO conta 1 interação de regulação
    assert flat["SLA_Interacoes_Regulacao"] == 1


def test_flatten_solicitacao_handles_none_perfil():
    """Perfil pode ser None no payload do vendor — V2 usa estado, não perfil."""
    payload = {
        "numeroCMCE": 1234,
        "evolucoes": [
            {
                "data": 1700000000000,
                "perfil": None,
                "usuario": {"nome": "Sistema"},
                "operacaoSolicitacao": "CRIACAO",
                "situacaoAnterior": "",
                "situacaoAtual": "AGUARDA_REGULACAO",
                "detalhes": "{}",
            },
        ],
    }

    flat = flatten_solicitacao(payload, "test")
    # V2: AGUARDA_REGULACAO é PING → conta como interação de regulação
    assert flat["SLA_Interacoes_Regulacao"] == 1
