import pytest
from src.domain.solicitacao_mapper import flatten_solicitacao, format_protocolo

# Mock puro sem infraestrutura, usando o padrão Red-Green-Refactor
def test_format_protocolo():
    assert format_protocolo(123456789012) == "12-34-5678901-2"
    assert format_protocolo(12345) == "12345"

def test_flatten_solicitacao_should_extract_data_correctly():
    payload = {
        "numeroCMCE": 123456789012,
        "situacao": "AGENDADA",
        "dataSolicitacao": 1700000000000,
        "usuarioSUS": {
            "nomeCompleto": "JOAO DA SILVA",
            "cpf": "123.456.789-00"
        },
        "evolucoes": []
    }
    
    flat_data = flatten_solicitacao(payload, "agendadas")
    
    assert flat_data["Protocolo"] == "12-34-5678901-2"
    assert flat_data["Situação"] == "AGENDADA"
    assert flat_data["Origem da Lista"] == "agendadas"
    assert flat_data["Nome do Paciente"] == "JOAO DA SILVA"
    assert flat_data["Data Solicitação"] == "14/11/2023 18:13:20"

def test_flatten_solicitacao_should_sort_historico_cronologicamente():
    # Simula desordem no JSON
    payload = {
        "numeroCMCE": 9999,
        "evolucoes": [
            {
                "data": 1700001000000, # Mais novo
                "usuario": {"nome": "DR B"},
                "detalhes": '{"itensEvolucao": [{"texto": "Avaliação 2"}]}'
            },
            {
                "data": 1700000000000, # Mais velho
                "usuario": {"nome": "DR A"},
                "detalhes": '{"itensEvolucao": [{"texto": "Avaliação 1"}]}'
            }
        ]
    }
    
    flat_data = flatten_solicitacao(payload, "test")
    
    # Validações matemáticas do Domínio:
    # 1. Médico Solicitante é sempre o primeiro a ter registrado evolução.
    assert flat_data["Médico Solicitante"] == "DR A"
    
    # 2. Histórico concatenado deve respeitar a cronologia.
    assert "Avaliação 1" in flat_data["Histórico Quadro Clínico"]
    assert "Avaliação 2" in flat_data["Histórico Quadro Clínico"]
    
    # Garante que Avaliação 1 vem ANTES da Avaliação 2 na string final.
    idx_a1 = flat_data["Histórico Quadro Clínico"].find("Avaliação 1")
    idx_a2 = flat_data["Histórico Quadro Clínico"].find("Avaliação 2")
    assert idx_a1 < idx_a2
