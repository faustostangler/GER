import pytest
from pydantic import ValidationError
from src.domain.schemas import GerconPayloadContract

def test_gercon_contract_should_accept_valid_payload():
    payload_valido = {
        "numeroCMCE": 12345,
        "situacao": "PENDENTE",
        "usuarioSUS": {"nomeCompleto": "MARIA SOUZA"},
        "campo_desconhecido": "Deve ser ignorado pelo model_config='ignore'"
    }
    
    contract = GerconPayloadContract(**payload_valido)
    assert contract.numeroCMCE == 12345
    assert contract.situacao == "PENDENTE"
    assert contract.usuarioSUS is not None
    assert contract.usuarioSUS.nomeCompleto == "MARIA SOUZA"

def test_gercon_contract_should_reject_invalid_schema():
    payload_invalido = {
        "situacao": "PENDENTE",
        # Falta numeroCMCE (obrigatório para identificar o paciente)
    }
    
    # Esse teste de Contrato (CDC) falhará no CI/CD se o Gercon mudar a API silenciosamente.
    with pytest.raises(ValidationError) as exc_info:
        GerconPayloadContract(**payload_invalido)
        
    assert "numeroCMCE" in str(exc_info.value)
