from domain.specifications import (
    PacienteUrgenteSpec,
    PacienteVencidoSpec,
    LeadTimeCriticoSpec,
)
from infrastructure.repositories.duckdb_repository import (
    DuckDBSpecificationTranslator,
)


def test_translate_paciente_urgente():
    spec = PacienteUrgenteSpec(cores_alvo=["VERMELHO", "LARANJA", "AMARELO"])
    result = DuckDBSpecificationTranslator.translate(spec)
    assert (
        result
        == "entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO')"
    )


def test_translate_paciente_vencido():
    spec = PacienteVencidoSpec(dias_tolerancia=180)
    result = DuckDBSpecificationTranslator.translate(spec)
    assert (
        result == "DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180"
    )


def test_translate_lead_time_critico():
    spec = LeadTimeCriticoSpec(max_dias=90)
    result = DuckDBSpecificationTranslator.translate(spec)
    assert result == "DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 90"


def test_translate_composite_and():
    spec = PacienteUrgenteSpec(
        cores_alvo=["VERMELHO", "LARANJA", "AMARELO"]
    ) & PacienteVencidoSpec(dias_tolerancia=180)
    result = DuckDBSpecificationTranslator.translate(spec)
    assert (
        result
        == "(entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO') AND DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180)"
    )


def test_translate_composite_or():
    spec = PacienteUrgenteSpec(
        cores_alvo=["VERMELHO", "LARANJA", "AMARELO"]
    ) | PacienteVencidoSpec(dias_tolerancia=180)
    result = DuckDBSpecificationTranslator.translate(spec)
    assert (
        result
        == "(entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO') OR DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180)"
    )


def test_translate_composite_not():
    spec = ~(PacienteUrgenteSpec(cores_alvo=["VERMELHO", "LARANJA", "AMARELO"]))
    result = DuckDBSpecificationTranslator.translate(spec)
    assert (
        result
        == "NOT (entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO'))"
    )


def test_translate_complex():
    spec = (
        PacienteUrgenteSpec(cores_alvo=["VERMELHO", "LARANJA", "AMARELO"])
        | PacienteVencidoSpec(dias_tolerancia=180)
    ) & LeadTimeCriticoSpec(50)
    result = DuckDBSpecificationTranslator.translate(spec)
    assert (
        result
        == "((entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO') OR DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180) AND DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 50)"
    )


def test_none_spec():
    result = DuckDBSpecificationTranslator.translate(None)
    assert result == "1=1"
