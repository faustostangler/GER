"""Tests for DuckDBCriteriaTranslator — the infrastructure adapter that converts
FiltroAvancadoSpec (domain semantic) into DuckDB SQL strings (infrastructure dialect).

WHY: The translator is the ACL (Anti-Corruption Layer) between domain vocabulary and
     SQL infrastructure. These tests validate the "how" translation is correct,
     while domain tests for FiltroAvancadoSpec validate the "what".

Related: SQL Leak refactoring from FilterCriteria → FiltroAvancadoSpec + DuckDBCriteriaTranslator.
"""
import pytest
from domain.specifications import (
    FiltroAvancadoSpec,
    PacienteUrgenteSpec,
    PacienteVencidoSpec,
    LeadTimeCriticoSpec,
)
from infrastructure.repositories.criteria_translator import DuckDBCriteriaTranslator


class TestTranslateFiltroAvancadoSpec:
    """Validates that semantic domain fields map to correct SQL predicates."""

    def test_empty_filtro_returns_universe_clause(self):
        """An empty FiltroAvancadoSpec must produce '1=1' (no-op WHERE clause)."""
        spec = FiltroAvancadoSpec()
        assert DuckDBCriteriaTranslator.translate(spec) == "1=1"

    def test_inclusao_single_value(self):
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"origem_lista": ["LISTA_SUS"]}
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        assert '"origem_lista" IN (\'LISTA_SUS\')' in result

    def test_inclusao_multiple_values_joined_with_in(self):
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"entidade_especialidade_descricao": ["CARDIOLOGIA", "ORTOPEDIA"]}
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        assert '"entidade_especialidade_descricao" IN' in result
        assert "'CARDIOLOGIA'" in result
        assert "'ORTOPEDIA'" in result

    def test_exclusao_single_value(self):
        spec = FiltroAvancadoSpec(
            colunas_exclusao={"entidade_complexidade": ["ALTA"]}
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        assert '"entidade_complexidade" NOT IN (\'ALTA\')' in result

    def test_inclusao_and_exclusao_combined_with_and(self):
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"origem_lista": ["LISTA_SUS"]},
            colunas_exclusao={"entidade_complexidade": ["ALTA"]},
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        assert "AND" in result
        assert '"origem_lista"' in result
        assert '"entidade_complexidade"' in result

    def test_booleano_true(self):
        spec = FiltroAvancadoSpec(booleanos={"SLA_Marco_Autorizada": True})
        result = DuckDBCriteriaTranslator.translate(spec)
        assert '"SLA_Marco_Autorizada" = TRUE' in result

    def test_booleano_false(self):
        spec = FiltroAvancadoSpec(booleanos={"SLA_Marco_Autorizada": False})
        result = DuckDBCriteriaTranslator.translate(spec)
        assert '"SLA_Marco_Autorizada" = FALSE' in result

    def test_limite_numerico_range(self):
        spec = FiltroAvancadoSpec(
            limites_numericos={"SLA_Lead_Time_Total_Dias": (10, 90)}
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        assert '"SLA_Lead_Time_Total_Dias" BETWEEN 10 AND 90' in result

    def test_limite_data_range(self):
        spec = FiltroAvancadoSpec(
            limites_data={"dataSolicitacao": ("2023-01-01", "2024-12-31")}
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        assert "CAST(dataSolicitacao AS DATE) BETWEEN" in result
        assert "'2023-01-01'" in result
        assert "'2024-12-31'" in result

    def test_texto_ilike_clause(self):
        """Text search must use case-insensitive ILIKE in DuckDB."""
        spec = FiltroAvancadoSpec(
            termos_texto={"justificativaRetorno": ["urgente"]}
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        assert "ILIKE" in result
        assert "%urgente%" in result

    def test_multiple_fields_all_combined_with_and(self):
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"origem_lista": ["LISTA_SUS"]},
            booleanos={"SLA_Marco_Autorizada": True},
            limites_numericos={"SLA_Interacoes_Regulacao": (1, 5)},
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        # Must contain all three predicates joined by AND
        parts = result.split(" AND ")
        assert len(parts) >= 3

    def test_sql_injection_escaping_in_values(self):
        """Single quotes in values must be escaped to prevent SQL injection."""
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"motivoCancelamento": ["O'Brien"]}
        )
        result = DuckDBCriteriaTranslator.translate(spec)
        # The apostrophe must be escaped (doubled) in SQL
        assert "O''Brien" in result

    def test_none_spec_returns_universe(self):
        assert DuckDBCriteriaTranslator.translate(None) == "1=1"


class TestTranslateSpecificationComposites:
    """Validates backward-compatible translation of all Specification subtypes."""

    def test_translate_paciente_urgente(self):
        spec = PacienteUrgenteSpec(cores_alvo=["VERMELHO", "LARANJA", "AMARELO"])
        result = DuckDBCriteriaTranslator.translate(spec)
        assert result == "entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO')"

    def test_translate_paciente_vencido(self):
        spec = PacienteVencidoSpec(dias_tolerancia=180)
        result = DuckDBCriteriaTranslator.translate(spec)
        assert result == "DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180"

    def test_translate_lead_time_critico(self):
        spec = LeadTimeCriticoSpec(max_dias=90)
        result = DuckDBCriteriaTranslator.translate(spec)
        assert result == "DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 90"

    def test_translate_composite_and(self):
        spec = PacienteUrgenteSpec(
            cores_alvo=["VERMELHO", "LARANJA", "AMARELO"]
        ) & PacienteVencidoSpec(dias_tolerancia=180)
        result = DuckDBCriteriaTranslator.translate(spec)
        assert result == (
            "(entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO')"
            " AND DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180)"
        )

    def test_translate_composite_or(self):
        spec = PacienteUrgenteSpec(
            cores_alvo=["VERMELHO", "LARANJA", "AMARELO"]
        ) | PacienteVencidoSpec(dias_tolerancia=180)
        result = DuckDBCriteriaTranslator.translate(spec)
        assert result == (
            "(entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO')"
            " OR DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) > 180)"
        )

    def test_translate_composite_not(self):
        spec = ~(PacienteUrgenteSpec(cores_alvo=["VERMELHO", "LARANJA", "AMARELO"]))
        result = DuckDBCriteriaTranslator.translate(spec)
        assert result == "NOT (entidade_classificacaoRisco_cor IN ('VERMELHO', 'LARANJA', 'AMARELO'))"

    def test_translate_none(self):
        assert DuckDBCriteriaTranslator.translate(None) == "1=1"

    def test_translate_unknown_spec_returns_universe(self):
        from domain.specifications import Specification

        class UnknownSpec(Specification):
            def is_satisfied_by(self, candidate) -> bool:
                return True

        result = DuckDBCriteriaTranslator.translate(UnknownSpec())
        assert result == "1=1"
