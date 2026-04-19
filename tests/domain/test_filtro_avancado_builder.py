"""RED tests for FiltroAvancadoSpecBuilder and new FiltroAvancadoSpec fields.

WHY: The presentation layer currently wraps raw SQL strings in clauses_legado
and injects them into the domain VO — a leaky abstraction anti-pattern.
This test file defines the EXACT contract that FiltroAvancadoSpecBuilder must
fulfil to eliminate that SQL leak at the UI call site.

New semantic fields under test:
- ``presenca_campos``     → IS NOT NULL / IS NULL for text-presence toggles
- ``booleanos_nullable``  → col = TRUE or (col = FALSE OR col IS NULL)
- ``busca_avancada``      → accent-tolerant, multi-term, optional aggregation

Ref: ADR-004 — SQL Leak refactoring (migration shim removal).
"""
from __future__ import annotations

import pytest

from domain.specifications import (
    AdvancedSearchCriteria,
    FiltroAvancadoSpec,
    FiltroAvancadoSpecBuilder,
)


# ---------------------------------------------------------------------------
# 1. AdvancedSearchCriteria value object
# ---------------------------------------------------------------------------


class TestAdvancedSearchCriteria:
    """Validates the new sub-model for accent-tolerant, boolean-logic text search."""

    def test_creates_with_or_terms(self):
        criteria = AdvancedSearchCriteria(
            column="historico_quadro_clinico",
            or_terms=["urgente", "grave"],
        )
        assert criteria.column == "historico_quadro_clinico"
        assert criteria.or_terms == ["urgente", "grave"]
        assert criteria.and_terms == []
        assert criteria.not_terms == []
        assert criteria.aggregate_by is None

    def test_creates_with_all_term_types(self):
        criteria = AdvancedSearchCriteria(
            column="evolucoes_json",
            or_terms=["alfa"],
            and_terms=["beta"],
            not_terms=["gama"],
            aggregate_by="numeroCMCE",
        )
        assert criteria.aggregate_by == "numeroCMCE"

    def test_is_immutable_pydantic_model(self):
        """WHY: Domain Value Objects must be immutable after creation."""
        from pydantic import ValidationError

        criteria = AdvancedSearchCriteria(
            column="col", or_terms=["x"]
        )
        with pytest.raises((ValidationError, TypeError)):
            criteria.column = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 2. New FiltroAvancadoSpec fields
# ---------------------------------------------------------------------------


class TestFiltroAvancadoSpecNewFields:
    """New fields required to cover all sidebar widget types."""

    def test_presenca_campos_defaults_empty(self):
        """WHY: presenca_campos replaces raw IS NOT NULL clauses from render_presence_radio."""
        spec = FiltroAvancadoSpec()
        assert spec.presenca_campos == {}

    def test_presenca_campos_accepts_true_for_present(self):
        spec = FiltroAvancadoSpec(presenca_campos={"liminarOrdemJudicial": True})
        assert spec.presenca_campos["liminarOrdemJudicial"] is True

    def test_presenca_campos_accepts_false_for_absent(self):
        spec = FiltroAvancadoSpec(presenca_campos={"liminarOrdemJudicial": False})
        assert spec.presenca_campos["liminarOrdemJudicial"] is False

    def test_booleanos_nullable_defaults_empty(self):
        """WHY: booleanos_nullable covers 'No' radio case (col=FALSE OR col IS NULL)."""
        spec = FiltroAvancadoSpec()
        assert spec.booleanos_nullable == {}

    def test_booleanos_nullable_accepts_both_values(self):
        spec = FiltroAvancadoSpec(
            booleanos_nullable={
                "entidade_especialidade_ativa": True,
                "entidade_semClassificacao": False,
            }
        )
        assert spec.booleanos_nullable["entidade_especialidade_ativa"] is True
        assert spec.booleanos_nullable["entidade_semClassificacao"] is False

    def test_busca_avancada_defaults_empty(self):
        """WHY: busca_avancada covers strip_accents ILIKE and bool_or aggregation."""
        spec = FiltroAvancadoSpec()
        assert spec.busca_avancada == []

    def test_busca_avancada_accepts_list_of_criteria(self):
        criteria = AdvancedSearchCriteria(
            column="historico_quadro_clinico",
            or_terms=["urgente"],
            aggregate_by="numeroCMCE",
        )
        spec = FiltroAvancadoSpec(busca_avancada=[criteria])
        assert len(spec.busca_avancada) == 1
        assert spec.busca_avancada[0].column == "historico_quadro_clinico"

    def test_spec_remains_immutable_with_new_fields(self):
        """WHY: frozen=True must hold — no field mutation after creation."""
        from pydantic import ValidationError
        spec = FiltroAvancadoSpec(presenca_campos={"col": True})
        with pytest.raises((ValidationError, TypeError)):
            spec.presenca_campos = {"col": False}  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 3. FiltroAvancadoSpecBuilder — mutable accumulator
# ---------------------------------------------------------------------------


class TestFiltroAvancadoSpecBuilderCreation:
    """Builder must start empty and produce a valid, empty spec."""

    def test_new_builder_is_empty(self):
        builder = FiltroAvancadoSpecBuilder()
        spec = builder.build()
        assert spec == FiltroAvancadoSpec()

    def test_build_returns_filtro_avancado_spec_instance(self):
        spec = FiltroAvancadoSpecBuilder().build()
        assert isinstance(spec, FiltroAvancadoSpec)

    def test_build_is_idempotent(self):
        """WHY: Multiple calls to build() must always return spec equivalent to
        the current accumulated state — no side-effects between calls."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_inclusao("col", ["v1"])
        spec1 = builder.build()
        spec2 = builder.build()
        assert spec1 == spec2


# ---------------------------------------------------------------------------
# 4. Builder: add_inclusao
# ---------------------------------------------------------------------------


class TestBuilderAddInclusao:
    def test_single_inclusao(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_inclusao("origem_lista", ["LISTA_SUS"])
        spec = builder.build()
        assert spec.colunas_inclusao == {"origem_lista": ["LISTA_SUS"]}

    def test_multiple_inclusao_different_columns(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_inclusao("origem_lista", ["A"])
        builder.add_inclusao("situacao", ["AGUARDANDO"])
        spec = builder.build()
        assert "origem_lista" in spec.colunas_inclusao
        assert "situacao" in spec.colunas_inclusao

    def test_add_inclusao_no_op_for_empty_values(self):
        """WHY: Empty selection must not add noise predicate to the spec."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_inclusao("col", [])
        spec = builder.build()
        assert spec.colunas_inclusao == {}


# ---------------------------------------------------------------------------
# 5. Builder: add_exclusao
# ---------------------------------------------------------------------------


class TestBuilderAddExclusao:
    def test_single_exclusao(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_exclusao("entidade_complexidade", ["ALTA"])
        spec = builder.build()
        assert spec.colunas_exclusao == {"entidade_complexidade": ["ALTA"]}

    def test_add_exclusao_no_op_for_empty(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_exclusao("col", [])
        assert builder.build().colunas_exclusao == {}


# ---------------------------------------------------------------------------
# 6. Builder: add_booleano / add_booleano_nullable / add_presenca
# ---------------------------------------------------------------------------


class TestBuilderBooleanAndPresence:
    def test_add_booleano_yes(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_booleano("regularizacaoAcesso", True)
        assert builder.build().booleanos == {"regularizacaoAcesso": True}

    def test_add_booleano_no_uses_nullable(self):
        """WHY: 'No' for boolean radios means FALSE OR NULL — different SQL than FALSE alone."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_booleano_nullable("entidade_semClassificacao", False)
        spec = builder.build()
        assert spec.booleanos_nullable == {"entidade_semClassificacao": False}
        assert spec.booleanos == {}  # must NOT pollute booleanos field

    def test_add_presenca_true(self):
        """WHY: Presence=True → (col IS NOT NULL AND col != '') for render_presence_radio."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_presenca("liminarOrdemJudicial", True)
        assert builder.build().presenca_campos == {"liminarOrdemJudicial": True}

    def test_add_presenca_false(self):
        """WHY: Presence=False → (col IS NULL OR col = '') for render_presence_radio."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_presenca("liminarOrdemJudicial", False)
        assert builder.build().presenca_campos == {"liminarOrdemJudicial": False}


# ---------------------------------------------------------------------------
# 7. Builder: add_limite_numerico / add_limite_data / add_texto
# ---------------------------------------------------------------------------


class TestBuilderNumericDateText:
    def test_add_limite_numerico(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_limite_numerico("entidade_idade_idadeInteiro", 18, 65)
        spec = builder.build()
        assert spec.limites_numericos["entidade_idade_idadeInteiro"] == (18, 65)

    def test_add_limite_data(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_limite_data("dataSolicitacao", "2024-01-01", "2024-12-31")
        spec = builder.build()
        assert spec.limites_data["dataSolicitacao"] == ("2024-01-01", "2024-12-31")

    def test_add_texto(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_texto("entidade_cidPrincipal_descricao", ["cardio"])
        spec = builder.build()
        assert spec.termos_texto["entidade_cidPrincipal_descricao"] == ["cardio"]


# ---------------------------------------------------------------------------
# 8. Builder: add_busca_avancada
# ---------------------------------------------------------------------------


class TestBuilderBuscaAvancada:
    def test_add_busca_avancada_row_search(self):
        """WHY: Non-aggregate search maps to ILIKE per-row (no bool_or)."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_busca_avancada(
            column="entidade_cidPrincipal_descricao",
            or_terms=["cardio", "arritmia"],
            and_terms=[],
            not_terms=["benigno"],
        )
        spec = builder.build()
        assert len(spec.busca_avancada) == 1
        assert spec.busca_avancada[0].or_terms == ["cardio", "arritmia"]
        assert spec.busca_avancada[0].aggregate_by is None

    def test_add_busca_avancada_aggregate_search(self):
        """WHY: Aggregate search maps to bool_or subquery grouped by aggregate_by column."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_busca_avancada(
            column="historico_quadro_clinico",
            or_terms=["urgente"],
            aggregate_by="numeroCMCE",
        )
        spec = builder.build()
        assert spec.busca_avancada[0].aggregate_by == "numeroCMCE"

    def test_add_busca_avancada_no_op_when_all_empty(self):
        """WHY: An empty search (all term lists empty) must not add noise to the spec."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_busca_avancada(column="col", or_terms=[], and_terms=[], not_terms=[])
        spec = builder.build()
        assert spec.busca_avancada == []

    def test_multiple_busca_avancada_accumulate(self):
        builder = FiltroAvancadoSpecBuilder()
        builder.add_busca_avancada("col1", or_terms=["a"])
        builder.add_busca_avancada("col2", and_terms=["b"])
        spec = builder.build()
        assert len(spec.busca_avancada) == 2


# ---------------------------------------------------------------------------
# 9. Builder: full accumulation round-trip
# ---------------------------------------------------------------------------


class TestBuilderFullRoundTrip:
    def test_all_fields_combined(self):
        """Simulates a full sidebar interaction — all widget types contributing."""
        builder = FiltroAvancadoSpecBuilder()
        builder.add_inclusao("origem_lista", ["Fila de Espera"])
        builder.add_exclusao("situacao", ["CANCELADO"])
        builder.add_booleano("entidade_especialidade_ativa", True)
        builder.add_booleano_nullable("entidade_semClassificacao", False)
        builder.add_presenca("liminarOrdemJudicial", True)
        builder.add_limite_numerico("entidade_idade_idadeInteiro", 0, 120)
        builder.add_limite_data("dataSolicitacao", "2024-01-01", "2024-12-31")
        builder.add_busca_avancada("historico_quadro_clinico", or_terms=["urgente"])

        spec = builder.build()

        assert spec.colunas_inclusao["origem_lista"] == ["Fila de Espera"]
        assert spec.colunas_exclusao["situacao"] == ["CANCELADO"]
        assert spec.booleanos["entidade_especialidade_ativa"] is True
        assert spec.booleanos_nullable["entidade_semClassificacao"] is False
        assert spec.presenca_campos["liminarOrdemJudicial"] is True
        assert spec.limites_numericos["entidade_idade_idadeInteiro"] == (0, 120)
        assert spec.limites_data["dataSolicitacao"] == ("2024-01-01", "2024-12-31")
        assert len(spec.busca_avancada) == 1
        # clauses_legado must be empty — no SQL leak from the builder
        assert spec.clauses_legado == ()
