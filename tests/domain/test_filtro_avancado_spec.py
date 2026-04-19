"""Tests for FiltroAvancadoSpec — pure semantic domain Value Object.

WHY: FilterCriteria was corrupted by SQL infrastructure concerns (list[str] clauses).
     FiltroAvancadoSpec replaces it as a clean domain object that knows WHAT to filter
     (via named semantic fields), not HOW the storage layer executes it.

Related: Issue — SQL Leak from Infrastructure into Core Domain (ADR-004 candidate).
"""
from domain.specifications import FiltroAvancadoSpec


class TestFiltroAvancadoSpecCreation:
    """Value Object invariants: immutability and semantic fields."""

    def test_creates_with_all_defaults(self):
        """An empty FiltroAvancadoSpec represents 'no filter' — universe clause."""
        spec = FiltroAvancadoSpec()
        assert spec.colunas_inclusao == {}
        assert spec.colunas_exclusao == {}
        assert spec.termos_texto == {}
        assert spec.limites_numericos == {}
        assert spec.limites_data == {}
        assert spec.booleanos == {}

    def test_creates_with_inclusao(self):
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"entidade_especialidade_descricao": ["CARDIOLOGIA"]}
        )
        assert spec.colunas_inclusao == {"entidade_especialidade_descricao": ["CARDIOLOGIA"]}

    def test_creates_with_exclusao(self):
        spec = FiltroAvancadoSpec(
            colunas_exclusao={"entidade_complexidade": ["ALTA"]}
        )
        assert spec.colunas_exclusao == {"entidade_complexidade": ["ALTA"]}

    def test_creates_with_texto_entry(self):
        spec = FiltroAvancadoSpec(
            termos_texto={"justificativaRetorno": ["urgente"]}
        )
        assert spec.termos_texto == {"justificativaRetorno": ["urgente"]}

    def test_creates_with_limite_numerico(self):
        spec = FiltroAvancadoSpec(
            limites_numericos={"SLA_Lead_Time_Total_Dias": (10, 90)}
        )
        assert spec.limites_numericos["SLA_Lead_Time_Total_Dias"] == (10, 90)

    def test_creates_with_limite_data(self):
        spec = FiltroAvancadoSpec(
            limites_data={"dataSolicitacao": ("2023-01-01", "2024-12-31")}
        )
        assert spec.limites_data["dataSolicitacao"] == ("2023-01-01", "2024-12-31")

    def test_creates_with_booleano(self):
        spec = FiltroAvancadoSpec(
            booleanos={"SLA_Marco_Autorizada": True}
        )
        assert spec.booleanos["SLA_Marco_Autorizada"] is True


class TestFiltroAvancadoSpecSatisfiedBy:
    """is_satisfied_by: pure in-memory predicate — infrastructure-agnostic."""

    def test_empty_spec_satisfies_any_candidate(self):
        """An empty FiltroAvancadoSpec must never reject a candidate (universe)."""
        spec = FiltroAvancadoSpec()
        assert spec.is_satisfied_by({"qualquer": "coisa"}) is True
        assert spec.is_satisfied_by({}) is True

    def test_inclusao_satisfies_matching_candidate(self):
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"origem_lista": ["LISTA_A", "LISTA_B"]}
        )
        assert spec.is_satisfied_by({"origem_lista": "LISTA_A"}) is True

    def test_inclusao_rejects_non_matching_candidate(self):
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"origem_lista": ["LISTA_A"]}
        )
        assert spec.is_satisfied_by({"origem_lista": "LISTA_C"}) is False

    def test_exclusao_rejects_matching_candidate(self):
        spec = FiltroAvancadoSpec(
            colunas_exclusao={"entidade_complexidade": ["ALTA"]}
        )
        assert spec.is_satisfied_by({"entidade_complexidade": "ALTA"}) is False

    def test_exclusao_allows_non_matching_candidate(self):
        spec = FiltroAvancadoSpec(
            colunas_exclusao={"entidade_complexidade": ["ALTA"]}
        )
        assert spec.is_satisfied_by({"entidade_complexidade": "BAIXA"}) is True

    def test_booleano_true_matches(self):
        spec = FiltroAvancadoSpec(booleanos={"SLA_Marco_Autorizada": True})
        assert spec.is_satisfied_by({"SLA_Marco_Autorizada": True}) is True
        assert spec.is_satisfied_by({"SLA_Marco_Autorizada": False}) is False

    def test_booleano_false_matches(self):
        spec = FiltroAvancadoSpec(booleanos={"SLA_Marco_Autorizada": False})
        assert spec.is_satisfied_by({"SLA_Marco_Autorizada": False}) is True
        assert spec.is_satisfied_by({"SLA_Marco_Autorizada": True}) is False

    def test_limite_numerico_within_range(self):
        spec = FiltroAvancadoSpec(
            limites_numericos={"SLA_Lead_Time_Total_Dias": (10, 90)}
        )
        assert spec.is_satisfied_by({"SLA_Lead_Time_Total_Dias": 50}) is True

    def test_limite_numerico_boundary_values(self):
        """Boundary value analysis — verifies inclusive bounds (mutant killer)."""
        spec = FiltroAvancadoSpec(
            limites_numericos={"SLA_Lead_Time_Total_Dias": (10, 90)}
        )
        # Min boundary (inclusive)
        assert spec.is_satisfied_by({"SLA_Lead_Time_Total_Dias": 10}) is True
        # Max boundary (inclusive)
        assert spec.is_satisfied_by({"SLA_Lead_Time_Total_Dias": 90}) is True
        # Outside lower
        assert spec.is_satisfied_by({"SLA_Lead_Time_Total_Dias": 9}) is False
        # Outside upper
        assert spec.is_satisfied_by({"SLA_Lead_Time_Total_Dias": 91}) is False

    def test_non_dict_candidate_is_rejected(self):
        """The specification must handle non-dict gracefully (defensive programming)."""
        spec = FiltroAvancadoSpec(
            colunas_inclusao={"origem_lista": ["LISTA_A"]}
        )
        assert spec.is_satisfied_by(None) is False
        assert spec.is_satisfied_by("not_a_dict") is False


class TestFiltroAvancadoSpecIsComposable:
    """FiltroAvancadoSpec must compose via & | ~ operators from Specification ABC."""

    def test_and_composition_with_other_specs(self):
        from domain.specifications import PacienteUrgenteSpec

        urgentes = PacienteUrgenteSpec(cores_alvo=["VERMELHO"])
        filtro = FiltroAvancadoSpec(
            colunas_inclusao={"origem_lista": ["LISTA_SUS"]}
        )
        composite = urgentes & filtro
        # Candidate matches both specs
        candidate_ok = {
            "entidade_classificacaoRisco_cor": "VERMELHO",
            "origem_lista": "LISTA_SUS",
        }
        assert composite.is_satisfied_by(candidate_ok) is True
        # Candidate fails the filtro spec
        candidate_ko = {
            "entidade_classificacaoRisco_cor": "VERMELHO",
            "origem_lista": "LISTA_OUTRO",
        }
        assert composite.is_satisfied_by(candidate_ko) is False
