import datetime
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, Field


class Specification(ABC):
    @abstractmethod
    def is_satisfied_by(self, candidate: Any) -> bool:
        pass

    def __and__(self, other: "Specification") -> "Specification":
        return AndSpecification(self, other)

    def __or__(self, other: "Specification") -> "Specification":
        return OrSpecification(self, other)

    def __invert__(self) -> "Specification":
        return NotSpecification(self)


class AndSpecification(Specification):
    def __init__(self, left: Specification, right: Specification):
        self.left = left
        self.right = right

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self.left.is_satisfied_by(candidate) and self.right.is_satisfied_by(
            candidate
        )


class OrSpecification(Specification):
    def __init__(self, left: Specification, right: Specification):
        self.left = left
        self.right = right

    def is_satisfied_by(self, candidate: Any) -> bool:
        return self.left.is_satisfied_by(candidate) or self.right.is_satisfied_by(
            candidate
        )


class NotSpecification(Specification):
    def __init__(self, spec: Specification):
        self.spec = spec

    def is_satisfied_by(self, candidate: Any) -> bool:
        return not self.spec.is_satisfied_by(candidate)


class PacienteUrgenteSpec(Specification):
    def __init__(self, cores_alvo: list[str]):
        self.cores_urgencia = cores_alvo

    def is_satisfied_by(self, candidate: Any) -> bool:
        if isinstance(candidate, dict):
            risk = candidate.get("entidade_classificacaoRisco_cor", "").upper()
            return risk in self.cores_urgencia
        return False


class PacienteVencidoSpec(Specification):
    def __init__(self, dias_tolerancia: int):
        self.dias_vencimento = dias_tolerancia

    def is_satisfied_by(self, candidate: Any) -> bool:
        if isinstance(candidate, dict):
            data_solicitacao = candidate.get("dataSolicitacao")
            if data_solicitacao:
                if isinstance(data_solicitacao, str):
                    data_solicitacao = datetime.datetime.strptime(
                        data_solicitacao.split()[0], "%Y-%m-%d"
                    )
                dias = (datetime.datetime.now() - data_solicitacao).days
                return dias > self.dias_vencimento
        return False


class LeadTimeCriticoSpec(Specification):
    def __init__(self, max_dias: int):
        self.max_dias = max_dias

    def is_satisfied_by(self, candidate: Any) -> bool:
        if isinstance(candidate, dict):
            data_solicitacao = candidate.get("dataSolicitacao")
            if data_solicitacao:
                if isinstance(data_solicitacao, str):
                    data_solicitacao = datetime.datetime.strptime(
                        data_solicitacao.split()[0], "%Y-%m-%d"
                    )
                dias = (datetime.datetime.now() - data_solicitacao).days
                return dias > self.max_dias
        return False


class AdvancedSearchCriteria(BaseModel):
    """Value Object para busca textual avançada (strip_accents, wildcards, agregação)."""

    model_config = {"frozen": True}

    column: str
    or_terms: list[str] = Field(default_factory=list)
    and_terms: list[str] = Field(default_factory=list)
    not_terms: list[str] = Field(default_factory=list)
    aggregate_by: str | None = None


class FiltroAvancadoSpec(BaseModel, Specification):
    """Value Object de Filtro de Consulta Analítica — representação semântica pura.

    WHY: Substituição do FilterCriteria corrompido que vazava SQL (list[str] clauses)
    para dentro do Core Domain. Este objeto representa "O QUÊ" filtrar usando
    vocabulário clínico (Ubiquitous Language), não "COMO" o storage executa.

    A tradução para SQL é responsabilidade exclusiva do DuckDBCriteriaTranslator
    (Adapter na camada Infrastructure), que implementa o Port IQueryTranslator.

    Attributes:
        colunas_inclusao: Mapa coluna → lista de valores permitidos (IN clause).
        colunas_exclusao: Mapa coluna → lista de valores bloqueados (NOT IN clause).
        termos_texto: Mapa coluna → termos para busca textual (ILIKE).
        limites_numericos: Mapa coluna → (min, max) para faixa numérica (BETWEEN).
        limites_data: Mapa coluna → (data_inicio, data_fim) ISO-8601 (BETWEEN).
        booleanos: Mapa coluna → valor booleano exato.
    """

    model_config = {"frozen": True}  # Value Object: imutável após criação

    colunas_inclusao: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Colunas com valores permitidos (lógica de inclusão)",
    )
    colunas_exclusao: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Colunas com valores bloqueados (lógica de exclusão)",
    )
    termos_texto: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Colunas com termos de busca textual (case-insensitive)",
    )
    limites_numericos: dict[str, tuple[int | float, int | float]] = Field(
        default_factory=dict,
        description="Colunas com faixa numérica permitida [min, max] incluso",
    )
    limites_data: dict[str, tuple[str, str]] = Field(
        default_factory=dict,
        description="Colunas com faixa de datas ISO-8601 [inicio, fim] inclusa",
    )
    booleanos: dict[str, bool] = Field(
        default_factory=dict,
        description="Colunas com valor booleano exato exigido",
    )
    booleanos_nullable: dict[str, bool] = Field(
        default_factory=dict,
        description="Trata o valor False como 'FALSE OU IS NULL' (necessário p/ UI No radios)",
    )
    presenca_campos: dict[str, bool] = Field(
        default_factory=dict,
        description="Colunas que devem estar presentes (!= null e != vazio) ou ausentes",
    )
    busca_avancada: list[AdvancedSearchCriteria] = Field(
        default_factory=list,
        description="Critérios complexos de busca textual tolerante a acentos",
    )

    def is_satisfied_by(self, candidate: Any) -> bool:
        """Avalia o candidato contra todos os critérios semânticos em memória.

        WHY: Permite uso em filtros in-memory (testes unitários, streaming futuros)
        sem dependência de infra SQL. Retorna True se TODOS os critérios forem
        satisfeitos (conjunção implícita entre campos de mesmo grupo).
        """
        if not isinstance(candidate, dict):
            return False

        for coluna, valores_permitidos in self.colunas_inclusao.items():
            if candidate.get(coluna) not in valores_permitidos:
                return False

        for coluna, valores_bloqueados in self.colunas_exclusao.items():
            if candidate.get(coluna) in valores_bloqueados:
                return False

        for coluna, valor_bool in self.booleanos.items():
            if candidate.get(coluna) != valor_bool:
                return False

        for coluna, (minimo, maximo) in self.limites_numericos.items():
            valor = candidate.get(coluna)
            if valor is None:
                return False
            try:
                if not (minimo <= float(valor) <= maximo):
                    return False
            except (TypeError, ValueError):
                return False

        # WHY: terms for busca_avancada, booleanos_nullable, and presenca_campos
        # are deferred to infrastructure (SQL) evaluation.

        return True


class FiltroAvancadoSpecBuilder:
    """Builder mutável para montagem progressiva da especificação imutável.

    WHY: Permite que os widgets da UI adicionem predicados progressivamente
    (ex: render_include_exclude, render_age_slider) preenchendo os dicionários,
    sem precisarem gerenciar tuplas imutáveis ou vazar listas SQL.
    """

    def __init__(self):
        self._colunas_inclusao: dict[str, list[str]] = {}
        self._colunas_exclusao: dict[str, list[str]] = {}
        self._termos_texto: dict[str, list[str]] = {}
        self._limites_numericos: dict[str, tuple[int | float, int | float]] = {}
        self._limites_data: dict[str, tuple[str, str]] = {}
        self._booleanos: dict[str, bool] = {}
        self._booleanos_nullable: dict[str, bool] = {}
        self._presenca_campos: dict[str, bool] = {}
        self._busca_avancada: list[AdvancedSearchCriteria] = []

    def add_inclusao(
        self, column: str, values: list[str]
    ) -> "FiltroAvancadoSpecBuilder":
        if values:
            self._colunas_inclusao[column] = values
        return self

    def add_exclusao(
        self, column: str, values: list[str]
    ) -> "FiltroAvancadoSpecBuilder":
        if values:
            self._colunas_exclusao[column] = values
        return self

    def add_booleano(self, column: str, value: bool) -> "FiltroAvancadoSpecBuilder":
        self._booleanos[column] = value
        return self

    def add_booleano_nullable(
        self, column: str, value: bool
    ) -> "FiltroAvancadoSpecBuilder":
        self._booleanos_nullable[column] = value
        return self

    def add_presenca(self, column: str, value: bool) -> "FiltroAvancadoSpecBuilder":
        self._presenca_campos[column] = value
        return self

    def add_limite_numerico(
        self, column: str, vmin: int | float, vmax: int | float
    ) -> "FiltroAvancadoSpecBuilder":
        self._limites_numericos[column] = (vmin, vmax)
        return self

    def add_limite_data(
        self, column: str, data_inicio: str, data_fim: str
    ) -> "FiltroAvancadoSpecBuilder":
        self._limites_data[column] = (data_inicio, data_fim)
        return self

    def add_texto(self, column: str, termos: list[str]) -> "FiltroAvancadoSpecBuilder":
        if termos:
            self._termos_texto[column] = termos
        return self

    def add_busca_avancada(
        self,
        column: str,
        or_terms: list[str] = None,
        and_terms: list[str] = None,
        not_terms: list[str] = None,
        aggregate_by: str | None = None,
    ) -> "FiltroAvancadoSpecBuilder":
        if not (or_terms or and_terms or not_terms):
            return self

        crit = AdvancedSearchCriteria(
            column=column,
            or_terms=or_terms or [],
            and_terms=and_terms or [],
            not_terms=not_terms or [],
            aggregate_by=aggregate_by,
        )
        self._busca_avancada.append(crit)
        return self

    def build(self) -> FiltroAvancadoSpec:
        """Produz o Value Object imutável."""
        return FiltroAvancadoSpec(
            colunas_inclusao=self._colunas_inclusao.copy(),
            colunas_exclusao=self._colunas_exclusao.copy(),
            termos_texto=self._termos_texto.copy(),
            limites_numericos=self._limites_numericos.copy(),
            limites_data=self._limites_data.copy(),
            booleanos=self._booleanos.copy(),
            booleanos_nullable=self._booleanos_nullable.copy(),
            presenca_campos=self._presenca_campos.copy(),
            busca_avancada=list(self._busca_avancada),
        )
