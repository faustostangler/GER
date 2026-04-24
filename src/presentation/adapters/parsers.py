"""Presentation layer parsers and ACL builders (Humble Object pattern).

WHY: Mantém a camada de apresentação fina (thin). Toda lógica testavel é
extraída para este módulo, protegendo o domínio de construçóes SQL diretas.
"""

from __future__ import annotations

from domain.specifications import FiltroAvancadoSpec


def parse_term(term: str) -> str:
    if not term or not str(term).strip():
        return ""

    term = str(term).strip()

    # Se o usuário injetou o wildcard explicitamente (*), nós respeitamos a intenção dele
    if "*" in term:
        return term.replace("*", "%")

    # Comportamento SRE padrão: Se não há wildcard, busca por Contenção (Contains)
    return f"%{term}%"


class FiltroAvancadoSpecBuilder:
    """ACL (Anti-Corruption Layer) da camada Presentation → Domínio.

    WHY: Durante a migração da sidebar (que ainda gera strings SQL via 'clauses')
    para campos semânticos puros (FiltroAvancadoSpec), este builder atua como
    a 'cola' de tradução na borda da camada de apresentação.

    O DuckDBCriteriaTranslator re-traduz FiltroAvancadoSpec para SQL na infra.
    Para os campos migrados semanticamente, usa-se os campos tipados.
    Para clauses não-migradas ainda, eles são temporariamente passados como
    FiltroAvancadoSpec com colunas_inclusao raw via esse mapper.

    TODO(#ADR-004): Migrar cada widget da sidebar para gerar campos semânticos
    (colunas_inclusao, limites_numericos, etc.) e remover este builder.
    """

    def __init__(self) -> None:
        self._colunas_inclusao: dict[str, list[str]] = {}
        self._colunas_exclusao: dict[str, list[str]] = {}
        self._termos_texto: dict[str, list[str]] = {}
        self._limites_numericos: dict[str, tuple[int | float, int | float]] = {}
        self._limites_data: dict[str, tuple[str, str]] = {}
        self._booleanos: dict[str, bool] = {}

    def com_inclusao(
        self, coluna: str, valores: list[str]
    ) -> "FiltroAvancadoSpecBuilder":
        """Adiciona critério de inclusão (coluna IN valores)."""
        if valores:
            self._colunas_inclusao[coluna] = valores
        return self

    def com_exclusao(
        self, coluna: str, valores: list[str]
    ) -> "FiltroAvancadoSpecBuilder":
        """Adiciona critério de exclusão (coluna NOT IN valores)."""
        if valores:
            self._colunas_exclusao[coluna] = valores
        return self

    def com_texto(self, coluna: str, termos: list[str]) -> "FiltroAvancadoSpecBuilder":
        """Adiciona busca textual (coluna ILIKE '%termo%')."""
        if termos:
            self._termos_texto[coluna] = termos
        return self

    def com_faixa_numerica(
        self, coluna: str, minimo: int | float, maximo: int | float
    ) -> "FiltroAvancadoSpecBuilder":
        """Adiciona faixa numérica (coluna BETWEEN min AND max)."""
        self._limites_numericos[coluna] = (minimo, maximo)
        return self

    def com_faixa_data(
        self, coluna: str, inicio: str, fim: str
    ) -> "FiltroAvancadoSpecBuilder":
        """Adiciona faixa de datas ISO-8601."""
        self._limites_data[coluna] = (inicio, fim)
        return self

    def com_booleano(self, coluna: str, valor: bool) -> "FiltroAvancadoSpecBuilder":
        """Adiciona filtro booleano exato."""
        self._booleanos[coluna] = valor
        return self

    def build(self) -> FiltroAvancadoSpec:
        """Constrói o Value Object imutável FiltroAvancadoSpec."""
        return FiltroAvancadoSpec(
            colunas_inclusao=self._colunas_inclusao,
            colunas_exclusao=self._colunas_exclusao,
            termos_texto=self._termos_texto,
            limites_numericos=self._limites_numericos,
            limites_data=self._limites_data,
            booleanos=self._booleanos,
        )
