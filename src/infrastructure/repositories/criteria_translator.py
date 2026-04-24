"""DuckDB SQL Criteria Translator — Infrastructure Adapter (Port: IQueryTranslator).

WHY: Implementa a tradução de Specification objects (domínio puro) para strings SQL
do dialeto DuckDB. Este é o Adapter no padrão Ports & Adapters que consome o Port
implícito do Specification pattern.

Antes desta refatoração, o DuckDBSpecificationTranslator estava embutido no mesmo
arquivo do DuckDBAnalyticsRepository, acoplando ainda mais o translator ao repositório.
A separação em arquivo próprio permite:
  1. Testar o translator de forma isolada (sem instanciar repositório DuckDB).
  2. Substituir o dialeto SQL (ex: BigQuery, Postgres) sem mudar o repositório.
  3. Cumprir o princípio SRP: o repositório executa, o translator converte.

Related: SQL Leak refactoring — FilterCriteria (SQL) → FiltroAvancadoSpec (semântico).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from domain.specifications import (
    AndSpecification,
    FiltroAvancadoSpec,
    LeadTimeCriticoSpec,
    NotSpecification,
    OrSpecification,
    PacienteUrgenteSpec,
    PacienteVencidoSpec,
    Specification,
)

if TYPE_CHECKING:
    pass


import logging

logger = logging.getLogger(__name__)


class DuckDBCriteriaTranslator:
    """Adapter: converte Specification domain objects em cláusulas SQL para DuckDB.

    WHY: Centraliza toda a lógia de tradução SQL neste único ponto (Single Source
    of Truth para mapeamento domínio → SQL). Mudanças no dialeto DuckDB ou na
    estrutura do Parquet impactam apenas esta classe, não o domínio.
    """

    @staticmethod
    def translate(spec: Specification | None) -> str:
        """Converte uma Specification em uma cláusula SQL (sem WHERE prefix).

        Args:
            spec: Specification do domínio, ou None para cláusula universo.

        Returns:
            String SQL pronta para injeção em cláusula WHERE. Retorna '1=1'
            para specs None ou desconhecidas (universo — sem filtro).
        """
        if spec is None:
            return "1=1"

        # Defensive check for hot-reload class mismatch
        if spec.__class__.__name__ == "FiltroAvancadoSpec":
            return DuckDBCriteriaTranslator._translate_filtro(spec)

        match spec:
            case AndSpecification():
                left = DuckDBCriteriaTranslator.translate(spec.left)
                right = DuckDBCriteriaTranslator.translate(spec.right)
                return f"({left} AND {right})"

            case OrSpecification():
                left = DuckDBCriteriaTranslator.translate(spec.left)
                right = DuckDBCriteriaTranslator.translate(spec.right)
                return f"({left} OR {right})"

            case NotSpecification():
                inner = DuckDBCriteriaTranslator.translate(spec.spec)
                return f"NOT ({inner})"

            case PacienteUrgenteSpec():
                cores_str = ", ".join(f"'{c}'" for c in spec.cores_urgencia)
                return f"entidade_classificacaoRisco_cor IN ({cores_str})"

            case PacienteVencidoSpec():
                return (
                    f"DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE)"
                    f" > {spec.dias_vencimento}"
                )

            case LeadTimeCriticoSpec():
                return (
                    f"DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE)"
                    f" > {spec.max_dias}"
                )

            case FiltroAvancadoSpec():
                return DuckDBCriteriaTranslator._translate_filtro(spec)

            case _:
                # WHY: Spec desconhecida → cláusula universo (fail-open seguro).
                # Nunca deve rejeitar dados silenciosamente para um tipo não mapeado.
                logger.warning(
                    f"[SRE-WARN] Unmapped Specification detected: {type(spec)}. Falling back to 1=1."
                )
                import sentry_sdk

                sentry_sdk.capture_message(
                    f"[SRE-WARN] Unmapped Specification detected: {type(spec)}. Falling back to 1=1.",
                    level="warning",
                )
                return "1=1"

    @staticmethod
    def _translate_filtro(spec: FiltroAvancadoSpec) -> str:
        """Converte os campos semânticos de FiltroAvancadoSpec em predicados SQL.

        WHY: Mantém a lógica de tradução dos múltiplos tipos de campo (IN, NOT IN,
        BETWEEN, ILIKE, boolean) centralizada e testável de forma unitária.
        """
        parts: list[str] = []

        # --- Inclusão: coluna IN (val1, val2, ...) ---
        for coluna, valores in spec.colunas_inclusao.items():
            if valores:
                vals_sql = ", ".join(
                    f"'{v.replace(chr(39), chr(39) + chr(39))}'" for v in valores
                )
                parts.append(f'"{coluna}" IN ({vals_sql})')

        # --- Exclusão: coluna NOT IN (val1, val2, ...) ---
        for coluna, valores in spec.colunas_exclusao.items():
            if valores:
                vals_sql = ", ".join(
                    f"'{v.replace(chr(39), chr(39) + chr(39))}'" for v in valores
                )
                parts.append(f'"{coluna}" NOT IN ({vals_sql})')

        # --- Booleanos: coluna = TRUE / FALSE ---
        for coluna, valor in spec.booleanos.items():
            bool_sql = "TRUE" if valor else "FALSE"
            parts.append(f'"{coluna}" = {bool_sql}')

        # --- Booleanos Nullable: coluna = TRUE ou (coluna = FALSE OR IS NULL) ---
        for coluna, valor in spec.booleanos_nullable.items():
            if valor:
                parts.append(f'"{coluna}" = TRUE')
            else:
                parts.append(f'("{coluna}" = FALSE OR "{coluna}" IS NULL)')

        # --- Presenca: coluna IS NOT NULL AND != '' ou IS NULL OR = '' ---
        for coluna, presente in spec.presenca_campos.items():
            if presente:
                parts.append(f'("{coluna}" IS NOT NULL AND "{coluna}" != \'\')')
            else:
                parts.append(f'("{coluna}" IS NULL OR "{coluna}" = \'\')')

        # --- Limites Numéricos: coluna BETWEEN min AND max ---
        for coluna, (minimo, maximo) in spec.limites_numericos.items():
            if coluna == "usuarioSUS_numero":
                parts.append(
                    f'TRY_CAST("{coluna}" AS INTEGER) BETWEEN {minimo} AND {maximo}'
                )
            else:
                parts.append(f'"{coluna}" BETWEEN {minimo} AND {maximo}')

        # --- Limites de Data: CAST(coluna AS DATE) BETWEEN 'inicio' AND 'fim' ---
        for coluna, (inicio, fim) in spec.limites_data.items():
            parts.append(f"CAST(\"{coluna}\" AS DATE) BETWEEN '{inicio}' AND '{fim}'")

        # --- Texto (ILIKE): coluna ILIKE '%termo%' ---
        for coluna, termos in spec.termos_texto.items():
            for termo in termos:
                termo_safe = termo.replace("'", "''")
                parts.append(f"\"{coluna}\" ILIKE '%{termo_safe}%'")

        # --- Busca Avançada: tolerante a acentos e opcionalmente agregada ---
        from presentation.adapters.parsers import parse_term

        for crit in spec.busca_avancada:
            col = crit.column
            having_conds = []
            row_conds = []

            if crit.or_terms:
                exprs = [
                    f"strip_accents(\"{col}\") ILIKE strip_accents('{parse_term(w)}')"
                    for w in crit.or_terms
                ]
                row_conds.append(f"({' OR '.join(exprs)})")
                agg_exprs = [
                    f"bool_or(strip_accents(\"{col}\") ILIKE strip_accents('{parse_term(w)}'))"
                    for w in crit.or_terms
                ]
                having_conds.append(f"({' OR '.join(agg_exprs)})")

            for w in crit.and_terms:
                p_term = parse_term(w)
                row_conds.append(
                    f"strip_accents(\"{col}\") ILIKE strip_accents('{p_term}')"
                )
                having_conds.append(
                    f"bool_or(strip_accents(\"{col}\") ILIKE strip_accents('{p_term}'))"
                )

            for w in crit.not_terms:
                p_term = parse_term(w)
                row_conds.append(
                    f"strip_accents(\"{col}\") NOT ILIKE strip_accents('{p_term}')"
                )
                having_conds.append(
                    f"bool_or(strip_accents(\"{col}\") ILIKE strip_accents('{p_term}')) = FALSE"
                )

            if crit.aggregate_by:
                if having_conds:
                    subq = f'SELECT "{crit.aggregate_by}" FROM BaseRLS GROUP BY "{crit.aggregate_by}" HAVING {" AND ".join(having_conds)}'
                    parts.append(f'"{crit.aggregate_by}" IN ({subq})')
            else:
                parts.extend(row_conds)

        return " AND ".join(parts) if parts else "1=1"
