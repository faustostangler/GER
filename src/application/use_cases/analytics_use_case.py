"""Orquestrador de métricas analíticas — Application Use Case.

WHY: Antes deste refactoring, o Use Case importava diretamente `settings`
da camada de infraestrutura para ler invariantes de negócio (SLA, cores de urgência,
mês comercial). Isso violava a Regra da Dependência (Clean Architecture): a camada
Application apenas pode depender do Domain, nunca da Infrastructure.

Solução: ClinicaPolicy (domain.models) é injetada via construtor, tornando o
Use Case testável sem qualquer dependência de .env ou pydantic-settings.

Ref: ADR-005 — Business Policy Extraction from Infrastructure Config
"""
from application.use_cases.interfaces import IAnalyticsRepository
from domain.models import (
    ClinicaPolicy,
    DEFAULT_CLINICA_POLICY,
    DashboardState,
)
from domain.specifications import (
    Specification,
    PacienteUrgenteSpec,
    PacienteVencidoSpec,
)
from infrastructure.auth.token_acl import ValidatedUserToken
import pandas as pd
from typing import List, Tuple, Any


class AnalyticsUseCase:
    """Orquestrador de métricas analíticas e consultas de domínio.

    Args:
        repository: Adaptador de repositório analítico (Port).
        policy: Política de negócio clínica com invariantes do domínio.
                Defaults para DEFAULT_CLINICA_POLICY quando não injetada.

    WHY policy como parâmetro explícito: Dependency Injection permite que testes
    sobrescrevam a política sem tocar em .env ou mocks de settings. Prod usa
    a instância padrão carregada na inicialização da app a partir de AppSettings.
    """

    def __init__(
        self,
        repository: IAnalyticsRepository,
        policy: ClinicaPolicy = DEFAULT_CLINICA_POLICY,
    ):
        self.repository = repository
        self._policy = policy

    def get_active_policy(self) -> ClinicaPolicy:
        """Returns the domain business policy applied to this use case session."""
        return self._policy

    def verify_data_readiness(self) -> None:
        self.repository.verify_data_readiness()

    def get_executive_summary(
        self, spec: Specification, current_user: ValidatedUserToken
    ) -> DashboardState:
        spec_vencidos = PacienteVencidoSpec(
            dias_tolerancia=self._policy.sla_dias_vencimento
        )
        spec_urgentes = PacienteUrgenteSpec(
            cores_alvo=list(self._policy.cores_urgencia)
        )

        kpis = self.repository.get_kpis(
            spec, spec_urgentes, spec_vencidos, current_user
        )
        kpis.mes_comercial = self._policy.mes_comercial_dias
        return DashboardState(kpis=kpis, policy=self._policy)

    def get_clinical_audit_heatmap(
        self,
        col_ator: str,
        col_diag: str,
        top_x_med: int,
        top_x_cid: int,
        modo_heatmap: str,
        spec: Specification,
        current_user: ValidatedUserToken,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Gera matrizes para o Heatmap de Auditoria Clínica (Z-Score).
        
        WHY: Remove lógica pesada de Pandas (pivot, desvio padrão) e SQL
        da camada de apresentação (Streamlit), permitindo testes isolados
        e cache otimizado.
        """
        OPT_CID = "Horizontal Analysis (Peer Comparison)"
        OPT_MED = "Vertical Analysis (Individual Profile)"

        df_heatmap = self.repository.execute_custom_query(
            f"""
            WITH TopAtores AS (
                SELECT "{col_ator}" FROM gercon
                WHERE {{FINAL_WHERE}} AND "{col_ator}" != '' AND "{col_ator}" IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT numeroCMCE) DESC LIMIT {top_x_med}
            ),
            TopDiags AS (
                SELECT "{col_diag}" FROM gercon
                WHERE {{FINAL_WHERE}} AND "{col_diag}" != '' AND "{col_diag}" IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT numeroCMCE) DESC LIMIT {top_x_cid}
            )
            SELECT
                "{col_ator}"  AS _ator,
                "{col_diag}"  AS _diag,
                COUNT(DISTINCT numeroCMCE) as Vol
            FROM gercon
            WHERE {{FINAL_WHERE}}
              AND "{col_ator}" IN (SELECT "{col_ator}" FROM TopAtores)
              AND "{col_diag}" IN (SELECT "{col_diag}" FROM TopDiags)
            GROUP BY 1, 2
            """,
            spec,
            current_user,
        )

        df_math = pd.DataFrame()
        df_pivot_vol = pd.DataFrame()
        df_text = pd.DataFrame()

        if (
            not df_heatmap.empty
            and "_diag" in df_heatmap.columns
            and "_ator" in df_heatmap.columns
        ):
            df_heatmap["_diag_curto"] = df_heatmap["_diag"].apply(
                lambda x: str(x)[:45] + "..." if len(str(x)) > 45 else x
            )

            df_pivot_vol = df_heatmap.pivot_table(
                index="_diag_curto",
                columns="_ator",
                values="Vol",
                fill_value=0,
            )
            df_math = df_pivot_vol.copy().astype(float)

            if modo_heatmap == OPT_CID:
                medias_linhas = df_math.mean(axis=1)
                desvios_linhas = df_math.std(axis=1).replace(0, 1)
                df_math = df_math.sub(medias_linhas, axis=0).div(
                    desvios_linhas, axis=0
                )
            elif modo_heatmap == OPT_MED:
                medias_colunas = df_math.mean(axis=0)
                desvios_colunas = df_math.std(axis=0).replace(0, 1)
                df_math = df_math.sub(medias_colunas, axis=1).div(
                    desvios_colunas, axis=1
                )

            df_text = df_math.apply(lambda col: col.map(lambda x: f"{x:+.1f}"))

        return df_math, df_pivot_vol, df_text

    def get_distribution_analysis(
        self, spec: Specification, current_user: ValidatedUserToken
    ) -> pd.DataFrame:
        return self.repository.get_distribution_data(spec, current_user)

    def get_dynamic_options(
        self, column: str, current_where: str, current_user: ValidatedUserToken
    ) -> List[Any]:
        return self.repository.get_dynamic_options(column, current_where, current_user)

    def get_global_bounds(
        self,
        column: str,
        is_date: bool = False,
        current_user: ValidatedUserToken = None,
    ) -> Tuple[Any, Any]:
        return self.repository.get_global_bounds(column, is_date, current_user)

    def execute_custom_query(
        self, sql: str, spec: Specification, current_user: ValidatedUserToken
    ) -> pd.DataFrame:
        return self.repository.execute_custom_query(sql, spec, current_user)
