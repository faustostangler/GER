"""Orquestrador de métricas analíticas — Application Use Case.

WHY: Antes deste refactoring, o Use Case importava diretamente `settings`
da camada de infraestrutura para ler invariantes de negócio (SLA, cores de urgência,
mês comercial). Isso violava a Regra da Dependência (Clean Architecture): a camada
Application apenas pode depender do Domain, nunca da Infrastructure.

Solução: ClinicaPolicy (domain.policies) é injetada via construtor, tornando o
Use Case testável sem qualquer dependência de .env ou pydantic-settings.

Ref: ADR-005 — Business Policy Extraction from Infrastructure Config
"""
from application.use_cases.interfaces import IAnalyticsRepository
from domain.models import AnalyticKPIs
from domain.policies import ClinicaPolicy, DEFAULT_CLINICA_POLICY
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

    def get_executive_summary(
        self, spec: Specification, current_user: ValidatedUserToken
    ) -> AnalyticKPIs:
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
        return kpis

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
