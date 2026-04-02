from src.application.use_cases.interfaces import IAnalyticsRepository
from src.domain.models import AnalyticKPIs
from src.domain.specifications import Specification, PacienteUrgenteSpec, PacienteVencidoSpec
from src.infrastructure.config import settings
from src.infrastructure.auth.token_acl import ValidatedUserToken
import pandas as pd
from typing import List, Tuple, Any

class AnalyticsUseCase:
    """Orquestrador de métricas analíticas e consultas de domínio."""
    def __init__(self, repository: IAnalyticsRepository):
        self.repository = repository

    def get_executive_summary(self, spec: Specification, current_user: ValidatedUserToken) -> AnalyticKPIs:
        spec_vencidos = PacienteVencidoSpec(dias_tolerancia=settings.SLA_DIAS_VENCIMENTO)
        spec_urgentes = PacienteUrgenteSpec(cores_alvo=settings.CORES_URGENCIA)
        
        kpis = self.repository.get_kpis(spec, spec_urgentes, spec_vencidos, current_user)
        kpis.mes_comercial = settings.MES_COMERCIAL_DIAS
        return kpis

    def get_distribution_analysis(self, spec: Specification, current_user: ValidatedUserToken) -> pd.DataFrame:
        return self.repository.get_distribution_data(spec, current_user)

    def get_dynamic_options(self, column: str, current_where: str, current_user: ValidatedUserToken) -> List[Any]:
        return self.repository.get_dynamic_options(column, current_where, current_user)

    def get_global_bounds(self, column: str, is_date: bool = False, current_user: ValidatedUserToken = None) -> Tuple[Any, Any]:
        return self.repository.get_global_bounds(column, is_date, current_user)

    def execute_custom_query(self, sql: str, spec: Specification, current_user: ValidatedUserToken) -> pd.DataFrame:
        # Injetando spec no sql usando tradutor se necessário, mas o spec_translator vive no adapter
        # O use case geralmente orquestra
        if spec and "{FINAL_WHERE}" in sql:
            # Atenção: Passar a responsabilidade da tradução para o repositório
            pass
        return self.repository.execute_custom_query(sql, current_user)
