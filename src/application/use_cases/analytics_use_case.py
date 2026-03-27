from src.application.use_cases.interfaces import IAnalyticsRepository
from src.domain.models import AnalyticKPIs, FilterCriteria
from src.infrastructure.auth.token_acl import ValidatedUserToken
import pandas as pd
from typing import List, Tuple, Any

class AnalyticsUseCase:
    """Orquestrador de métricas analíticas e consultas de domínio."""
    def __init__(self, repository: IAnalyticsRepository):
        self.repository = repository

    def get_executive_summary(self, filters: FilterCriteria, current_user: ValidatedUserToken) -> AnalyticKPIs:
        return self.repository.get_kpis(filters, current_user)

    def get_distribution_analysis(self, filters: FilterCriteria, current_user: ValidatedUserToken) -> pd.DataFrame:
        return self.repository.get_distribution_data(filters, current_user)

    def get_dynamic_options(self, column: str, current_where: str, current_user: ValidatedUserToken) -> List[Any]:
        return self.repository.get_dynamic_options(column, current_where, current_user)

    def get_global_bounds(self, column: str, is_date: bool = False, current_user: ValidatedUserToken = None) -> Tuple[Any, Any]:
        return self.repository.get_global_bounds(column, is_date, current_user)

    def execute_custom_query(self, sql: str, filters: FilterCriteria, current_user: ValidatedUserToken) -> pd.DataFrame:
        # Injetando FilterCriteria no sql
        if filters and "{FINAL_WHERE}" in sql:
            sql = sql.replace("{FINAL_WHERE}", filters.get_where_clause())
        return self.repository.execute_custom_query(sql, current_user)
