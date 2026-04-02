from abc import ABC, abstractmethod
from typing import List, Any, Tuple
from src.domain.models import AnalyticKPIs
from src.domain.specifications import Specification
from src.infrastructure.auth.token_acl import ValidatedUserToken
import pandas as pd

class IAnalyticsRepository(ABC):
    @abstractmethod
    def get_kpis(self, spec: Specification, spec_urgentes: Specification, spec_vencidos: Specification, user: ValidatedUserToken) -> AnalyticKPIs:
        pass

    @abstractmethod
    def get_distribution_data(self, spec: Specification, user: ValidatedUserToken) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_dynamic_options(self, column: str, current_where: str, user: ValidatedUserToken) -> List[Any]:
        pass

    @abstractmethod
    def get_global_bounds(self, column: str, is_date: bool = False, user: ValidatedUserToken = None) -> Tuple[Any, Any]:
        pass

    @abstractmethod
    def execute_custom_query(self, sql: str, user: ValidatedUserToken) -> pd.DataFrame:
        pass
