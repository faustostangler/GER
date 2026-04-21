from abc import ABC, abstractmethod
from typing import List, Any, Tuple, Optional
from domain.models import AnalyticKPIs
from domain.specifications import Specification
from domain.identity import DoctorProfile
from infrastructure.auth.token_acl import ValidatedUserToken
import pandas as pd


class IAnalyticsRepository(ABC):
    @abstractmethod
    def verify_data_readiness(self) -> None:
        pass

    @abstractmethod
    def get_kpis(
        self,
        spec: Specification,
        spec_urgentes: Specification,
        spec_vencidos: Specification,
        user: ValidatedUserToken,
    ) -> AnalyticKPIs:
        pass

    @abstractmethod
    def get_distribution_data(
        self, spec: Specification, user: ValidatedUserToken
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_dynamic_options(
        self, column: str, current_where: str, user: ValidatedUserToken
    ) -> List[Any]:
        pass

    @abstractmethod
    def get_global_bounds(
        self, column: str, is_date: bool = False, user: ValidatedUserToken = None
    ) -> Tuple[Any, Any]:
        pass

    @abstractmethod
    def execute_custom_query(
        self, sql: str, spec: Specification, user: ValidatedUserToken
    ) -> pd.DataFrame:
        pass

class IIdentityService(ABC):
    @abstractmethod
    def get_current_user(self) -> ValidatedUserToken:
        pass

    @abstractmethod
    def get_logout_url(self) -> str | None:
        pass

    @abstractmethod
    def is_authenticated(self) -> bool:
        pass


class IDoctorProfileRepository(ABC):
    """Port for DoctorProfile persistence and lookup."""

    @abstractmethod
    def find_by_user_id(self, user_id: str) -> Optional[DoctorProfile]:
        """Lookup a doctor profile by their Keycloak UUID (sub)."""
        pass

    @abstractmethod
    def save(self, profile: DoctorProfile) -> None:
        """Persist or update a doctor profile."""
        pass


class ICFMClient(ABC):
    """Port for external CRM (Medical Council) validation."""

    @abstractmethod
    async def validate(self, crm_numero: str, crm_uf: str) -> bool:
        """Validate CRM registration against the CFM API."""
        pass
