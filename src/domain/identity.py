from pydantic import BaseModel, ConfigDict, Field, field_validator
import re
from typing import Optional


class MedicalCouncilRegistration(BaseModel):
    """Value Object representing a validated Brazilian Medical Council registration.

    WHY (frozen=True): Value Objects are identified by their content, not by reference.
    Immutability guarantees that two objects with the same CRM/UF are always equivalent
    and prevents silent mutation after CFM validation has been confirmed.
    """

    model_config = ConfigDict(frozen=True)
    crm_numero: str = Field(..., description="O número do CRM contendo apenas dígitos")
    crm_uf: str = Field(..., description="A UF do CRM contendo exatamente 2 letras")

    @field_validator("crm_uf")
    @classmethod
    def validate_uf(cls, v: str) -> str:
        v = v.strip().upper()
        if not re.match(r"^[A-Z]{2}$", v):
            raise ValueError("CRM UF deve conter exatamente 2 letras.")
        return v

    @field_validator("crm_numero")
    @classmethod
    def validate_crm(cls, v: str) -> str:
        v = v.strip()
        if not re.match(r"^\d+$", v):
            raise ValueError("CRM Numero deve conter apenas digitos numéricos.")
        return v


class DoctorProfile(BaseModel):
    """Domain Entity linking a Keycloak identity to a locally verified CRM registration.

    WHY (DDD separation of concerns): Keycloak is the Identity Provider — it answers
    "who are you?" via password verification. This entity answers "are you authorized
    to practice medicine in this domain?" — a separate, domain-owned concern.

    The ``crm_verified`` flag is ONLY set to True by the Kafka consumer pipeline after
    successful CFM (Conselho Federal de Medicina) validation. It must NEVER be sourced
    from the JWT payload, which is controlled by an external system (Keycloak).

    Lifecycle:
      1. USER_REGISTERED Keycloak event → consumer creates DoctorProfile(crm_verified=False).
      2. CFM validation succeeds → consumer sets crm_verified=True and persists.
      3. Every API request: jwt_validator looks up DoctorProfile by sub and calls is_authorized().

    Ref: ADR-006 — IAM Zero-Trust & CRM Domain Authorization.

    Attributes:
        user_id: Keycloak subject UUID (``sub`` claim). Primary key for DB lookups.
        crm:     Embedded MedicalCouncilRegistration VO with validated CRM number and UF.
        crm_verified: True only after CFM validation. Default False (safe-fail state).
    """

    model_config = ConfigDict(frozen=True)

    user_id: str = Field(..., min_length=1, description="Keycloak UUID (sub claim)")
    crm: MedicalCouncilRegistration
    crm_verified: bool = Field(
        default=False,
        description="True only after successful CFM validation by the event consumer pipeline",
    )

    def is_authorized(self) -> bool:
        """Domain authorization gate — the single source of truth for CRM access control.

        WHY: Centralising this predicate in the domain ensures that authorization
        logic cannot drift between API routes, background workers, or CLI tools.
        All entry points call is_authorized(); none implement their own CRM check.

        Returns:
            bool: True if the doctor's CRM has been locally verified against CFM.
                  False in all other states (pending, rejected, or unknown).
        """
        return self.crm_verified


class IdentityContractViolationException(Exception):
    """Exception raised when a user is authenticated but does not possess a verified CRM."""

    def __init__(self, message: str, user_id: str, crm_raw: Optional[str] = None):
        super().__init__(message)
        self.user_id = user_id
        self.crm_raw = crm_raw
