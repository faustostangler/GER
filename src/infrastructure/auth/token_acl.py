from pydantic import BaseModel, Field
from typing import Optional, List

# WHY (ADR-006 / IAM Zero-Trust): ValidatedUserToken is the ACL boundary between the
# Keycloak Identity Provider and our Domain Authorization layer.
#
# Identity (Keycloak):   answers "who are you?" — verified via JWT signature + expiry.
# Authorization (Domain): answers "are you CRM-authorized?" — verified via DoctorProfile
#                          stored in our DB after CFM validation (cf. keycloak_kafka_consumer).
#
# Migration completed: crm_numero and crm_uf are NO LONGER read from JWT payload claims.
# They are populated by jwt_validator.py after a successful DoctorProfile DB lookup.
# This prevents Keycloak (an external system) from bypassing domain CRM authorization.


class ValidatedUserToken(BaseModel):
    """ACL Data Transfer Envelope (DTE) between IAM infrastructure and domain consumers.

    Carries only the information that has been validated by BOTH layers:
      - Identity  : JWT signature verified by PyJWT against Keycloak JWKS.
      - Authorization: CRM confirmed by local DoctorProfile (crm_verified=True in DB).

    Consumers (FastAPI routes, Streamlit adapters) must treat every field here as
    trustworthy — the jwt_validator gate guarantees it before this object is created.

    Attributes:
        sub:                Keycloak UUID — stable user identifier across sessions.
        email:              User e-mail from the verified JWT payload.
        preferred_username: Human-readable username from the verified JWT payload.
        roles:              RBAC roles extracted from ``realm_access.roles`` JWT claim.
        crm_numero:         CRM registration number — sourced from DoctorProfile (DB),
                            NOT from JWT payload. Present only for crm_verified users.
        crm_uf:             CRM federation unit — same provenance as crm_numero.
        exp:                JWT expiration epoch. Used for session staleness checks in UI.
    """

    sub: str = Field(
        ..., description="Keycloak UUID (sub claim) — stable user identifier"
    )
    email: str
    preferred_username: str
    roles: List[str] = Field(
        default_factory=list, description="RBAC Roles extracted from realm_access.roles"
    )
    # WHY: These fields are populated from DoctorProfile (DB), not from JWT payload.
    #      They are Optional because the domain lookup happens AFTER JWT verification;
    #      a missing profile raises 403 before this object is constructed, so in practice
    #      a fully constructed ValidatedUserToken always has both fields set.
    crm_numero: Optional[str] = Field(
        default=None,
        description="CRM from DoctorProfile (DB-verified). Never from JWT claims.",
    )
    crm_uf: Optional[str] = Field(
        default=None,
        description="CRM UF from DoctorProfile (DB-verified). Never from JWT claims.",
    )
    exp: Optional[int] = Field(
        None, description="JWT expiration epoch for session staleness checks"
    )
