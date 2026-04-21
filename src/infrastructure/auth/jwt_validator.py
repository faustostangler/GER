import threading
import logging
from typing import Optional

import jwt
from jwt import PyJWKClient, PyJWKClientError

from domain.identity import (
    DoctorProfile,
    IdentityContractViolationException,
)
from infrastructure.auth.token_acl import ValidatedUserToken
from infrastructure.config import settings
from infrastructure.repositories.doctor_profile_repository import SQLDoctorProfileRepository

logger = logging.getLogger(__name__)


class InvalidTokenFormatError(Exception):
    """Raised when the JWT structure is malformed (e.g. missing 'kid' header)."""


# SRE: 24h caching layer for Keycloak JWKS public keys.
jwks_client = PyJWKClient(settings.jwks_url, cache_keys=True, lifespan=86400)

# SRE: Lock for Double-Checked Locking (DCL) to prevent 'self-DDoS' (Thundering Herd)
jwks_lock = threading.Lock()

# Global repository for DoctorProfile lookups
_doctor_profile_repo: Optional[SQLDoctorProfileRepository] = None
_repo_lock = threading.Lock()


def get_doctor_profile_repo() -> SQLDoctorProfileRepository:
    """Singleton getter for the doctor profile repository."""
    global _doctor_profile_repo
    if _doctor_profile_repo is None:
        with _repo_lock:
            if _doctor_profile_repo is None:
                _doctor_profile_repo = SQLDoctorProfileRepository()
    return _doctor_profile_repo


def _lookup_doctor_profile(user_id: str) -> Optional[DoctorProfile]:
    """Port implementation: retrieves a DoctorProfile by Keycloak UUID from the SQL store."""
    repo = get_doctor_profile_repo()
    return repo.find_by_user_id(user_id)


def verify_token(token: str) -> ValidatedUserToken:
    """Verify a Bearer JWT and enforce domain CRM authorization.

    Zero-Trust Strategy:
    1. Verify signature using RS256/JWKS (Distributed Keys).
    2. Check CRM validation status in the local PostgreSQL 'Source of Truth'.
    3. Raise IdentityContractViolationException if unverified.

    WHY (DCL Pattern): Thundering Herd protection for JWKS fetching.
    """
    global jwks_client

    try:
        # Phase 1: Identity (JWKS with SRE Resilience)
        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token)
        except PyJWKClientError:
            # Double-Checked Locking
            with jwks_lock:
                try:
                    signing_key = jwks_client.get_signing_key_from_jwt(token)
                except PyJWKClientError:
                    logger.info("JWKS cache stale — refreshing from Keycloak (1 HTTP call).")
                    jwks_client = PyJWKClient(settings.jwks_url, cache_keys=True, lifespan=86400)
                    signing_key = jwks_client.get_signing_key_from_jwt(token)

        payload = jwt.decode(
            token,
            key=signing_key.key,
            algorithms=["RS256"],
            audience=settings.KEYCLOAK_CLIENT_ID,
            issuer=settings.keycloak_issuer,
            options={"verify_iat": True, "verify_exp": True},
        )

        sub: str = payload.get("sub", "")
        if not sub:
            raise InvalidTokenFormatError("JWT payload missing 'sub' claim.")

        # Phase 2: Domain Authorization (Zero-Trust CRM)
        # WHY: truths live in our DB, not in external JWT claims.
        profile = _lookup_doctor_profile(sub)

        if not profile or not profile.is_authorized():
            crm_raw = payload.get("crm_numero", "N/A")
            logger.warning(f"Resilience Breach Attempt: User {sub} (CRM {crm_raw}) failed clinical authorization.")
            raise IdentityContractViolationException(
                message="Seu acesso está bloqueado: CRM não verificado ou inexistente.",
                user_id=sub,
                crm_raw=crm_raw
            )

        # Success: ACL mapping
        return ValidatedUserToken(
            sub=sub,
            email=payload.get("email", ""),
            preferred_username=payload.get("preferred_username", ""),
            roles=payload.get("realm_access", {}).get("roles", []),
            crm_numero=profile.crm.crm_numero,
            crm_uf=profile.crm.crm_uf,
            exp=payload.get("exp"),
        )

    except IdentityContractViolationException:
        raise
    except jwt.ExpiredSignatureError:
        raise ValueError("Token expirado.")
    except (jwt.PyJWTError, PyJWKClientError, ValueError) as e:
        logger.error(f"JWT Verification failed: {str(e)}")
        raise ValueError(f"Falha na validação do token: {str(e)}")
    except Exception as e:
        logger.exception(f"Unexpected error in verify_token: {str(e)}")
        raise
