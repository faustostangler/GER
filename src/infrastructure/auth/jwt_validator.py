"""JWT verification adapter with SRE resilience and DDD domain authorization.

WHY (ADR-006): Two independent concerns are resolved here in strict layered order:

  1. Identity  (SRE layer) : Validates the JWT cryptographic signature against Keycloak
                              JWKS. Uses a Double-Checked Locking pattern to prevent the
                              "Thundering Herd" — a scenario where N concurrent threads
                              simultaneously detect a stale JWKS cache and hammer Keycloak
                              with N simultaneous /certs requests, causing an accidental
                              self-DDoS under key rotation.

  2. Authorization (Domain): After identity is proven, queries the local DoctorProfile
                              store to confirm the user's CRM has been verified by our
                              domain pipeline (cf. keycloak_kafka_consumer + CFM validation).
                              Returns 403 (not 401) on authorization failure because the
                              user IS authenticated; they simply lack domain authorization.

Ref: docs/adr/ADR-006-iam-zero-trust-crm-authorization.md
"""
import threading
from typing import Optional
import logging

import jwt
from jwt import PyJWKClient, PyJWKClientError
from fastapi import HTTPException, status

from infrastructure.config import settings
from infrastructure.auth.token_acl import ValidatedUserToken
from domain.identity import DoctorProfile

logger = logging.getLogger(__name__)


class InvalidTokenFormatError(Exception):
    """Raised when the JWT structure is malformed (e.g. missing 'kid' header)."""


# SRE: 24h caching layer for Keycloak JWKS public keys.
# WHY: Avoids a network round-trip to Keycloak on every request. Keys rotate rarely;
# 24h is a safe TTL that balances freshness with performance.
jwks_client = PyJWKClient(settings.jwks_url, cache_keys=True, lifespan=86400)

# SRE: Module-level threading.Lock for the Double-Checked Locking pattern.
# WHY threading.Lock() (not asyncio.Lock): FastAPI with Uvicorn can run in
# multi-threaded mode (--workers N); asyncio.Lock() is coroutine-safe but NOT
# thread-safe across OS threads. threading.Lock() is the correct primitive here.
# An asyncio.Lock() would deadlock if awaited from a sync context.
jwks_lock = threading.Lock()


def _lookup_doctor_profile(user_id: str) -> Optional[DoctorProfile]:
    """Port stub: retrieves a DoctorProfile by Keycloak UUID from the domain store.

    WHY (Hexagonal Architecture / Port): This function is the Port boundary between
    the jwt_validator adapter and the domain repository. In production it will delegate
    to a concrete IDoctorProfileRepository (Redis cache → PostgreSQL fallback).
    During unit tests it is monkeypatched to inject controlled DoctorProfile fixtures.

    Args:
        user_id: Keycloak ``sub`` UUID extracted from the verified JWT payload.

    Returns:
        DoctorProfile if found and loaded from the store, None otherwise.

    TODO(IAM/ADR-006): Replace stub with real IDoctorProfileRepository injection
    backed by Redis (fast path) → PostgreSQL (fallback). See implementation plan
    in docs/adr/ADR-006-iam-zero-trust-crm-authorization.md.
    """
    # ACL boundary: external infrastructure call isolated from domain logic.
    # Current implementation: in-memory no-op (safe-fail → returns None → 403).
    # Production: inject IDoctorProfileRepository via DI container.
    return None  # pragma: no cover — replaced by monkeypatch in tests


def verify_token(token: str) -> ValidatedUserToken:
    """Verify a Bearer JWT and enforce domain CRM authorization.

    Two-phase validation (order is critical):
      Phase 1 — Identity  : Verify cryptographic signature via Keycloak JWKS.
                             Raises HTTP 401 on any JWT error (expired, tampered, etc.).
      Phase 2 — Authorization: Look up DoctorProfile by ``sub``; assert crm_verified.
                             Raises HTTP 403 if the profile is missing or unverified.

    Double-Checked Locking (Thundering Herd prevention):
      If JWKS key lookup fails (PyJWKClientError), we acquire jwks_lock and attempt
      once more inside the critical section. Only ONE thread recreates the jwks_client;
      all others see the refreshed client when they acquire the lock next.

      Mental Model: 500 concurrent requests detect a stale cache simultaneously.
        → All 500 raise PyJWKClientError.
        → Thread A wins the lock race.
        → Thread A recreates jwks_client (1 HTTP call to Keycloak /certs).
        → Threads B–Z acquire the lock after A releases it, find valid client, skip recreate.
        → Result: exactly 1 Keycloak /certs request instead of 500.

    Args:
        token: Raw Bearer JWT string from the Authorization header.

    Returns:
        ValidatedUserToken: ACL envelope populated from JWT payload + DoctorProfile.

    Raises:
        HTTPException 401: JWT is expired, malformed, or cryptographically invalid.
        HTTPException 403: JWT is valid but the user lacks domain CRM authorization.
    """
    global jwks_client

    try:
        # ── Phase 1: Identity ─────────────────────────────────────────────────
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if not kid:
            raise InvalidTokenFormatError("Missing 'kid' in token header.")

        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token)
        except PyJWKClientError:
            # Double-Checked Locking: acquire the lock before recreating the client.
            # WHY: Multiple threads may reach this point simultaneously on key rotation.
            # The inner re-attempt reads a potentially already-refreshed client, preventing
            # redundant HTTP calls to Keycloak (the "Thundering Herd" DDoS vector).
            with jwks_lock:
                try:
                    # Inner check: another thread may have refreshed the client while
                    # this thread was waiting for the lock. Attempt before recreating.
                    signing_key = jwks_client.get_signing_key_from_jwt(token)
                    logger.debug(
                        "JWKS cache refreshed by peer thread — skipping re-fetch (lock acquired)."
                    )
                except PyJWKClientError:
                    # We are the first (or only) thread to hold the lock with a stale cache.
                    # Exactly ONE network call to Keycloak /certs happens here.
                    logger.info(
                        "JWKS cache stale — refreshing from Keycloak (lock held, 1 HTTP call)."
                    )
                    jwks_client = PyJWKClient(
                        settings.jwks_url, cache_keys=True, lifespan=86400
                    )
                    try:
                        signing_key = jwks_client.get_signing_key_from_jwt(token)
                    except PyJWKClientError as e:
                        raise InvalidTokenFormatError(
                            f"JWKS fetch failed after forced cache refresh: {e}"
                        )

        # Strict audience + issuer validation (Confused Deputy prevention).
        payload = jwt.decode(
            token,
            key=signing_key.key,
            algorithms=["RS256"],
            audience=settings.KEYCLOAK_CLIENT_ID,
            issuer=settings.keycloak_issuer,
        )

        # Extract identity fields from the verified payload.
        sub: str = payload.get("sub", "")
        realm_access = payload.get("realm_access", {})
        roles = realm_access.get("roles", [])

        # ── Phase 2: Domain Authorization ─────────────────────────────────────
        # WHY: We deliberately do NOT read crm_numero/crm_uf from payload here.
        # The JWT is an external artefact controlled by Keycloak. CRM authorization
        # is a domain concern whose truth lives in our DoctorProfile store, not in
        # claims emitted by a third-party IdP. This prevents privilege escalation
        # via crafted JWTs that carry fake crm_numero claims.
        profile: Optional[DoctorProfile] = _lookup_doctor_profile(sub)

        if profile is None:
            logger.warning(
                "Domain authorization denied: no DoctorProfile for sub=%s. "
                "User authenticated but not yet CRM-verified by the domain pipeline.",
                sub,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "CRM authorization pending: your medical registration has not been "
                    "verified by the domain pipeline yet. Contact your administrator."
                ),
            )

        if not profile.is_authorized():
            logger.warning(
                "Domain authorization denied: DoctorProfile for sub=%s has crm_verified=False.",
                sub,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"CRM not verified: your medical registration "
                    f"(CRM {profile.crm.crm_numero}/{profile.crm.crm_uf}) is pending "
                    f"CFM validation. Contact your administrator."
                ),
            )

        # Both phases passed — construct the ACL token from domain-sourced CRM data.
        return ValidatedUserToken(
            sub=sub,
            email=payload.get("email", ""),
            preferred_username=payload.get("preferred_username", ""),
            roles=roles,
            # WHY: CRM fields populated from DoctorProfile (domain store), not JWT payload.
            crm_numero=profile.crm.crm_numero,
            crm_uf=profile.crm.crm_uf,
            exp=payload.get("exp"),
        )

    except HTTPException:
        # Re-raise domain 403 raised above without wrapping it in a 401.
        raise

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Invalid token: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except InvalidTokenFormatError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token validation failed: {e}",
            headers={"WWW-Authenticate": "Bearer"},
        )
