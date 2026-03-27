import jwt
from jwt import PyJWKClient, PyJWKClientError
from fastapi import HTTPException, status
from src.infrastructure.config import settings
from src.infrastructure.auth.token_acl import ValidatedUserToken

class InvalidTokenFormatError(Exception):
    pass

# SRE FIX: 24h caching memory layer for JWKS
jwks_client = PyJWKClient(settings.jwks_url, cache_keys=True, lifespan=86400)

def verify_token(token: str) -> ValidatedUserToken:
    try:
        # Extract kid from unverified header
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        if not kid:
            raise InvalidTokenFormatError("Missing 'kid' in token header.")

        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token)
        except PyJWKClientError:
            # Fallback for Keycloak Key Rotation
            # Force a cache bypass/refresh
            global jwks_client
            jwks_client = PyJWKClient(settings.jwks_url, cache_keys=True, lifespan=86400)
            try:
                signing_key = jwks_client.get_signing_key_from_jwt(token)
            except PyJWKClientError as e:
                raise InvalidTokenFormatError(f"JWKS Fetch failed after fallback: {e}")

        # Strict decode
        payload = jwt.decode(
            token,
            key=signing_key.key,
            algorithms=["RS256"],
            audience=settings.KEYCLOAK_CLIENT_ID, # Confused Deputy prevention
            issuer=settings.keycloak_issuer
        )

        # Map realm_roles to extract roles if present
        realm_access = payload.get("realm_access", {})
        roles = realm_access.get("roles", [])

        # Map to ACL domain object
        return ValidatedUserToken(
            sub=payload.get("sub"),
            email=payload.get("email", ""),
            preferred_username=payload.get("preferred_username", ""),
            roles=roles,
            crm_numero=payload.get("crm_numero"),
            crm_uf=payload.get("crm_uf")
        )
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
