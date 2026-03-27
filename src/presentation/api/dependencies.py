from fastapi import Depends, HTTPException, status, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, SecurityScopes
from src.infrastructure.auth.jwt_validator import verify_token
from src.infrastructure.auth.token_acl import ValidatedUserToken

security = HTTPBearer()

async def get_current_user(
    security_scopes: SecurityScopes,
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> ValidatedUserToken:
    
    if security_scopes.scopes:
        authenticate_value = f'Bearer scope="{security_scopes.scope_str}"'
    else:
        authenticate_value = "Bearer"

    try:
        token = credentials.credentials
        user = verify_token(token)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": authenticate_value},
        )

    # RBAC Validation
    for scope in security_scopes.scopes:
        if scope not in user.roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Not enough permissions. Required: {scope}",
                headers={"WWW-Authenticate": authenticate_value},
            )
            
    return user
