from pydantic import BaseModel, Field
from typing import Optional, List

# TODO(SRE/Domain): O Token JWT com as claims de CRM deve ser tratado como um 
# DTE (Data Transfer Envelope). Atualmente aceitamos a claim 'crm_numero' do Keycloak.
# Implementação Futura: O FastAPI deve consumir o evento USER_REGISTERED via Kafka 
# (emitido pelo Keycloak Event Listener SPI), validar o CRM na base do CFM e criar 
# o 'DoctorProfile' local. O Keycloak cuida da senha, o Domínio cuida do status médico.
# Somente após isso, o token exigirá 'crm_verified=True'.

class ValidatedUserToken(BaseModel):
    sub: str = Field(..., description="UUID do usuário no Keycloak")
    email: str
    preferred_username: str
    roles: List[str] = Field(default_factory=list, description="RBAC Roles extracted from token")
    crm_numero: Optional[str] = None
    crm_uf: Optional[str] = None
