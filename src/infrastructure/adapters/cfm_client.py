import httpx
import logging
from application.use_cases.interfaces import ICFMClient
from infrastructure.config import settings

logger = logging.getLogger(__name__)

class CFMClient(ICFMClient):
    """Adapter for the CFM (Conselho Federal de Medicina) REST API.
    
    WHY (ACL): This class encapsulates all HTTP-specific logic, headers, and
    error translation. It protects the application layer from external API changes.
    """

    def __init__(self):
        self.base_url = str(settings.cfm.api_url).rstrip("/")
        # WHY: Handle both SecretStr (Pydantic) and plain str gracefully.
        token = settings.cfm.api_token
        self.token = (
            token.get_secret_value() if hasattr(token, "get_secret_value") else str(token)
        )
        self.timeout = settings.cfm.timeout

    async def validate(self, crm_numero: str, crm_uf: str) -> bool:
        """Validate CRM registration against the CFM API.
        
        Args:
            crm_numero: CRM registration number.
            crm_uf: Federation unit (2-letter uppercase).
            
        Returns:
            bool: True if registration is active.
            
        Raises:
            ConnectionError: If API is unreachable or returns an error.
        """
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }
        
        url = f"{self.base_url}/valida_crm?crm={crm_numero}&uf={crm_uf}"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url, headers=headers)
                
                if response.status_code != 200:
                    logger.error(f"CFM API returned error {response.status_code} for CRM {crm_numero}/{crm_uf}")
                    raise ConnectionError("Falha na comunicação com a API do CFM")
                
                data = response.json()
                # WHY: The 'active' field is the domain source of truth for verification.
                return data.get("active", False)
                
        except httpx.TimeoutException:
            logger.warning(f"Timeout reaching CFM API for CRM {crm_numero}/{crm_uf}")
            raise ConnectionError("Timeout na API do CFM")
        except httpx.RequestError as e:
            logger.error(f"Network error reaching CFM API: {e}")
            raise ConnectionError(f"Falha na comunicação com a API do CFM: {e}")
        except Exception as e:
            if isinstance(e, ConnectionError):
                raise
            logger.error(f"Unexpected error validating CRM {crm_numero}/{crm_uf}: {e}")
            raise ConnectionError(f"Erro inesperado na validação do CFM: {e}")
