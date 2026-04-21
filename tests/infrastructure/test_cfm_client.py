import pytest
from unittest.mock import AsyncMock, patch
from infrastructure.adapters.cfm_client import CFMClient
from infrastructure.config import settings
import httpx

@pytest.mark.asyncio
async def test_cfm_client_validate_success():
    """Test successful CRM validation with active status.
    
    WHY: Verifies the ACL adapter correctly parses the CFM API success response
    and returns True for an active registration.
    """
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = AsyncMock(
            status_code=200,
            json=lambda: {"status": "success", "active": True}
        )
        
        client = CFMClient()
        # The interface defined in interfaces.py is async
        result = await client.validate("12345", "SP")
        
        assert result is True
        mock_get.assert_called_once()
        
        # Verify URL construction and headers (Auth token)
        args, kwargs = mock_get.call_args
        url = str(args[0])
        assert "12345" in url
        assert "SP" in url
        
        token = settings.cfm.api_token
        expected_token = token.get_secret_value() if hasattr(token, "get_secret_value") else str(token)
        assert kwargs["headers"]["Authorization"] == f"Bearer {expected_token}"

@pytest.mark.asyncio
async def test_cfm_client_validate_inactive():
    """Test CRM validation for an inactive registration.
    
    WHY: Ensures the adapter returns False if the CFM API reports the doctor is inactive.
    """
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = AsyncMock(
            status_code=200,
            json=lambda: {"status": "success", "active": False}
        )
        
        client = CFMClient()
        result = await client.validate("12345", "SP")
        
        assert result is False

@pytest.mark.asyncio
async def test_cfm_client_validate_http_error():
    """Test CRM validation when API returns a 500 error.
    
    WHY: Verifies the adapter translates HTTP errors into a domain-appropriate ConnectionError.
    """
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.return_value = AsyncMock(status_code=500)
        
        client = CFMClient()
        with pytest.raises(ConnectionError, match="Falha na comunicação com a API do CFM"):
            await client.validate("12345", "SP")

@pytest.mark.asyncio
async def test_cfm_client_validate_timeout():
    """Test CRM validation on request timeout.
    
    WHY: Resilience check — timeouts should be caught and translated to ConnectionError
    to trigger the consumer's retry logic.
    """
    with patch("httpx.AsyncClient.get") as mock_get:
        mock_get.side_effect = httpx.TimeoutException("Timeout")
        
        client = CFMClient()
        with pytest.raises(ConnectionError, match="Timeout na API do CFM"):
            await client.validate("12345", "SP")
