import uuid
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker

# Add the 'src' path resolution for tests
import sys
from pathlib import Path
src_path = str(Path(__file__).resolve().parent.parent.parent / "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# 1. Setup Test DB
TEST_DB_URL = "sqlite:///test_crm_auth_e2e.db"
engine = create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

# 2. Force Patch modules
# WHY (E402): These imports MUST come after sys.path manipulation and module
# patching to ensure the test SQLite engine replaces the production session.
import infrastructure.database.session  # noqa: E402
infrastructure.database.session.SessionLocal = SessionLocal
infrastructure.database.session.engine = engine

import infrastructure.repositories.doctor_profile_repository  # noqa: E402
infrastructure.repositories.doctor_profile_repository.SessionLocal = SessionLocal

from infrastructure.database.models import Base  # noqa: E402
from infrastructure.repositories.doctor_profile_repository import SQLDoctorProfileRepository  # noqa: E402
from infrastructure.events.keycloak_kafka_consumer import _process_register_event  # noqa: E402
from infrastructure.auth.jwt_validator import verify_token, IdentityContractViolationException  # noqa: E402
import infrastructure.auth.jwt_validator as jwt_validator  # noqa: E402

# We'll use the pytest-asyncio to run async tests
pytestmark = pytest.mark.asyncio

@pytest.fixture(scope="module", autouse=True)
def setup_teardown_db():
    """Ensure the DB tables are created and truncated before/after the integration test."""
    # Ensure tables exist
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    
    # Verify table existence
    with engine.connect() as conn:
        result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table' AND name='doctor_profiles'"))
        if not result.fetchone():
            raise RuntimeError("Table 'doctor_profiles' was not created!")
    
    yield
    
    # Cleanup DB after tests
    try:
        import os
        if os.path.exists("test_crm_auth_e2e.db"):
            os.remove("test_crm_auth_e2e.db")
    except Exception:
        pass
    
    # Reset the singleton
    jwt_validator._doctor_profile_repo = None


async def test_e2e_crm_authorization_flow():
    """
    E2E Integration Test: Zero-Trust CRM Authorization
    """
    user_id = str(uuid.uuid4())
    crm_numero = "99999"
    crm_uf = "SP"
    
    mock_event = {
        "id": str(uuid.uuid4()),
        "type": "REGISTER",
        "userId": user_id,
        "details": {
            "crm_numero": crm_numero,
            "crm_uf": crm_uf
        }
    }
    
    # Reset singleton
    jwt_validator._doctor_profile_repo = None
    
    # Simulate Consumer processing the event
    repo = SQLDoctorProfileRepository()
    repo.SessionLocal = SessionLocal # Force it
    
    mock_cfm_client = AsyncMock()
    mock_cfm_client.validate.return_value = True
    
    # Patch idempotency to avoid Redis
    with patch("infrastructure.events.keycloak_kafka_consumer.is_already_processed", return_value=False):
        await _process_register_event(mock_event, repo, mock_cfm_client)
    
    # Verify the repository actually persisted the data
    profile = repo.find_by_user_id(user_id)
    assert profile is not None, "DoctorProfile was not saved to the database."
    assert profile.crm.crm_numero == crm_numero
    assert profile.crm.crm_uf == crm_uf
    assert profile.crm_verified is True
    
    # 3. Simulate an API request with a JWT token
    mock_jwt_payload = {
        "sub": user_id,
        "email": "doctor@example.com",
        "preferred_username": "dr_john",
        "realm_access": {"roles": ["doctor"]},
        "exp": 9999999999,
    }
    
    with patch("infrastructure.auth.jwt_validator.jwks_client.get_signing_key_from_jwt") as mock_jwks, \
         patch("infrastructure.auth.jwt_validator.jwt.decode") as mock_jwt_decode:
        
        mock_jwks.return_value = MagicMock(key="fake_key")
        mock_jwt_decode.return_value = mock_jwt_payload
        
        # 4. Call verify_token
        validated_token = verify_token("fake.jwt.token")
        
        # 5. Access is GRANTED
        assert validated_token.sub == user_id
        assert validated_token.crm_numero == crm_numero
        assert validated_token.crm_uf == crm_uf
        assert validated_token.email == "doctor@example.com"


async def test_e2e_crm_authorization_blocked_for_unverified_user():
    """
    E2E Integration Test: Zero-Trust CRM Authorization (Blocked)
    """
    unverified_user_id = str(uuid.uuid4())
    
    mock_jwt_payload = {
        "sub": unverified_user_id,
        "email": "hacker@example.com",
        "crm_numero": "00000", # Fake CRM injected in JWT
        "exp": 9999999999,
    }
    
    # Reset singleton
    jwt_validator._doctor_profile_repo = None
    
    with patch("infrastructure.auth.jwt_validator.jwks_client.get_signing_key_from_jwt") as mock_jwks, \
         patch("infrastructure.auth.jwt_validator.jwt.decode") as mock_jwt_decode:
        
        mock_jwks.return_value = MagicMock(key="fake_key")
        mock_jwt_decode.return_value = mock_jwt_payload
        
        # The verify_token should raise IdentityContractViolationException
        with pytest.raises(IdentityContractViolationException) as exc_info:
            verify_token("fake.jwt.token")
        
        assert exc_info.value.user_id == unverified_user_id
        assert exc_info.value.crm_raw == "00000"
        assert "CRM não verificado ou inexistente" in str(exc_info.value)
