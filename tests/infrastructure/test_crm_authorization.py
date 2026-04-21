"""Infrastructure tests: CRM domain authorization gate.

WHY (TDD Red phase): These tests verify Epic 2 — the DDD contract that separates
Keycloak Identity (password authentication) from Domain Authorization (CRM verification).

Key behavioral contracts:
1. A valid JWT for a user WITHOUT a DoctorProfile → 403 Forbidden (not 401).
2. A valid JWT for a user WITH a DoctorProfile but crm_verified=False → 403 Forbidden.
3. A valid JWT for a user WITH a verified DoctorProfile (crm_verified=True) → 200 OK.
4. The CRM is NEVER read from the JWT payload (no payload.get('crm_numero')).
5. The 403 reason distinguishes "domain not authorized" from "bad credentials".
"""
import pytest
from unittest.mock import MagicMock, patch
from fastapi import HTTPException


def _make_jwt_payload(sub: str = "kc-uuid-test") -> dict:
    """Canonical JWT payload — deliberately WITHOUT crm_numero/crm_uf claims.

    WHY: After this refactoring, the domain NO LONGER trusts CRM data from the JWT.
    Any test that requires crm_numero in the payload is testing the OLD (wrong) behavior.
    """
    return {
        "sub": sub,
        "email": "doctor@gercon.com",
        "preferred_username": "dr_test",
        "realm_access": {"roles": ["diretor_medico"]},
        "exp": 9999999999,
        # Intentionally absent: "crm_numero", "crm_uf"
    }


def _make_mock_signing_key() -> MagicMock:
    key = MagicMock()
    key.key = MagicMock()
    return key


class TestCRMAuthorizationGate:
    """Contract: Domain authorization is independent of JWT claims."""

    def test_valid_jwt_with_no_doctor_profile_raises_403(self):
        """Identity OK (valid JWT) + Domain FAIL (no DoctorProfile) = 403 Forbidden.

        This is the key DDD invariant: Keycloak confirms WHO you are,
        the Domain confirms if you are AUTHORIZED to practice medicine here.
        """
        from infrastructure.auth.jwt_validator import verify_token

        with patch("infrastructure.auth.jwt_validator.jwks_client") as mock_client:
            mock_client.get_signing_key_from_jwt.return_value = _make_mock_signing_key()
            with patch("jwt.get_unverified_header", return_value={"kid": "test-kid"}):
                with patch("jwt.decode", return_value=_make_jwt_payload(sub="kc-no-profile")):
                    with patch(
                        "infrastructure.auth.jwt_validator._lookup_doctor_profile",
                        return_value=None,  # No DoctorProfile found in DB
                    ):
                        with pytest.raises(HTTPException) as exc_info:
                            verify_token("valid.jwt.token")

        assert exc_info.value.status_code == 403, (
            "Missing DoctorProfile must yield 403 (unauthorized), not 401 (unauthenticated)"
        )
        assert "crm" in exc_info.value.detail.lower() or "authorized" in exc_info.value.detail.lower()

    def test_valid_jwt_with_unverified_crm_raises_403(self):
        """Identity OK + Domain FAIL (crm_verified=False) = 403 Forbidden.

        The user registered but CFM validation has not completed yet.
        Their password is correct (401 won't fire), but their CRM is not domain-verified.
        """
        from infrastructure.auth.jwt_validator import verify_token
        from domain.identity import DoctorProfile, MedicalCouncilRegistration

        unverified_profile = DoctorProfile(
            user_id="kc-uuid-unverified",
            crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
            crm_verified=False,  # CFM validation pending
        )

        with patch("infrastructure.auth.jwt_validator.jwks_client") as mock_client:
            mock_client.get_signing_key_from_jwt.return_value = _make_mock_signing_key()
            with patch("jwt.get_unverified_header", return_value={"kid": "test-kid"}):
                with patch("jwt.decode", return_value=_make_jwt_payload(sub="kc-uuid-unverified")):
                    with patch(
                        "infrastructure.auth.jwt_validator._lookup_doctor_profile",
                        return_value=unverified_profile,
                    ):
                        with pytest.raises(HTTPException) as exc_info:
                            verify_token("valid.jwt.token")

        assert exc_info.value.status_code == 403
        assert "crm" in exc_info.value.detail.lower() or "verified" in exc_info.value.detail.lower()

    def test_valid_jwt_with_verified_crm_returns_token(self):
        """Identity OK + Domain OK (crm_verified=True) = ValidatedUserToken returned.

        The happy path: a properly registered and CFM-validated doctor.
        """
        from infrastructure.auth.jwt_validator import verify_token
        from infrastructure.auth.token_acl import ValidatedUserToken
        from domain.identity import DoctorProfile, MedicalCouncilRegistration

        verified_profile = DoctorProfile(
            user_id="kc-uuid-verified",
            crm=MedicalCouncilRegistration(crm_numero="98765", crm_uf="SP"),
            crm_verified=True,
        )

        with patch("infrastructure.auth.jwt_validator.jwks_client") as mock_client:
            mock_client.get_signing_key_from_jwt.return_value = _make_mock_signing_key()
            with patch("jwt.get_unverified_header", return_value={"kid": "test-kid"}):
                with patch("jwt.decode", return_value=_make_jwt_payload(sub="kc-uuid-verified")):
                    with patch(
                        "infrastructure.auth.jwt_validator._lookup_doctor_profile",
                        return_value=verified_profile,
                    ):
                        result = verify_token("valid.jwt.token")

        assert isinstance(result, ValidatedUserToken)
        assert result.sub == "kc-uuid-verified"
        assert result.crm_numero == "98765"
        assert result.crm_uf == "SP"

    def test_crm_is_never_read_from_jwt_payload(self):
        """Structural contract: verify_token must NOT call payload.get('crm_numero').

        WHY: The CRM data in the JWT comes from Keycloak, which is the Identity Provider.
        Post-refactoring, the authoritative source of CRM truth is our DoctorProfile DB,
        not the JWT claim. This test fails if any code path reads crm from the token.
        """
        import ast
        import pathlib

        validator_src = pathlib.Path(
            __file__
        ).parent.parent.parent / "src/infrastructure/auth/jwt_validator.py"
        source_code = validator_src.read_text()

        assert 'payload.get("crm_numero")' not in source_code, (
            "ARCHITECTURAL VIOLATION: jwt_validator.py must not read crm_numero from JWT payload. "
            "CRM authorization must come from DoctorProfile DB lookup, not from Keycloak claims."
        )
        assert "payload.get('crm_numero')" not in source_code, (
            "ARCHITECTURAL VIOLATION: jwt_validator.py must not read crm_numero from JWT payload."
        )


class TestValidatedUserTokenContract:
    """Contract: ValidatedUserToken carries domain-resolved CRM, not raw JWT claims."""

    def test_token_crm_fields_are_optional_strings(self):
        """crm_numero and crm_uf must remain on the model for downstream consumers.

        WHY: The adapter outputs ValidatedUserToken to presentation-layer consumers
        (Streamlit, FastAPI routes) that display CRM info. The fields must exist,
        but they are now populated from DoctorProfile, not from JWT payload.
        """
        from infrastructure.auth.token_acl import ValidatedUserToken

        token = ValidatedUserToken(
            sub="kc-uuid-test",
            email="doc@gercon.com",
            preferred_username="dr_doc",
            roles=["diretor_medico"],
            exp=9999999999,
            crm_numero="12345",
            crm_uf="RS",
        )
        assert token.crm_numero == "12345"
        assert token.crm_uf == "RS"
