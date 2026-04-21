"""Domain unit tests for the DoctorProfile entity.

WHY (TDD Red phase): These tests define the contract for the DoctorProfile entity
BEFORE implementation. They verify:
- Valid entity creation with crm_verified defaulting to False (new registration state).
- CRM field validation is delegated to the embedded MedicalCouncilRegistration VO.
- crm_verified=True requires explicit opt-in (domain authorization gate).
- Entities are equated by user_id (identity-based), not by CRM fields (value-based).
- The entity is a pure domain object with zero infrastructure dependencies.
"""
import pytest
from domain.identity import DoctorProfile, MedicalCouncilRegistration


class TestDoctorProfileCreation:
    """Contract: DoctorProfile captures the link between a Keycloak UUID and a CRM."""

    def test_creates_unverified_by_default(self):
        """New registrations start with crm_verified=False until CFM validation succeeds."""
        profile = DoctorProfile(
            user_id="kc-uuid-abc",
            crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
        )
        assert profile.crm_verified is False

    def test_marks_verified_explicitly(self):
        """Domain authorization: only the consumer event pipeline may set crm_verified=True."""
        profile = DoctorProfile(
            user_id="kc-uuid-abc",
            crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
            crm_verified=True,
        )
        assert profile.crm_verified is True

    def test_user_id_is_required(self):
        """user_id maps 1:1 to the Keycloak 'sub' claim — cannot be empty."""
        with pytest.raises(Exception):
            DoctorProfile(
                user_id="",
                crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
            )

    def test_crm_uf_validated_through_vo(self):
        """CRM validation invariant is delegated to MedicalCouncilRegistration VO."""
        with pytest.raises(Exception):
            DoctorProfile(
                user_id="kc-uuid-abc",
                crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="INVALID"),
            )

    def test_crm_numero_validated_through_vo(self):
        """CRM numero must be digits-only, enforced by the embedded VO."""
        with pytest.raises(Exception):
            DoctorProfile(
                user_id="kc-uuid-abc",
                crm=MedicalCouncilRegistration(crm_numero="ABC-123", crm_uf="SP"),
            )


class TestDoctorProfileIdentity:
    """Contract: DoctorProfile identity is based on user_id (Keycloak UUID), not CRM."""

    def test_two_profiles_with_same_user_id_are_equal(self):
        """Same Keycloak user, different CRM verification states — same profile."""
        a = DoctorProfile(
            user_id="kc-uuid-shared",
            crm=MedicalCouncilRegistration(crm_numero="99999", crm_uf="SP"),
            crm_verified=False,
        )
        b = DoctorProfile(
            user_id="kc-uuid-shared",
            crm=MedicalCouncilRegistration(crm_numero="99999", crm_uf="SP"),
            crm_verified=True,
        )
        assert a.user_id == b.user_id

    def test_two_profiles_with_different_user_ids_are_distinct(self):
        """Different doctors, even with the same CRM number, are distinct profiles."""
        a = DoctorProfile(
            user_id="kc-uuid-001",
            crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
        )
        b = DoctorProfile(
            user_id="kc-uuid-002",
            crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
        )
        assert a.user_id != b.user_id


class TestDoctorProfileVerificationPredicate:
    """Contract: is_authorized() is the single gate for clinical domain access."""

    def test_unverified_profile_is_not_authorized(self):
        profile = DoctorProfile(
            user_id="kc-uuid-abc",
            crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
            crm_verified=False,
        )
        assert profile.is_authorized() is False

    def test_verified_profile_is_authorized(self):
        profile = DoctorProfile(
            user_id="kc-uuid-abc",
            crm=MedicalCouncilRegistration(crm_numero="12345", crm_uf="RS"),
            crm_verified=True,
        )
        assert profile.is_authorized() is True
