"""
TDD: Domain Model Tests for IngestionLogEntry and IngestionStatus.

Validates the Value Object's invariants, boundary conditions, and
Pydantic serialization for the audit trail domain model.
"""
import time
import pytest
from pydantic import ValidationError
from src.domain.models import IngestionLogEntry, IngestionStatus


class TestIngestionStatus:
    """Verifica a enumeração de status da ingestão."""

    def test_all_expected_values_exist(self):
        assert IngestionStatus.SUCCESS == "SUCCESS"
        assert IngestionStatus.PARTIAL == "PARTIAL"
        assert IngestionStatus.FAILURE == "FAILURE"
        assert IngestionStatus.CIRCUIT_BREAKER == "CIRCUIT_BREAKER"

    def test_status_count_is_exactly_four(self):
        """CDC Guard: Novos status devem ser adicionados conscientemente."""
        assert len(IngestionStatus) == 4


class TestIngestionLogEntry:
    """Verifica o Value Object de auditoria."""

    @pytest.fixture
    def valid_entry(self):
        return IngestionLogEntry(
            timestamp=time.time(),
            duration_seconds=42.5,
            status=IngestionStatus.SUCCESS,
            items_ingested=150,
            items_failed=3,
            bytes_processed=1024000,
            target_lists=["lista_a", "lista_b"],
            error_message="",
        )

    def test_valid_entry_creates_successfully(self, valid_entry):
        assert valid_entry.items_ingested == 150
        assert valid_entry.status == IngestionStatus.SUCCESS

    def test_required_fields_raise_on_missing(self):
        """timestamp, duration_seconds e status são obrigatórios."""
        with pytest.raises(ValidationError):
            IngestionLogEntry()

    def test_defaults_are_zero_for_optional_counters(self):
        entry = IngestionLogEntry(
            timestamp=1.0,
            duration_seconds=0.1,
            status=IngestionStatus.FAILURE,
        )
        assert entry.items_ingested == 0
        assert entry.items_failed == 0
        assert entry.bytes_processed == 0
        assert entry.target_lists == []
        assert entry.error_message == ""

    def test_circuit_breaker_status_with_error_message(self):
        entry = IngestionLogEntry(
            timestamp=1.0,
            duration_seconds=10.0,
            status=IngestionStatus.CIRCUIT_BREAKER,
            error_message="5% threshold exceeded",
        )
        assert entry.status == IngestionStatus.CIRCUIT_BREAKER
        assert "threshold" in entry.error_message

    def test_partial_status_signals_degraded_ingestion(self):
        entry = IngestionLogEntry(
            timestamp=1.0,
            duration_seconds=60.0,
            status=IngestionStatus.PARTIAL,
            items_ingested=100,
            items_failed=5,
        )
        assert entry.status == IngestionStatus.PARTIAL
        assert entry.items_failed > 0

    def test_serialization_roundtrip(self, valid_entry):
        """Garante que o model_dump → reconstrução funciona (para persistência SQLite)."""
        data = valid_entry.model_dump()
        reconstructed = IngestionLogEntry(**data)
        assert reconstructed == valid_entry

    @pytest.mark.parametrize(
        "field,invalid_value",
        [
            ("timestamp", "not-a-number"),
            ("duration_seconds", "abc"),
            ("items_ingested", "xyz"),
        ],
    )
    def test_type_coercion_rejects_garbage(self, field, invalid_value):
        """Rejeita dados corrompidos nos campos numéricos."""
        base = {
            "timestamp": 1.0,
            "duration_seconds": 1.0,
            "status": IngestionStatus.SUCCESS,
        }
        base[field] = invalid_value
        with pytest.raises(ValidationError):
            IngestionLogEntry(**base)
