"""
TDD: Infrastructure Adapter Tests for SQLiteRawRepository (Ingestion Log).

Valida o ciclo completo de persistência e leitura do audit log
usando SQLite em modo :memory: para isolamento total do teste.
"""
import time
import pytest
from src.infrastructure.repositories.sqlite_raw_repository import SQLiteRawRepository
from src.domain.models import IngestionLogEntry, IngestionStatus


@pytest.fixture
def repo(tmp_path):
    """Cria um repositório isolado com DB temporário."""
    db_file = str(tmp_path / "test_audit.db")
    r = SQLiteRawRepository(db_file=db_file)
    r.init_db()
    r.init_log_table()
    return r


class TestIngestionLogTable:
    """Verifica a criação e integridade da tabela ingestion_logs."""

    def test_init_log_table_is_idempotent(self, repo):
        """Chamar init_log_table múltiplas vezes não deve gerar erro."""
        repo.init_log_table()
        repo.init_log_table()
        entries = repo.get_last_entries()
        assert entries == []

    def test_log_execution_persists_entry(self, repo):
        entry = IngestionLogEntry(
            timestamp=time.time(),
            duration_seconds=30.0,
            status=IngestionStatus.SUCCESS,
            items_ingested=200,
            items_failed=0,
            bytes_processed=512000,
            target_lists=["aguardando_vaga"],
            error_message="",
        )
        repo.log_execution(entry)
        entries = repo.get_last_entries(limit=1)
        assert len(entries) == 1
        assert entries[0]["status"] == "SUCCESS"
        assert entries[0]["items_ingested"] == 200

    def test_log_execution_preserves_target_lists_as_json(self, repo):
        entry = IngestionLogEntry(
            timestamp=1.0,
            duration_seconds=5.0,
            status=IngestionStatus.PARTIAL,
            target_lists=["lista_a", "lista_b", "lista_c"],
        )
        repo.log_execution(entry)
        stored = repo.get_last_entries(limit=1)[0]
        assert stored["target_lists"] == ["lista_a", "lista_b", "lista_c"]

    def test_get_last_entries_respects_limit(self, repo):
        for i in range(5):
            entry = IngestionLogEntry(
                timestamp=float(i),
                duration_seconds=float(i),
                status=IngestionStatus.SUCCESS,
                items_ingested=i * 10,
            )
            repo.log_execution(entry)

        assert len(repo.get_last_entries(limit=3)) == 3
        assert len(repo.get_last_entries(limit=10)) == 5

    def test_get_last_entries_returns_newest_first(self, repo):
        for i in range(3):
            entry = IngestionLogEntry(
                timestamp=float(i + 1),
                duration_seconds=1.0,
                status=IngestionStatus.SUCCESS,
                items_ingested=(i + 1) * 100,
            )
            repo.log_execution(entry)

        entries = repo.get_last_entries(limit=3)
        # Newest (id=3) should come first
        assert entries[0]["items_ingested"] == 300
        assert entries[2]["items_ingested"] == 100

    def test_circuit_breaker_status_persists_error_message(self, repo):
        entry = IngestionLogEntry(
            timestamp=time.time(),
            duration_seconds=120.0,
            status=IngestionStatus.CIRCUIT_BREAKER,
            items_failed=50,
            error_message="API do Vendor Quebrou Radicalmente com índice de 7.0% Poison Pills!",
        )
        repo.log_execution(entry)
        stored = repo.get_last_entries(limit=1)[0]
        assert stored["status"] == "CIRCUIT_BREAKER"
        assert "Poison Pills" in stored["error_message"]

    def test_failure_status_with_zero_items(self, repo):
        entry = IngestionLogEntry(
            timestamp=time.time(),
            duration_seconds=2.0,
            status=IngestionStatus.FAILURE,
            items_ingested=0,
            items_failed=0,
            error_message="Login failed",
        )
        repo.log_execution(entry)
        stored = repo.get_last_entries(limit=1)[0]
        assert stored["status"] == "FAILURE"
        assert stored["items_ingested"] == 0
