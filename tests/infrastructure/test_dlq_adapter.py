import sqlite3
import json
import pytest
from infrastructure.repositories.sqlite_raw_repository import SQLiteDLQRepository


@pytest.fixture
def dlq_repo(tmp_path):
    """Cria um repositório DLQ isolado."""
    db_file = str(tmp_path / "test_dlq.db")
    repo = SQLiteDLQRepository(db_file=db_file)
    repo.init_dlq_table()
    return repo


def test_dlq_persistence_saves_record(dlq_repo):
    """Verifica se um poison pill é persistido corretamente no SQLite."""
    payload = {"numeroCMCE": "CMD-666", "mock": True}
    error_msg = "Invalid Schema"
    target = "pendentes"

    dlq_repo.push_poison_pill(payload, error_msg, target)

    # Verifica diretamente no banco
    conn = sqlite3.connect(dlq_repo.db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT target_list, payload, error_message FROM dead_letter_queue")
    row = cursor.fetchone()
    conn.close()

    assert row is not None
    assert row[0] == target
    assert json.loads(row[1]) == payload
    assert row[2] == error_msg


def test_dlq_initialization_is_idempotent(dlq_repo):
    """Garante que reinicializar a tabela não causa erros."""
    dlq_repo.init_dlq_table()
    dlq_repo.init_dlq_table()
    # Se não estourou exceção, está OK
