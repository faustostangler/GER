import sqlite3
import json
import time
import logging
from typing import List, Dict, Any
from application.use_cases.scraper_interfaces import IRawDataRepository, IIngestionLogRepository
from domain.models import IngestionLogEntry

logger = logging.getLogger(__name__)


class SQLiteRawRepository(IRawDataRepository, IIngestionLogRepository):
    def __init__(self, db_file: str = "gercon_raw_data.db"):
        self.db_file = db_file

    def init_db(self):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS solicitacoes_raw (
                protocolo TEXT PRIMARY KEY,
                data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_alteracao INTEGER,
                conteudo_json TEXT,
                origem_lista TEXT
            )
        """)
        try:
            cursor.execute(
                "ALTER TABLE solicitacoes_raw ADD COLUMN data_alteracao INTEGER"
            )
        except Exception:
            pass
        conn.commit()
        conn.close()

    def get_watermark(self, chave: str) -> int:
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MAX(data_alteracao) FROM solicitacoes_raw WHERE origem_lista = ?",
                (chave,),
            )
            result = cursor.fetchone()
            conn.close()
            return result[0] if result and result[0] else 0
        except Exception as e:
            logger.warning(f"Erro ao consultar watermark para '{chave}': {e}")
            return 0

    def save_raw_batch(self, jsons: List[Dict[str, Any]], origem: str):
        if not jsons:
            return
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        data_to_insert = []
        for j in jsons:
            if not j or "error" in j:
                continue
            prot = str(j.get("numeroCMCE", "SEM_PROTOCOLO_" + str(time.time())))
            data_alt = j.get("dataAlterouUltimaSituacao", 0)
            data_to_insert.append(
                (prot, data_alt, json.dumps(j, ensure_ascii=False), origem)
            )

        cursor.executemany(
            """
            INSERT OR REPLACE INTO solicitacoes_raw (protocolo, data_alteracao, conteudo_json, origem_lista)
            VALUES (?, ?, ?, ?)
        """,
            data_to_insert,
        )
        conn.commit()
        conn.close()

    # ==========================================
    # AUDIT LOG: IIngestionLogRepository
    # ==========================================

    def init_log_table(self):
        """Cria tabela técnica de auditoria para Post-Mortem de ingestão."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                duration_seconds REAL NOT NULL,
                status TEXT NOT NULL,
                items_ingested INTEGER DEFAULT 0,
                items_failed INTEGER DEFAULT 0,
                bytes_processed INTEGER DEFAULT 0,
                target_lists TEXT DEFAULT '[]',
                error_message TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def log_execution(self, entry: IngestionLogEntry):
        """Persiste um registro de auditoria para cada ciclo de scraping."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO ingestion_logs
                (timestamp, duration_seconds, status, items_ingested, items_failed, bytes_processed, target_lists, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry.timestamp,
                entry.duration_seconds,
                entry.status.value,
                entry.items_ingested,
                entry.items_failed,
                entry.bytes_processed,
                json.dumps(entry.target_lists),
                entry.error_message,
            ),
        )
        conn.commit()
        conn.close()

    def get_last_entries(self, limit: int = 10) -> list:
        """Retorna as últimas N execuções para análise de tendência."""
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT timestamp, duration_seconds, status, items_ingested, items_failed, bytes_processed, target_lists, error_message FROM ingestion_logs ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = cursor.fetchall()
        conn.close()
        return [
            {
                "timestamp": r[0],
                "duration_seconds": r[1],
                "status": r[2],
                "items_ingested": r[3],
                "items_failed": r[4],
                "bytes_processed": r[5],
                "target_lists": json.loads(r[6]),
                "error_message": r[7],
            }
            for r in rows
        ]
