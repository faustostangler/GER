import sqlite3
import json
import time
import logging
from typing import List, Dict, Any
from src.application.use_cases.scraper_interfaces import IRawDataRepository

logger = logging.getLogger(__name__)

class SQLiteRawRepository(IRawDataRepository):
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
            cursor.execute("ALTER TABLE solicitacoes_raw ADD COLUMN data_alteracao INTEGER")
        except Exception:
            pass
        conn.commit()
        conn.close()

    def get_watermark(self, chave: str) -> int:
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            cursor.execute("SELECT MAX(data_alteracao) FROM solicitacoes_raw WHERE origem_lista = ?", (chave,))
            result = cursor.fetchone()
            conn.close()
            return result[0] if result and result[0] else 0
        except Exception as e:
            logger.warning(f"Erro ao consultar watermark para '{chave}': {e}")
            return 0

    def save_raw_batch(self, jsons: List[Dict[str, Any]], origem: str):
        if not jsons: return
        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        data_to_insert = []
        for j in jsons:
            if not j or "error" in j: continue
            prot = str(j.get("numeroCMCE", "SEM_PROTOCOLO_" + str(time.time())))
            data_alt = j.get("dataAlterouUltimaSituacao", 0)
            data_to_insert.append((prot, data_alt, json.dumps(j, ensure_ascii=False), origem))
            
        cursor.executemany("""
            INSERT OR REPLACE INTO solicitacoes_raw (protocolo, data_alteracao, conteudo_json, origem_lista)
            VALUES (?, ?, ?, ?)
        """, data_to_insert)
        conn.commit()
        conn.close()
