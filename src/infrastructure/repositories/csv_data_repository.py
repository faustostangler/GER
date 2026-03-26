import os
import csv
import logging
from typing import Dict, Any
from src.application.use_cases.scraper_interfaces import IProcessedDataRepository
from src.domain.solicitacao_mapper import COLUNAS

logger = logging.getLogger(__name__)

class CsvDataRepository(IProcessedDataRepository):
    def _get_filename(self, collection_name: str) -> str:
        return f"dados_gercon_{collection_name}.csv"

    def init_storage(self, collection_name: str):
        filename = self._get_filename(collection_name)
        if not os.path.exists(filename):
            logger.info(f"Criando novo arquivo CSV: {filename}")
            with open(filename, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
                writer.writeheader()

    def load_existing(self, collection_name: str) -> Dict[str, Any]:
        filename = self._get_filename(collection_name)
        existing = {}
        if os.path.exists(filename):
            try:
                with open(filename, mode='r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("Protocolo"):
                            existing[row["Protocolo"]] = row
            except Exception as e:
                logger.warning(f"Erro ao carregar CSV existente {filename}: {e}")
        return existing

    def save_all(self, data_dict: Dict[str, Any], collection_name: str):
        filename = self._get_filename(collection_name)
        temp_file = filename + ".tmp"
        try:
            with open(temp_file, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
                writer.writeheader()
                for row in data_dict.values():
                    writer.writerow(row)
            os.replace(temp_file, filename)
        except Exception as e:
            logger.error(f"Erro crítico ao salvar CSV {filename}: {e}")
