import os
import csv
import logging
import io
from typing import Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

import s3fs
import boto3

from src.application.use_cases.scraper_interfaces import IProcessedDataRepository
from src.domain.solicitacao_mapper import COLUNAS

logger = logging.getLogger(__name__)

class CsvDataRepository(IProcessedDataRepository):
    def __init__(self, s3_bucket: str = None):
        # Se um bucket for injetado (Ex: s3://gercon-enterprise-data-lake-prod/), usa Rede. Senão, local.
        self.s3_bucket = s3_bucket
        if self.s3_bucket:
            # S3FS herda credenciais do Boto3 que herdam do IAM Role associada ao Pod (IRSA K8s)
            self.fs = s3fs.S3FileSystem(anon=False)

    def _get_path(self, collection_name: str) -> str:
        filename = f"dados_gercon_{collection_name}.csv"
        if self.s3_bucket:
            return f"{self.s3_bucket.rstrip('/')}/{filename}"
        return filename

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
    def init_storage(self, collection_name: str):
        path = self._get_path(collection_name)
        
        if self.s3_bucket:
            if not self.fs.exists(path):
                logger.info(f"S3 Object Missing. Provisionando novo S3 CSV: {path}")
                with self.fs.open(path, 'w', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
                    writer.writeheader()
        else:
            if not os.path.exists(path):
                with open(path, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
                    writer.writeheader()

    @retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=8))
    def load_existing(self, collection_name: str) -> Dict[str, Any]:
        path = self._get_path(collection_name)
        existing = {}
        
        try:
            if self.s3_bucket and self.fs.exists(path):
                with self.fs.open(path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("Protocolo"):
                            existing[row["Protocolo"]] = row
            elif not self.s3_bucket and os.path.exists(path):
                with open(path, mode='r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("Protocolo"):
                            existing[row["Protocolo"]] = row
        except Exception as e:
            logger.warning(f"Erro Transitório de Rede (S3) ao carregar {path}: {e}. Retentando via Tenacity...")
            raise e # Levanta para o tenacity pegar
            
        return existing

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=2, max=15))
    def save_all(self, data_dict: Dict[str, Any], collection_name: str):
        path = self._get_path(collection_name)
        temp_path = path + ".tmp"
        
        try:
            if self.s3_bucket:
                # 1. Escreve no S3 o arquivo temporário
                with self.fs.open(temp_path, 'w', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
                    writer.writeheader()
                    for row in data_dict.values():
                        writer.writerow(row)
                
                # 2. Swap atômico simulado (Copy/Delete no S3)
                self.fs.copy(temp_path, path)
                self.fs.rm(temp_path)
            else:
                with open(temp_path, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
                    writer.writeheader()
                    for row in data_dict.values():
                        writer.writerow(row)
                os.replace(temp_path, path)
                
        except Exception as e:
            logger.error(f"Erro Crítico de Rede ou Transação I/O no CSV {path}: {e}")
            raise e
