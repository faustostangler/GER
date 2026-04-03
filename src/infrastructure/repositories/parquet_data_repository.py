import os
import logging
from typing import Dict, Any
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

import s3fs

from src.application.use_cases.scraper_interfaces import IProcessedDataRepository

logger = logging.getLogger(__name__)


class ParquetDataRepository(IProcessedDataRepository):
    """
    Renomeado para manter alinhamento com a Ubiquitous Language do ecossistema.
    Armazena e lê os dados diretamente em formato colunar (Parquet), compatível nativamente
    com o DuckDB sem necessidade de parse explícito como CSV.
    """

    def __init__(self, s3_bucket: str = None):
        self.s3_bucket = s3_bucket
        if self.s3_bucket:
            self.fs = s3fs.S3FileSystem(
                anon=False
            )  # Herda do IRSA do ambiente Node (Zero-Trust)

    def _get_path(self, collection_name: str) -> str:
        filename = f"dados_gercon_{collection_name}.parquet"
        if self.s3_bucket:
            return f"{self.s3_bucket.rstrip('/')}/{filename}"
        return filename

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def init_storage(self, collection_name: str):
        path = self._get_path(collection_name)

        # Cria um parquet inicial vazio caso não exista
        if self.s3_bucket:
            if not self.fs.exists(path):
                logger.info(f"S3 Object Missing. Provisionando novo S3 Parquet: {path}")
                empty_df = pd.DataFrame()
                with self.fs.open(path, "wb") as f:
                    empty_df.to_parquet(f, engine="pyarrow")
        else:
            if not os.path.exists(path):
                empty_df = pd.DataFrame()
                empty_df.to_parquet(path, engine="pyarrow")

    @retry(
        stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=2, max=8)
    )
    def load_existing(self, collection_name: str) -> Dict[str, Any]:
        path = self._get_path(collection_name)
        existing = {}

        try:
            if self.s3_bucket and self.fs.exists(path):
                with self.fs.open(path, "rb") as f:
                    df = pd.read_parquet(f, engine="pyarrow")
            elif not self.s3_bucket and os.path.exists(path):
                df = pd.read_parquet(path, engine="pyarrow")
            else:
                return existing

            # Restaura para os domínios mapeados dinamicamente via Pandas
            if not df.empty and "Protocolo" in df.columns:
                existing = df.set_index("Protocolo", drop=False).to_dict(orient="index")

        except Exception as e:
            logger.warning(
                f"Erro Transitório de Rede (S3) ao carregar {path}: {e}. Retentando via Tenacity..."
            )
            raise e

        return existing

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1.5, min=2, max=15)
    )
    def save_all(self, data_dict: Dict[str, Any], collection_name: str):
        path = self._get_path(collection_name)

        try:
            # Transformação estrita de dicionários python para dataframe colunar (Parquet)
            df = pd.DataFrame.from_dict(data_dict, orient="index")

            if self.s3_bucket:
                # Com S3, pandas + s3fs já gerenciam I/O nativo internamente mas forçamos open para controle
                with self.fs.open(path, "wb") as f:
                    df.to_parquet(f, engine="pyarrow", index=False)
            else:
                temp_path = path + ".tmp"
                df.to_parquet(temp_path, engine="pyarrow", index=False)
                os.replace(temp_path, path)  # Swap Atômico OS-Level (POSIX Sync)

        except Exception as e:
            logger.error(f"Erro Crítico de Rede ou Transação I/O Parquet ({path}): {e}")
            raise e
