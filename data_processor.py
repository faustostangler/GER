import os
import glob
import hashlib
import re
import logging
from datetime import datetime
from typing import List, Dict, Optional, Any
from pathlib import Path

import pandas as pd
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- INFRASTRUCTURE: CONFIGURATION (Fail-fast) ---
class ProcessorSettings(BaseSettings):
    """
    Configurações centralizadas para o pipeline de processamento de dados do Gercon.
    Segue princípios de Twelve-Factor App e Pydantic Validation.
    """
    model_config = SettingsConfigDict(
        env_file=("env/creds.env", "env/config.env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    ANONYMIZATION_SALT: str = Field(default="secret_key", description="Salt for hashing PIls to ensure GDPR compliance")
    INPUT_PATTERN: str = Field(default="dados_gercon_*.csv", description="Pattern to find source csv files")
    OUTPUT_FILE: str = Field(default="gercon_consolidado.csv", description="Target file for cleaned data")
    LOG_FILE: str = Field(default="data_processor.log", description="Task persistent log file")
    LOG_LEVEL: str = Field(default="INFO")

settings = ProcessorSettings()

# --- INFRASTRUCTURE: LOGGING (Observability) ---
file_handler = logging.FileHandler(settings.LOG_FILE, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, settings.LOG_LEVEL.upper()))

logging.basicConfig(
    level=logging.DEBUG, # Let handlers filter
    format='%(asctime)s [%(levelname)s] (%(name)s): %(message)s',
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger("GerconProcessor")

# --- DOMAIN: DATA TYPES & SCHEMAS (Ubiquitous Language) ---
DTYPES_CARGA = {
    'Protocolo': str,
    'Teleconsulta': str,
    'Origem da Regulação': str,
    'Situação': str,
    'Complexidade': str,
    'Risco Cor': str,
    'Cartão SUS': str,
    'CPF': str,
    'CEP': str
}

COLUNAS_DATA = [
    "Data Solicitação", 
    "Data de Nascimento", 
    "Data do Cadastro"
]

PATTERN_EVOLUCAO = (
    r'\[(?P<Data_Evolucao>.*?)\s*\|\s*'
    r'(?P<Tipo_Informacao>.*?)\s*\|\s*'
    r'(?P<Origem_Informacao>.*?)\]:\s*'
    r'(?P<Texto_Evolucao>.*)'
)

# Lists for Anonymization as per DDD privacy patterns
IDENTIFICADORES_DIRETOS = [
    "Nome do Paciente", "CPF", "Cartão SUS", "Nome da Mãe", 
]

# --- APPLICATION: CORE SERVICES (The Engine) ---

class DataAnonymizer:
    """
    Service responsible for protecting domain integrity and patient data.
    Ensures that identifiable records are hashed before entering analytical layers.
    """
    def __init__(self, salt: str):
        self.salt = salt

    def hash_value(self, value: Any) -> Optional[str]:
        if pd.isna(value) or value == "":
            return None
        return hashlib.sha256(f"{value}{self.salt}".encode()).hexdigest()

    def process(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        logger.info(f"Anonymizing {len(columns)} sensitive columns...")
        for col in columns:
            if col in df.columns:
                df[col] = df[col].apply(self.hash_value)
        return df

class HistoryExploder:
    """
    Domain Logic for transforming raw clinical history into structured temporal records.
    Implements Red-Green-Refactor logic to prevent data loss in regex extractions.
    """
    def __init__(self, pattern: str):
        self.pattern = pattern

    def explode(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'Histórico Quadro Clínico' not in df.columns:
            logger.warning("Column 'Histórico Quadro Clínico' not found. Skipping explosion.")
            return df

        logger.info("Exploding clinical history strings into temporal records...")
        df_result = df.copy()
        
        # 1. Split robusto: Baseado no delimitador definido no Master Scraper
        # O separador ' | ' entre evoluções previne quebras internas nos textos de evolução.
        df_result['Histórico Quadro Clínico'] = (
            df_result['Histórico Quadro Clínico']
            .str.split(r'\s*\|\s*(?=\n\n\[)')
        )
        df_result = df_result.explode('Histórico Quadro Clínico')
        
        # 2. Sanitização (Sanity Check)
        df_result['Histórico Quadro Clínico'] = df_result['Histórico Quadro Clínico'].str.strip()
        
        # 3. Extração Hierárquica
        detalhes = df_result['Histórico Quadro Clínico'].str.extract(self.pattern, flags=re.DOTALL)
        
        # 4. Consolidating logic and dropping the original unstructured field
        df_final = pd.concat([df_result.drop(columns=['Histórico Quadro Clínico']), detalhes], axis=1)
        
        # 5. Robust Date Parsing (Handles legacy and new second-precision formats)
        df_final['Data_Evolucao'] = pd.to_datetime(
            df_final['Data_Evolucao'], 
            dayfirst=True, 
            errors='coerce'
        )
        
        return df_final

# --- APPLICATION: PIPELINE (Orchestrator/Hexagonal Port) ---

class GerconPipeline:
    """
    Main entry point for the GER data lifecycle.
    Orchestrates ingestion, cleaning, transformation and persistence.
    """
    def __init__(self):
        self.anonymizer = DataAnonymizer(settings.ANONYMIZATION_SALT)
        self.exploder = HistoryExploder(PATTERN_EVOLUCAO)

    def run(self):
        start_time = datetime.now()
        logger.info("Starting Gercon Data Transformation Pipeline (Modular Monolith Model)")

        # 1. Ingestion (Infrastructure Layer)
        df = self._load_data()
        if df.empty:
            logger.error("Pipeline failed: No data loaded.")
            return

        # 2. Memory & Type Optimization (DX Focused)
        df = self._optimize_types(df)

        # 3. Domain Quality Gate (Clean Protocol Rules)
        df = self._clean_duplicates(df)

        # 4. Use Case: Anonymization
        df = self.anonymizer.process(df, IDENTIFICADORES_DIRETOS)

        # 5. Use Case: History Explosion
        df = self.exploder.explode(df)

        # 6. Persistence/Egress (Infrastructure Adapter)
        self._export_data(df)

        duration = datetime.now() - start_time
        logger.info(f"Pipeline completed successfully in {duration.total_seconds():.2f}s")
        logger.info(f"Total processed records: {len(df)}")

    def _load_data(self) -> pd.DataFrame:
        arquivos = glob.glob(settings.INPUT_PATTERN)
        if not arquivos:
            logger.warning(f"No files matching {settings.INPUT_PATTERN} found.")
            return pd.DataFrame()

        dfs = []
        for arquivo in arquivos:
            try:
                logger.debug(f"Reading ingestion source: {arquivo}")
                df_temp = pd.read_csv(
                    arquivo,
                    encoding='utf-8',
                    quoting=1,
                    dtype=DTYPES_CARGA,
                    parse_dates=[c for c in COLUNAS_DATA if c in DTYPES_CARGA or True],
                    dayfirst=True,
                    low_memory=False
                )
                df_temp['source_file'] = Path(arquivo).name
                df_temp['ingestion_at'] = datetime.now()
                dfs.append(df_temp)
            except Exception as e:
                logger.error(f"Ingestion failure at {arquivo}: {e}")

        if not dfs:
            return pd.DataFrame()
        
        return pd.concat(dfs, ignore_index=True, sort=False)

    def _optimize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        # Categorical conversion for low cardinality to reduce memory footprint
        for col in ['Situação', 'Complexidade', 'Risco Cor']:
            if col in df.columns:
                df[col] = df[col].astype('category')
        return df

    def _clean_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'Protocolo' not in df.columns:
            return df
        
        # Protocol schema enforcement (Ubiquitous Language: must have '-')
        original_count = len(df)
        df = df[df['Protocolo'].str.contains('-', na=False)]
        df = df.drop_duplicates(subset=['Protocolo'], keep='last')
        
        removed = original_count - len(df)
        if removed > 0:
            logger.info(f"Data Quality Gate: Removed {removed} duplicated or invalid protocols.")
        return df

    def _export_data(self, df: pd.DataFrame):
        target = settings.OUTPUT_FILE
        logger.info(f"Exporting processed data to: {target}")
        try:
            df.to_csv(target, index=False, encoding='utf-8', quoting=1)
            logger.info("Persistency successful.")
        except Exception as e:
            logger.error(f"Egress failure: {e}")

if __name__ == "__main__":
    # Fail-fast execution with direct logging
    try:
        pipeline = GerconPipeline()
        pipeline.run()
    except Exception as fatal_e:
        logger.critical(f"Fatal System Error: {fatal_e}", exc_info=True)
        exit(1)
