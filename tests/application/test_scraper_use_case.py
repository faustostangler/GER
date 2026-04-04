import pytest
from unittest.mock import MagicMock
from application.use_cases.scraper_use_case import ScraperUseCase
from application.use_cases.scraper_interfaces import (
    IScraperAdapter,
    IRawDataRepository,
    IParquetRepository
)
from domain.models import IngestionLogEntry

def test_scraper_use_case_successful_execution():
    # 1. Setup dos Mocks das Portas (Hexagonal)
    mock_scraper = MagicMock(spec=IScraperAdapter)
    mock_sqlite = MagicMock(spec=IRawDataRepository)
    mock_parquet = MagicMock(spec=IParquetRepository)
    mock_logger = MagicMock() # Mock do Ingestion Logger
    
    # Simula que o scraper rodou e retornou algumas linhas
    mock_scraper.scrape_all_pages.return_value = [{"id": 1, "status": "ok"}]
    mock_sqlite.get_unprocessed_records.return_value = [{"id": 1, "status": "ok"}]
    
    use_case = ScraperUseCase(
        scraper=mock_scraper,
        raw_repo=mock_sqlite,
        parquet_repo=mock_parquet,
        ingestion_log=mock_logger
    )
    
    # 2. Execução (Bypass do loop assíncrono para testar o core)
    # Como execute_sync captura erros, validamos se ele não quebra
    use_case.execute_sync(limit=10)
    
    # 3. Asserções
    # Verifica se os métodos das portas foram chamados no pipeline correto
    mock_scraper.scrape_all_pages.assert_called_once()
    mock_sqlite.save_raw_data.assert_called()
    mock_parquet.export_to_parquet.assert_called_once()
    
    # Verifica se o log de auditoria registrou SUCESSO
    log_call_args = mock_logger.log_execution.call_args[0][0]
    assert log_call_args.status.value == "SUCCESS"
    assert log_call_args.items_ingested > 0
