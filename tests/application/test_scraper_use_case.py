import pytest
from unittest.mock import MagicMock
from application.use_cases.scraper_use_case import ScraperUseCase
from application.use_cases.scraper_interfaces import (
    IScraperClient,
    IRawDataRepository,
    IProcessedDataRepository,
    IIngestionLogRepository
)
from domain.models import IngestionStatus

def test_scraper_use_case_successful_execution():
    # 1. Setup dos Mocks das Portas (Hexagonal)
    mock_scraper = MagicMock(spec=IScraperClient)
    mock_sqlite = MagicMock(spec=IRawDataRepository)
    mock_csv = MagicMock(spec=IProcessedDataRepository)
    mock_logger = MagicMock(spec=IIngestionLogRepository)
    
    # 2. Configurações dos Mocks
    mock_scraper.login.return_value = True
    
    # Simula dados sendo puxados (uma página com item e a segunda vazia)
    valid_payload = {
        "numeroCMCE": "CMD-001",
        "dataSolicitacao": "2026-04-10T00:00:00Z",
        "dataCadastro": "2026-04-10T00:00:00Z",
        "situacao": "PENDENTE"
    }
    
    mock_scraper.fetch_batch.side_effect = [
        {"jsons": [valid_payload], "totalDados": 1, "bytesDownload": 100},
        {"jsons": [], "totalDados": 0, "bytesDownload": 0}
    ]
    
    mock_sqlite.get_watermark.return_value = 0
    mock_csv.load_existing.return_value = {}
    
    listas_alvo = [{"chave": "fila_teste", "nome": "Fila de Teste"}]
    
    use_case = ScraperUseCase(
        scraper_client=mock_scraper,
        raw_repo=mock_sqlite,
        csv_repo=mock_csv,
        listas_alvo=listas_alvo,
        page_size=10,
        ingestion_log=mock_logger
    )
    
    # Executa sincronicamente
    use_case.execute_sync()
    
    # 3. Asserções
    mock_scraper.login.assert_called_once()
    mock_scraper.select_unit.assert_called_once()
    mock_sqlite.save_raw_batch.assert_called()
    mock_csv.save_all.assert_called()
    
    # Verifica o Audit Log
    log_call_args = mock_logger.log_execution.call_args[0][0]
    assert log_call_args.status == IngestionStatus.SUCCESS
    assert log_call_args.items_ingested > 0
