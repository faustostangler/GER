from unittest.mock import MagicMock, patch
from application.use_cases.scraper_use_case import ScraperUseCase
from application.use_cases.scraper_interfaces import (
    IScraperClient,
    IRawDataRepository,
    IProcessedDataRepository,
    IIngestionLogRepository
)
from domain.models import IngestionStatus

# SRE FIX: Isolamento de Unidade (Bypass do Validador Pydantic)
@patch("application.use_cases.scraper_use_case.GerconPayloadContract")
def test_scraper_use_case_successful_execution(mock_contract):
    
    # 1. Força o Pydantic a aprovar o payload simulado (Bypass de Validação)
    mock_instance = MagicMock()
    mock_instance.model_dump.return_value = {"numeroCMCE": "CMD-001", "situacao": "PENDENTE"}
    
    # Cobre tanto a instanciação GerconPayloadContract(**data) quanto o model_validate(data)
    mock_contract.return_value = mock_instance
    mock_contract.model_validate.return_value = mock_instance

    # 2. Setup dos Mocks das Portas (Hexagonal)
    mock_scraper = MagicMock(spec=IScraperClient)
    mock_sqlite = MagicMock(spec=IRawDataRepository)
    mock_csv = MagicMock(spec=IProcessedDataRepository)
    mock_logger = MagicMock(spec=IIngestionLogRepository)

    mock_scraper.login.return_value = True

    valid_payload = {"numeroCMCE": "CMD-001", "situacao": "PENDENTE"}

    # Simulando a paginação (A página 2 vazia acionará o nosso novo SRE Break)
    mock_scraper.fetch_batch.side_effect = [
        {"jsons": [valid_payload], "totalDados": 1, "bytesDownload": 100},
        {"jsons": [], "totalDados": 1, "bytesDownload": 0}
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

    # 3. Asserções (Caminho Feliz Validado SRE)
    mock_scraper.login.assert_called_once()
    mock_scraper.select_unit.assert_called_once()
    mock_sqlite.save_raw_batch.assert_called()
    mock_csv.save_all.assert_called()

    # Verifica o Audit Log (O Sucesso finalmente é atingido!)
    log_call_args = mock_logger.log_execution.call_args[0][0]
    assert log_call_args.status == IngestionStatus.SUCCESS
