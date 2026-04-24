from unittest.mock import MagicMock, patch
from application.use_cases.scraper_use_case import ScraperUseCase
from application.use_cases.scraper_interfaces import (
    IScraperClient,
    IRawDataRepository,
    IProcessedDataRepository,
    IIngestionLogRepository,
    IDLQRepository,
)
from domain.models import IngestionStatus


# SRE FIX: Isolamento de Unidade (Bypass do Validador Pydantic)
@patch("application.use_cases.scraper_use_case.GerconPayloadContract")
def test_scraper_use_case_successful_execution(mock_contract):

    # 1. Força o Pydantic a aprovar o payload simulado (Bypass de Validação)
    mock_instance = MagicMock()
    mock_instance.model_dump.return_value = {
        "numeroCMCE": "CMD-001",
        "situacao": "PENDENTE",
    }

    # Cobre tanto a instanciação GerconPayloadContract(**data) quanto o model_validate(data)
    mock_contract.return_value = mock_instance
    mock_contract.model_validate.return_value = mock_instance

    # 2. Setup dos Mocks das Portas (Hexagonal)
    mock_scraper = MagicMock(spec=IScraperClient)
    mock_sqlite = MagicMock(spec=IRawDataRepository)
    mock_csv = MagicMock(spec=IProcessedDataRepository)
    mock_logger = MagicMock(spec=IIngestionLogRepository)
    mock_dlq = MagicMock(spec=IDLQRepository)

    mock_scraper.login.return_value = True

    valid_payload = {"numeroCMCE": "CMD-001", "situacao": "PENDENTE"}

    # Simulando a paginação (A página 2 vazia acionará o nosso novo SRE Break)
    mock_scraper.fetch_batch.side_effect = [
        {"jsons": [valid_payload], "totalDados": 1, "bytesDownload": 100},
        {"jsons": [], "totalDados": 1, "bytesDownload": 0},
    ]

    mock_sqlite.get_watermark.return_value = 0
    mock_csv.load_existing.return_value = {}

    listas_alvo = [{"chave": "fila_teste", "nome": "Fila de Teste"}]

    use_case = ScraperUseCase(
        scraper_client=mock_scraper,
        raw_repo=mock_sqlite,
        csv_repo=mock_csv,
        listas_alvo=listas_alvo,
        dlq_repo=mock_dlq,
        page_size=10,
        ingestion_log=mock_logger,
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


def test_scraper_use_case_circuit_breaker_and_dlq():
    """Valida se o Worker lida graciosamente com falhas catastróficas da API (Unhappy Path)."""
    from unittest.mock import MagicMock
    from application.use_cases.scraper_use_case import ScraperUseCase
    from application.use_cases.scraper_interfaces import (
        IScraperClient,
        IRawDataRepository,
        IProcessedDataRepository,
        IIngestionLogRepository,
    )
    from domain.models import IngestionStatus

    # 1. Setup
    mock_scraper = MagicMock(spec=IScraperClient)
    mock_sqlite = MagicMock(spec=IRawDataRepository)
    mock_csv = MagicMock(spec=IProcessedDataRepository)
    mock_logger = MagicMock(spec=IIngestionLogRepository)
    mock_dlq = MagicMock(spec=IDLQRepository)

    # 2. Injeta o Veneno (A API vai estourar um Timeout)
    mock_scraper.login.return_value = True
    mock_scraper.fetch_batch.side_effect = Exception("API HTTP 504 Gateway Timeout")

    use_case = ScraperUseCase(
        scraper_client=mock_scraper,
        raw_repo=mock_sqlite,
        csv_repo=mock_csv,
        listas_alvo=[{"chave": "fila_teste", "nome": "Fila Teste"}],
        dlq_repo=mock_dlq,
        page_size=10,
        ingestion_log=mock_logger,
    )

    # 3. Execução (Não deve estourar exceção para o SO, deve ser contido)
    use_case.execute_sync()

    # 4. Asserções (O log de ingestão DEVE registrar a falha)
    log_call_args = mock_logger.log_execution.call_args[0][0]
    assert log_call_args.status == IngestionStatus.FAILURE
    assert "API HTTP 504" in log_call_args.error_message


def test_scraper_dlq_persistence():
    """Valida se poison pills são enviadas para o repositório persistente (SQLite)."""
    from pydantic import ValidationError

    mock_scraper = MagicMock(spec=IScraperClient)
    mock_sqlite = MagicMock(spec=IRawDataRepository)
    mock_csv = MagicMock(spec=IProcessedDataRepository)
    mock_dlq = MagicMock(spec=IDLQRepository)

    mock_scraper.login.return_value = True
    poison_pill = {"numeroCMCE": "POISON-001", "situacao": "INVALIDA"}

    # 1. Faz o primeiro fetch retornar uma pílula envenenada
    mock_scraper.fetch_batch.side_effect = [
        {"jsons": [poison_pill], "totalDados": 1, "bytesDownload": 100},
        {"jsons": [], "totalDados": 1, "bytesDownload": 0},
    ]

    use_case = ScraperUseCase(
        scraper_client=mock_scraper,
        raw_repo=mock_sqlite,
        csv_repo=mock_csv,
        listas_alvo=[{"chave": "fila_poison", "nome": "Fila Poison"}],
        dlq_repo=mock_dlq,
    )

    # Simula falha de validação do Pydantic no loop
    with patch(
        "application.use_cases.scraper_use_case.GerconPayloadContract",
        side_effect=ValidationError.from_exception_data(
            title="TestError", line_errors=[]
        ),
    ):
        use_case.execute_sync()

    # Verifica se push_poison_pill foi chamado no repositório persistente
    mock_dlq.push_poison_pill.assert_called_once()
    args, kwargs = mock_dlq.push_poison_pill.call_args
    assert kwargs["payload"] == poison_pill
    assert kwargs["target_list"] == "fila_poison"
