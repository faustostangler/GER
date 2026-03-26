import os
import logging
from dotenv import load_dotenv

from src.infrastructure.adapters.playwright_scraper import PlaywrightGerconAdapter
from src.infrastructure.repositories.sqlite_raw_repository import SQLiteRawRepository
from src.infrastructure.repositories.csv_data_repository import CsvDataRepository
from src.application.use_cases.scraper_use_case import ScraperUseCase

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("master_scraper.log", encoding='utf-8'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

LISTAS_ALVO = [
    {"nome": "Agendadas e Confirmadas", "chave": "agendadas"},    
    {"nome": "Pendentes", "chave": "pendente"},     
    {"nome": "Expiradas", "chave": "cancelada"}, 
    {"nome": "Fila de Espera", "chave": "filaDeEspera"},   
    {"nome": "Outras", "chave": "outras"}
]

def main():
    logger.info("==================================================")
    logger.info("Iniciando Dependency Injection do Scraper (Hexagonal)")
    logger.info("==================================================")
    
    load_dotenv("env/creds.env")
    load_dotenv("env/config.env")
    
    user = os.getenv("username")
    pwd = os.getenv("password")
    url = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")
    headless = os.getenv("HEADLESS", "True").lower() == "true"
    page_size = int(os.getenv("PAGE_SIZE", "50"))
    
    if not user or not pwd:
        logger.error("Credenciais ausentes. Impossível instanciar Adapters.")
        return

    # 1. Instanciar Portas Driven (Adapters de Saída/Infra)
    scraper_client = PlaywrightGerconAdapter(
        username=user, 
        password=pwd, 
        url=url, 
        headless=headless
    )
    raw_repo = SQLiteRawRepository(db_file="gercon_raw_data.db")
    csv_repo = CsvDataRepository()
    
    # 2. Instanciar a Porta Driving (Use Case) com Injeção de Dependências
    use_case = ScraperUseCase(
        scraper_client=scraper_client,
        raw_repo=raw_repo,
        csv_repo=csv_repo,
        listas_alvo=LISTAS_ALVO,
        page_size=page_size
    )
    
    # 3. Executar Lógica Orquestrada
    try:
        use_case.execute_sync()
    except Exception as e:
        logger.error(f"Falha fatal no Use Case: {e}", exc_info=True)
        scraper_client.close()

if __name__ == "__main__":
    main()
