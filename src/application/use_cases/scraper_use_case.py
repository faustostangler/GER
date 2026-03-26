import os
import json
import time
from typing import Dict, Any, List

from src.application.use_cases.scraper_interfaces import IScrapingUseCase, IScraperClient, IRawDataRepository, IProcessedDataRepository
from src.domain.solicitacao_mapper import flatten_solicitacao, clean_data_row

from src.infrastructure.telemetry.logger import setup_structured_logger
from src.infrastructure.telemetry.metrics import (
    SCRAPER_PAGES_FETCHED, SCRAPER_ITEMS_SAVED, 
    SCRAPER_ERRORS_TOTAL, SCRAPER_DURATION_SECONDS
)
from src.infrastructure.telemetry.tracing import tracer

logger = setup_structured_logger("scraper_use_case")

class ScraperUseCase(IScrapingUseCase):
    def __init__(self, scraper_client: IScraperClient, raw_repo: IRawDataRepository, csv_repo: IProcessedDataRepository, listas_alvo: List[Dict[str, str]], page_size: int = 50):
        self.scraper_client = scraper_client
        self.raw_repo = raw_repo
        self.csv_repo = csv_repo
        self.listas_alvo = listas_alvo
        self.page_size = page_size
        self.state_file = "scraper_state.json"

    def _load_global_state(self) -> Dict[str, Any]:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except: pass
        return {}

    def _save_global_state(self, state: Dict[str, Any]):
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=4)

    @tracer.start_as_current_span("scraper_sync_execution")
    def execute_sync(self):
        logger.info("Initializing Hexagonal Scraper Use Case (With Telemetry)...")
        self.raw_repo.init_db()
        
        with tracer.start_as_current_span("vendor_login"):
            if not self.scraper_client.login():
                logger.error("Failed to login via Scraper Client.")
                SCRAPER_ERRORS_TOTAL.labels(error_type="LOGIN", target_list="ALL").inc()
                return
            self.scraper_client.select_unit()

        for lista in self.listas_alvo:
            nome = lista["nome"]
            chave = lista["chave"]
            
            global_state = self._load_global_state()
            list_state = global_state.get(chave, {"full_sync_completed": False, "last_page": 1})
            watermark = self.raw_repo.get_watermark(chave)
            
            sort_order_js = "params.ordenacao = ['dataAlterouUltimaSituacao'];"
            if list_state["full_sync_completed"]:
                sort_order_js = "params.ordenacao = ['-dataAlterouUltimaSituacao'];"
            
            self.csv_repo.init_storage(chave)
            records = self.csv_repo.load_existing(chave)
            
            page_num = list_state.get("last_page", 1)
            stop_scraping = False
            
            with tracer.start_as_current_span(f"list_sync_{chave}"):
                while not stop_scraping:
                    logger.info(f"Buscando página {page_num}...", extra={"lista": chave, "pagina": page_num})
                    
                    fetch_start = time.time()
                    response_data = self.scraper_client.fetch_batch(chave, nome, page_num, self.page_size, sort_order_js)
                    fetch_duration = time.time() - fetch_start
                    
                    # MÉTRICA: Tempo de Duração (P90)
                    SCRAPER_DURATION_SECONDS.labels(target_list=chave).observe(fetch_duration)
                    
                    if not response_data:
                        logger.info(f"Fim da lista atingido pacificamente.", extra={"lista": chave})
                        if not list_state["full_sync_completed"]:
                            list_state["full_sync_completed"] = True
                            list_state["last_page"] = 1
                            global_state[chave] = list_state
                            self._save_global_state(global_state)
                        break
                        
                    if "error" in response_data:
                        logger.error(f"Erro assíncrono durante fetch: {response_data['error']}", extra={"lista": chave, "pagina": page_num})
                        SCRAPER_ERRORS_TOTAL.labels(error_type="JS_FETCH", target_list=chave).inc()
                        break
                        
                    jsons = response_data.get("jsons", [])
                    
                    # MÉTRICA: Volume Paginado (Throughput)
                    SCRAPER_PAGES_FETCHED.labels(target_list=chave).inc()
                    
                    self.raw_repo.save_raw_batch(jsons, nome)
                    
                    novos = 0
                    with tracer.start_as_current_span(f"domain_mapper_loop"):
                        for j in jsons:
                            if j is None or "error" in j: 
                                SCRAPER_ERRORS_TOTAL.labels(error_type="PAYLOAD_CORRUPT", target_list=chave).inc()
                                continue
                            
                            # Condição de parada de Ingestão Incremental
                            if list_state["full_sync_completed"] and watermark > 0:
                                data_alt = j.get("dataAlterouUltimaSituacao", 0) or 0
                                if 0 < data_alt <= watermark:
                                    stop_scraping = True
                                    break
                                    
                            data = flatten_solicitacao(j, nome)
                            if data and data.get("Protocolo"):
                                records[data["Protocolo"]] = clean_data_row(data)
                                novos += 1
                                
                    # MÉTRICA: Pacientes convertidos e injetados com êxito
                    SCRAPER_ITEMS_SAVED.labels(target_list=chave).inc(novos)
                    self.csv_repo.save_all(records, chave)
                    
                    logger.info(f"Batch consumido com {novos} atualizações.", extra={"lista": chave, "pagina": page_num, "novos_itens": novos})
                    page_num += 1
                    
                    if not list_state["full_sync_completed"]:
                        list_state["last_page"] = page_num
                        global_state[chave] = list_state
                        self._save_global_state(global_state)

        self.scraper_client.close()
        logger.info("Processo principal de rede desativado e Scraper concluído.", extra={"status": "DONE"})
