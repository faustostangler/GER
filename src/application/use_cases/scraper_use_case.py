import time
import os
import json
import logging
from typing import Dict, Any, List

from src.application.use_cases.scraper_interfaces import IScrapingUseCase, IScraperClient, IRawDataRepository, IProcessedDataRepository
from src.domain.solicitacao_mapper import flatten_solicitacao, clean_data_row

logger = logging.getLogger(__name__)

class ScraperUseCase(IScrapingUseCase):
    def __init__(
        self, 
        scraper_client: IScraperClient, 
        raw_repo: IRawDataRepository, 
        csv_repo: IProcessedDataRepository,
        listas_alvo: List[Dict[str, str]],
        page_size: int = 50
    ):
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

    def execute_sync(self):
        logger.info("Iniciando Use Case de Scraper via Arquitetura Hexagonal...")
        self.raw_repo.init_db()
        
        if not self.scraper_client.login():
            logger.error("Falha no login do Scraper Client.")
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
            
            while not stop_scraping:
                logger.info(f"[{nome}] Buscando página {page_num}...")
                response_data = self.scraper_client.fetch_batch(chave, nome, page_num, self.page_size, sort_order_js)
                
                if not response_data:
                    logger.info(f"Fim da lista {nome}.")
                    if not list_state["full_sync_completed"]:
                        list_state["full_sync_completed"] = True
                        list_state["last_page"] = 1
                        global_state[chave] = list_state
                        self._save_global_state(global_state)
                    break
                    
                if "error" in response_data:
                    logger.error(f"Erro na extração: {response_data['error']}")
                    break
                    
                jsons = response_data.get("jsons", [])
                
                # Porta 1: Persistência Bruta (Auditoria/Raw)
                self.raw_repo.save_raw_batch(jsons, nome)
                
                novos = 0
                for j in jsons:
                    if j is None or "error" in j: continue
                    
                    if list_state["full_sync_completed"] and watermark > 0:
                        data_alt = j.get("dataAlterouUltimaSituacao", 0) or 0
                        if 0 < data_alt <= watermark:
                            stop_scraping = True
                            break
                            
                    # Porta Core: Lógica de Domínio
                    data = flatten_solicitacao(j, nome)
                    if data and data.get("Protocolo"):
                        records[data["Protocolo"]] = clean_data_row(data)
                        novos += 1
                        
                # Porta 2: Persistência Estruturada (CSV)
                self.csv_repo.save_all(records, chave)
                
                logger.info(f"Página {page_num} concluída com {novos} atualizações.")
                page_num += 1
                
                if not list_state["full_sync_completed"]:
                    list_state["last_page"] = page_num
                    global_state[chave] = list_state
                    self._save_global_state(global_state)

        self.scraper_client.close()
        logger.info("Use Case de Sincronização Finalizado.")
