from abc import ABC, abstractmethod
from typing import List, Dict, Any

class IScraperClient(ABC):
    """Porta Driven (Saída) para a extração bruta de dados."""
    @abstractmethod
    def login(self) -> bool:
        pass
        
    @abstractmethod
    def select_unit(self) -> bool:
        pass

    @abstractmethod
    def fetch_batch(self, lista_chave: str, lista_nome: str, page_num: int, page_size: int, sort_order_js: str) -> Dict[str, Any]:
        """A API deve retornar um dicionário com:
        {
           'jsons': List[Dict],
           'totalDados': int,
           'bytesDownload': int,
           'error': str (opcional)
        }
        """
        pass

    @abstractmethod
    def close(self):
        pass

class IRawDataRepository(ABC):
    """Porta Driven para Salvar dados brutos (JSON) para Auditoria."""
    @abstractmethod
    def init_db(self):
        pass

    @abstractmethod
    def get_watermark(self, chave: str) -> int:
        pass
        
    @abstractmethod
    def save_raw_batch(self, jsons: List[Dict[str, Any]], origem: str):
        pass

class IProcessedDataRepository(ABC):
    """Porta Driven para Salvar dados processados e planificados (Ex: CSV/Parquet)."""
    @abstractmethod
    def init_storage(self, collection_name: str):
        pass
        
    @abstractmethod
    def load_existing(self, collection_name: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def save_all(self, data_dict: Dict[str, Any], collection_name: str):
        pass

class IScrapingUseCase(ABC):
    """Porta de Entrada (Driving) que aciona o fluxo."""
    @abstractmethod
    def execute_sync(self):
        pass
