import time
import logging
from typing import Dict, Any, List
from playwright.sync_api import sync_playwright

from src.application.use_cases.scraper_interfaces import IScraperClient

logger = logging.getLogger(__name__)

class PlaywrightGerconAdapter(IScraperClient):
    def __init__(self, username: str, password: str, url: str, headless: bool = True, timeout: int = 30000):
        self.username = username
        self.password = password
        self.url = url
        self.headless = headless
        self.timeout = timeout
        
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None

    def login(self) -> bool:
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless, args=['--no-sandbox'])
        self.context = self.browser.new_context()
        self.page = self.context.new_page()

        self.page.goto(self.url, wait_until="networkidle")
        self.page.fill('#username', self.username)
        self.page.fill('#password', self.password)
        self.page.click('#kc-login')
        self.page.wait_for_load_state("networkidle")
        return True

    def select_unit(self) -> bool:
        try:
            xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
            self.page.wait_for_selector(f"xpath={xpath_btn}", timeout=self.timeout)
            self.page.locator(f"xpath={xpath_btn}").click()
            self.page.wait_for_load_state("networkidle")
            
            # Inicializa Angular Context
            xpath_item = "/html/body/div[6]/div/ul/li[1]"
            self.page.wait_for_selector(f"xpath={xpath_item}")
            self.page.locator(f"xpath={xpath_item}").click()
            self.page.wait_for_selector("table.ng-table tbody tr", timeout=self.timeout*10)
            return True
        except Exception as e:
            logger.warning(f"Erro ao selecionar unidade: {e}")
            return False

    def _navigate_to_tab(self, chave: str, nome: str) -> bool:
        selectors = [f"a[ng-click*=\"'{chave}'\"]", f"xpath=//a[contains(., '{nome}')]", f"xpath=//li[contains(., '{nome}')]"]
        for sel in selectors:
            if self.page.locator(sel).first.is_visible():
                self.page.locator(sel).first.click()
                self.page.wait_for_selector("table.ng-table tbody tr", timeout=self.timeout*10)
                self.page.wait_for_timeout(1000)
                return True
        return False

    def fetch_batch(self, lista_chave: str, lista_nome: str, page_num: int, page_size: int, sort_order_js: str) -> Dict[str, Any]:
        """Injeta JavaScript no Angular para obter os dados."""
        # Se precisar, clique na aba (já com cache lógico para não clicar à toa)
        self._navigate_to_tab(lista_chave, lista_nome)

        js_script = f"""async () => {{
            try {{
                if (typeof angular === 'undefined') return {{ error: "Angular não carregado" }};
                let table = document.querySelector('table.ng-table');
                if (!table) return {{ error: "Tabela não encontrada" }};
                let scope = angular.element(table).scope();
                let $http = angular.element(document.body).injector().get('$http');
                
                let origParams = scope.solicCtrl?.parametros?.['{lista_chave}'];
                if (!origParams) return {{ error: "Falta parâmetros '{lista_chave}'" }};
                
                let params = angular.copy(origParams);
                delete params.dataInicioConsulta; delete params.dataFimConsulta;
                delete params.dataInicioAlta; delete params.dataFimAlta;
                
                {sort_order_js}
                
                params.pagina = {page_num};
                params.tamanhoPagina = {page_size};
                
                let pageR = await $http.get('/gercon/rest/solicitacoes/paineis', {{ params: params }});
                if (!pageR.data?.dados?.length) return null;
                
                let ids = pageR.data.dados.map(i => i.id);
                let totalRegistros = pageR.data.totalDados || 0;
                let totalBytes = 0;
                
                let promises = ids.map(id => 
                    $http.get('/gercon/rest/solicitacoes/' + id, {{ transformResponse: [d => d] }})
                        .then(r => {{
                            totalBytes += new Blob([r.data || ""]).size;
                            return JSON.parse(r.data);
                        }})
                        .catch(e => ({{ error: id }}))
                );

                let timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('TIMEOUT_LOTE')), 240000));
                let results = await Promise.race([Promise.all(promises), timeout]);
                return {{ jsons: results, totalDados: totalRegistros, bytesDownload: totalBytes }};
            }} catch (e) {{ return {{ error: e.message || e.toString() }}; }}
        }}"""
        
        try:
            return self.page.evaluate(js_script)
        except Exception as e:
            return {"error": f"Execução JS falhou: {e}"}

    def close(self):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
