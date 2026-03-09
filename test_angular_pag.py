import os
import json
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(".env/creds.env")
USER = os.getenv("username")
PASS = os.getenv("password")

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = browser.new_context()
        page = context.new_page()
        
        page.goto("https://gercon.procempa.com.br/gerconweb/", wait_until="networkidle")
        page.fill('#username', USER)
        page.fill('#password', PASS)
        page.click('#kc-login')
        page.wait_for_load_state("networkidle")
        
        try:
            xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
            page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
            page.locator(f"xpath={xpath_btn}").click()
            page.wait_for_load_state("networkidle")
        except:
            pass
            
        print("Acessando Menu Fila de Espera...")
        xpath_item = "/html/body/div[6]/div/ul/li[4]"
        page.locator(f"xpath={xpath_item}").click()
        page.wait_for_selector("table.ng-table tbody tr", timeout=15000)
        
        print("Pegando parametros do scope...")
        js_script = """async () => {
            let scope = angular.element(document.querySelector('table.ng-table')).scope();
            let $http = angular.element(document.body).injector().get('$http');
            
            // Fila de espera usa solicCtrl.parametros.filaDeEspera
            let params = angular.copy(scope.solicCtrl.parametros.filaDeEspera);
            params.pagina = 1;
            params.tamanhoPagina = 20; // Pegando 20 de uma vez para testar
            
            let queryUrl = '/gercon/rest/solicitacoes/paineis';
            let r = await $http.get(queryUrl, { params: params });
            return r.data;
        }"""
        
        try:
            data = page.evaluate(js_script)
            with open("amostra_angular_paginacao.json", "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print("Sucesso! Salvo em amostra_angular_paginacao.json")
        except Exception as e:
            print("Erro:", e)
            
        browser.close()

if __name__ == "__main__":
    main()
