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
            
        print("Teste de Injector para Paginacao...")
        
        js_script = """async () => {
            let $http = angular.element(document.body).injector().get('$http');
            let query = '/gercon/rest/solicitacoes/paineis?cancelaAgendamento=false&ordenacao=%2BdiasNaFilaDeEspera&pagina=1&possuiDita=TODAS&provisoria=false&situacao=AGUARDA_REAVALIACAO&situacao=AGUARDA_REGULACAO&situacao=AGUARDA_REVERSAO&situacao=AUTORIZADA&situacao=AUTORIZACAO_AUTOMATICA&situacao=ENCAMINHADA_AO_NIR&situacao=SOLICITADA&tamanhoPagina=10';
            let r = await $http.get(query);
            return r.data;
        }"""
        
        data = page.evaluate(js_script)
        
        with open("amostra_paginacao.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            
        print("Salvo em amostra_paginacao.json")
        browser.close()

if __name__ == "__main__":
    main()
