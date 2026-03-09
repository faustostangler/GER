import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(".env/creds.env")
USER = os.getenv("username")
PASS = os.getenv("password")

def main():
    print("Testando acesso direto à URL do detalhe da solicitação...")
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
            
        print("Acessando Menu Fila de Espera para carregar o state inicial...")
        xpath_item = "/html/body/div[6]/div/ul/li[4]"
        page.wait_for_selector(f"xpath={xpath_item}", timeout=15000)
        page.locator(f"xpath={xpath_item}").click()
        page.wait_for_load_state("networkidle")
        page.wait_for_selector("table.ng-table tbody tr", timeout=15000)
        
        # Pega o primeiro ID
        ids = page.evaluate("""() => {
            let el = document.querySelectorAll('table.ng-table tbody tr.ng-scope');
            let result = [];
            for (let i = 0; i < el.length; i++) {
                try {
                    result.push(angular.element(el[i]).scope().solicitacao.id);
                } catch(e) {}
            }
            return result;
        }""")
        
        if not ids:
            print("Nenhum ID encontrado.")
            return

        req_id = ids[0]
        print(f"ID escolhido: {req_id}. Acessando URL direta...")
        
        # Cria nova aba
        worker_page = context.new_page()
        url = f"https://gercon.procempa.com.br/gerconweb/#/painelSolicitante/detalheSolicitacao?idSolicitacao={req_id}"
        
        worker_page.goto(url, wait_until="networkidle")
        worker_page.wait_for_timeout(5000)  # Espera genérica para ver o que carrega
        
        html = worker_page.content()
        with open("direct_url_dom.html", "w", encoding="utf-8") as f:
            f.write(html)
            
        print("DOM salvo em direct_url_dom.html")
        browser.close()

if __name__ == "__main__":
    main()
