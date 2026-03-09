import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import time

load_dotenv(".env/creds.env")
USER = os.getenv("username")
PASS = os.getenv("password")

def main():
    print("Testando window.open()...")
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
            
        xpath_item = "/html/body/div[6]/div/ul/li[4]"
        page.wait_for_selector(f"xpath={xpath_item}", timeout=15000)
        page.locator(f"xpath={xpath_item}").click()
        page.wait_for_load_state("networkidle")
        page.wait_for_selector("table.ng-table tbody tr", timeout=15000)
        
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
            return

        req_id = ids[0]
        url = f"https://gercon.procempa.com.br/gerconweb/#/painelSolicitante/detalheSolicitacao?idSolicitacao={req_id}"
        print(f"Opening {url} with window.open...")
        
        with context.expect_page() as new_page_info:
            page.evaluate(f"window.open('{url}', '_blank');")
            
        worker_page = new_page_info.value
        worker_page.wait_for_load_state("networkidle")
        worker_page.wait_for_timeout(5000)
        
        html = worker_page.content()
        with open("window_open_dom.html", "w", encoding="utf-8") as f:
            f.write(html)
            
        browser.close()

if __name__ == "__main__":
    main()
