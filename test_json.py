import os
import json
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(".env/creds.env")

USER = os.getenv("username")
PASS = os.getenv("password")

def main():
    print("Testing JSON extraction...")
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
        
        req_id = ids[0]
        print(f"Buscando ID {req_id} via fetch API...")
        
        json_data = page.evaluate(f"""async () => {{
            const req = await fetch('/gercon/rest/solicitacoes/{req_id}');
            return await req.json();
        }}""")
        
        with open("amostra_json.json", "w", encoding="utf-8") as f:
            json.dump(json_data, f, indent=4, ensure_ascii=False)
            
        print("Salvo em amostra_json.json")
        browser.close()

if __name__ == "__main__":
    main()
