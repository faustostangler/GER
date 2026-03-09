import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(".env/creds.env")
USER = os.getenv("username")
PASS = os.getenv("password")

def main():
    print("Testing angular extraction...")
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
            
        page.locator("xpath=/html/body/div[6]/div/ul/li[4]").click()
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
        print("IDS FOUND:", ids)
        browser.close()

if __name__ == "__main__":
    main()
