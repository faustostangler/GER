import os
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
        
        def log_request(route):
            if "rest/" in route.request.url:
                print(f"API CALL: {route.request.url}")
            route.continue_()
            
        page.route("**/*", log_request)
        
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
            
        print("--- CLICANDO EM FILA DE ESPERA ---")
        xpath_item = "/html/body/div[6]/div/ul/li[4]"
        page.locator(f"xpath={xpath_item}").click()
        page.wait_for_selector("table.ng-table tbody tr", timeout=15000)
        
        print("--- CLICANDO EM PROXIMA ---")
        next_btn_li = page.locator("ul.pagination li", has=page.locator("a:text-is('›')")).first
        if next_btn_li.count() > 0:
            next_btn_li.locator("a").first.click()
            page.wait_for_timeout(3000)
            
        browser.close()

if __name__ == "__main__":
    main()
