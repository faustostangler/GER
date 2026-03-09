import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import time

load_dotenv(".env/creds.env")
USER = os.getenv("username")
PASS = os.getenv("password")

def print_request(request):
    if "api" in request.url or "rest" in request.url or "solicitacao" in request.url:
        print(f"REQUEST -> {request.method} {request.url}")

def print_response(response):
    if "api" in response.url or "rest" in response.url or "solicitacao" in response.url:
        pass # print(f"RESPONSE <- {response.status} {response.url}")

def main():
    print("Testing network API capture...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = browser.new_context()
        page = context.new_page()
        page.on("request", print_request)
        page.on("response", print_response)
        
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
        
        print("\n\n--- CLICKING A ROW ---\n\n")
        page.locator("table.ng-table tbody tr").nth(0).click()
        page.wait_for_selector("text=Voltar", timeout=15000)
        time.sleep(3)
        browser.close()

if __name__ == "__main__":
    main()
