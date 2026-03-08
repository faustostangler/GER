import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(".env/creds.env")

USER = os.getenv("username")
PASS = os.getenv("password")

def main():
    print("Starting playwright...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, args=['--no-sandbox'])
        context = browser.new_context()
        page = context.new_page()
        
        print("Navigating to https://gercon.procempa.com.br/gerconweb/ ...")
        page.goto("https://gercon.procempa.com.br/gerconweb/", wait_until="networkidle", timeout=15000)
        
        print("Saving login DOM...")
        with open("login_dom.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print("Saved login DOM to login_dom.html")
        
        print("Filling login credentials...")
        page.fill('#username', USER)
        page.fill('#password', PASS)
        page.click('#kc-login')
        
        print("Waiting for page load after login...")
        page.wait_for_load_state("networkidle")
        print(f"Post-login URL: {page.url}")
        
        print("Saving post-login DOM...")
        with open("post_login_dom.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print("Saved post-login DOM to post_login_dom.html")
        
        print("Looking for post-login button...")
        try:
            # The xpath from the user
            xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
            page.wait_for_selector(f"xpath={xpath_btn}", timeout=15000)
            page.locator(f"xpath={xpath_btn}").click()
            print("Clicked the post-login button.")
            
            # Wait for any potential navigation/load after clicking
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(2000) # Give it a bit more time for any JS menus to load
            print(f"URL after first click: {page.url}")
            
            print("Looking for LISTA DE ESPERA item...")
            xpath_item = "/html/body/div[6]/div/ul/li[4]"
            page.wait_for_selector(f"xpath={xpath_item}", timeout=15000)
            page.locator(f"xpath={xpath_item}").click()
            print("Clicked the LISTA DE ESPERA item.")
            
            # Wait for any potential navigation/load after clicking list item
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(3000) # Give it a bit of time for the data/list to load
            print(f"Final URL: {page.url}")
        except Exception as e:
            print(f"Failed to find or click the buttons: {e}")
        
        print(f"Page Title: {page.title()}")
        
        with open("lista_espera_dom.html", "w", encoding="utf-8") as f:
            f.write(page.content())
        print("Saved scrape target DOM to lista_espera_dom.html")
            
        browser.close()

if __name__ == "__main__":
    if USER and PASS:
        main()
    else:
        print("Credentials not found in .env!")

print("Done!")
