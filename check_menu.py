import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")
GERCON_URL = "https://gercon.procempa.com.br/gerconweb/"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto(GERCON_URL)
    page.fill("#username", USER)
    page.fill("#password", PASS)
    page.click("#kc-login")
    page.wait_for_load_state("networkidle")
    
    try:
        xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
        page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
        page.locator(f"xpath={xpath_btn}").click()
        page.wait_for_load_state("networkidle")
    except: pass
    
    # Wait for the sidebar menu
    page.wait_for_selector("xpath=/html/body/div[6]/div/ul", timeout=10000)
    li_elements = page.locator("xpath=/html/body/div[6]/div/ul/li")
    count = li_elements.count()
    print(f"Found {count} menu items")
    for i in range(count):
        text = li_elements.nth(i).inner_text().strip()
        print(f"Item {i+1}: {text}")
    browser.close()
