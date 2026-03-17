import os
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
import json

load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
    context = browser.new_context()
    main_page = context.new_page()
    main_page.goto(GERCON_URL, wait_until="networkidle", timeout=30000)
    
    main_page.fill('#username', USER)
    main_page.fill('#password', PASS)
    main_page.click('#kc-login')
    main_page.wait_for_load_state("networkidle")
    
    xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
    try:
        main_page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
        main_page.locator(f"xpath={xpath_btn}").click()
        main_page.wait_for_load_state("networkidle")
    except:
        pass
        
    xpath_item = "/html/body/div[6]/div/ul/li[4]"
    main_page.wait_for_selector(f"xpath={xpath_item}", timeout=30000)
    main_page.locator(f"xpath={xpath_item}").click()
    main_page.wait_for_load_state("networkidle")
    
    row_selector = "table.ng-table tbody tr"
    main_page.wait_for_selector(row_selector, timeout=30000)

    js_script = """() => {
        let scope = angular.element(document.querySelector('table.ng-table')).scope();
        return JSON.stringify(scope.solicCtrl.parametros, null, 2);
    }"""
    
    params_str = main_page.evaluate(js_script)
    with open("params_dump.json", "w") as f:
        f.write(params_str)
    
    browser.close()
