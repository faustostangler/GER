import os
import logging
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")


def test_params(page, chave):
    js_script = f"""async () => {{
        let scope = angular.element(document.querySelector('table.ng-table')).scope();
        let $http = angular.element(document.body).injector().get('$http');
        
        let origParams = scope.solicCtrl?.parametros?.['{chave}'];
        if (!origParams) return {{ error: "Chave não encontrada: {chave}" }};
        
        return origParams;
    }}"""

    logger.info(f"\\n--- PARAMS para {chave} ---")
    data = page.evaluate(js_script)
    logger.info(data)


def main():
    if not USER or not PASS:
        logger.error("Credenciais não configuradas!")
        return

    logger.info("Iniciando navegador Playwright para ler estruturas de parâmetros...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        page.goto(GERCON_URL, wait_until="networkidle")
        page.fill("#username", USER)
        page.fill("#password", PASS)
        page.click("#kc-login")
        page.wait_for_load_state("networkidle")

        try:
            xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
            page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
            page.locator(f"xpath={xpath_btn}").click()
            page.wait_for_load_state("networkidle")
        except Exception:
            pass

        xpath_init = "/html/body/div[6]/div/ul/li[1]"
        page.wait_for_selector(f"xpath={xpath_init}")
        page.locator(f"xpath={xpath_init}").click()
        page.wait_for_selector("table.ng-table tbody tr", timeout=30000)

        for p in ["filaDeEspera", "outras", "agendadas", "cancelada", "pendente"]:
            page.locator(f"a[ng-click*=\"'{p}'\"]").first.click()
            page.wait_for_timeout(2000)
            test_params(page, p)

        browser.close()


if __name__ == "__main__":
    main()
