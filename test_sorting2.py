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


def timestamp_to_date(ts):
    if not ts:
        return "N/A"
    try:
        from datetime import datetime

        return datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


def test_request(page, description, custom_params_js):
    js_script = f"""async () => {{
        try {{
            let scope = angular.element(document.querySelector('table.ng-table')).scope();
            let $http = angular.element(document.body).injector().get('$http');
            
            let origParams = scope.solicCtrl?.parametros?.['filaDeEspera'];
            if (!origParams) return {{ error: "Chave não encontrada: filaDeEspera" }};
            
            let params = angular.copy(origParams);
            delete params.dataInicioConsulta; delete params.dataFimConsulta;
            delete params.dataInicioAlta; delete params.dataFimAlta;
            
            params.pagina = 1;
            params.tamanhoPagina = 5;
            
            // Adicionar os parametros customizados para testar ordenacao
            {custom_params_js}
            
            let pageResponse = await $http.get('/gercon/rest/solicitacoes/paineis', {{ params: params }});
            
            if (!pageResponse.data || !pageResponse.data.dados || pageResponse.data.dados.length === 0) {{
                return []; 
            }}
            
            let ids = pageResponse.data.dados.map(item => item.id);
            let promises = ids.map(id => $http.get('/gercon/rest/solicitacoes/' + id).then(r => r.data).catch(e => null));
            let results = await Promise.all(promises);
            return results;
        }} catch (e) {{
            return {{ error: e.message }};
        }}
    }}"""

    logger.info(f"\\n--- TESTE: {description} ---")
    data = page.evaluate(js_script)
    if isinstance(data, dict) and "error" in data:
        logger.error(f"Erro no JS: {data['error']}")
        return
    if not data:
        logger.info("  Nenhum resultado.")
        return

    for index, item in enumerate(data):
        if not item:
            continue
        prot = item.get("numeroCMCE", "")
        dt_solic = timestamp_to_date(item.get("dataSolicitacao", 0))
        timestamp_to_date(item.get("dataCadastro", 0))
        dt_alt = timestamp_to_date(item.get("dataAlterouUltimaSituacao", 0))

        logger.info(
            f"  #{index + 1} Prot: {prot} | Alterado: {dt_alt} | Solic: {dt_solic}"
        )


def main():
    if not USER or not PASS:
        return
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
        page.locator("a[ng-click*=\"'filaDeEspera'\"]").first.click()
        page.wait_for_timeout(3000)

        # Bateria de Testes =============

        test_request(page, "Original (+diasNaFilaDeEspera)", "")

        test_request(
            page,
            "Ord: -dataAlterouUltimaSituacao",
            "params.ordenacao = ['-dataAlterouUltimaSituacao'];",
        )
        test_request(
            page,
            "Ord: +dataAlterouUltimaSituacao",
            "params.ordenacao = ['+dataAlterouUltimaSituacao'];",
        )

        test_request(
            page, "Ord: -dataSolicitacao", "params.ordenacao = ['-dataSolicitacao'];"
        )

        test_request(
            page,
            "Filtro 7 Dias (dataInicioSolicitacao)",
            """
            let d = new Date(); d.setDate(d.getDate() - 7);
            params.dataInicioSolicitacao = d.toISOString();
        """,
        )

        test_request(
            page,
            "Filtro 7 Dias (dataInicioAlteracaoUltimaSituacao)",
            """
            let d = new Date(); d.setDate(d.getDate() - 7);
            params.dataInicioAlteracaoUltimaSituacao = d.toISOString();
        """,
        )

        test_request(
            page,
            "Filtro 7 Dias (dataInicial)",
            """
            let d = new Date(); d.setDate(d.getDate() - 7);
            params.dataInicial = d.toISOString();
        """,
        )

        browser.close()


if __name__ == "__main__":
    main()
