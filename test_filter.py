"""
Teste de filtros de data de alteração na API do Gercon.

Objetivo: descobrir se a API aceita parâmetros do tipo
'dataAlteracaoInicio' ou similar para filtrar registros
alterados a partir de uma data X.

Estratégia de detecção:
- Usa o watermark real do SQLite local como ponto de corte
- Testa cada candidato e compara o `totalDados` retornado
- Se totalDados < total_sem_filtro → o filtro funcionou!
"""

import os
import sqlite3
import logging
from datetime import datetime
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")
DB_FILE = "gercon_raw_data.db"
CHAVE = "filaDeEspera"  # lista alvo para os testes

TIMEOUT = 30_000


def get_watermark_from_db(chave: str):
    """Recupera o watermark real já salvo no SQLite."""
    try:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            "SELECT MAX(data_alteracao) FROM solicitacoes_raw WHERE origem_lista = ?",
            (chave,),
        )
        row = conn.fetchone() if False else cur.fetchone()
        conn.close()
        if row and row[0]:
            ts_ms = row[0]
            ts_s = ts_ms / 1000
            dt = datetime.fromtimestamp(ts_s)
            return ts_ms, ts_s, dt
    except Exception as e:
        logger.warning(f"Erro ao ler DB: {e}")
    # Fallback: 7 dias atrás
    from datetime import timedelta

    dt = datetime.now() - timedelta(days=7)
    ts_s = dt.timestamp()
    ts_ms = int(ts_s * 1000)
    return ts_ms, ts_s, dt


def probe_filter(page, description: str, param_js: str, baseline: int) -> int:
    """
    Executa uma requisição com o filtro informado.
    Retorna o totalDados encontrado (ou -1 em caso de erro).
    """
    js = f"""async () => {{
        try {{
            let scope = angular.element(document.querySelector('table.ng-table')).scope();
            let $http = angular.element(document.body).injector().get('$http');

            let origParams = scope.solicCtrl?.parametros?.['{CHAVE}'];
            if (!origParams) return {{ error: 'Chave nao encontrada: {CHAVE}' }};

            let params = angular.copy(origParams);
            delete params.dataInicioConsulta; delete params.dataFimConsulta;
            delete params.dataInicioAlta;     delete params.dataFimAlta;

            params.pagina      = 1;
            params.tamanhoPagina = 1;  // só queremos o total, não os dados

            // Injeta o filtro candidato
            {param_js}

            let resp = await $http.get('/gercon/rest/solicitacoes/paineis', {{ params: params }});
            let total = resp?.data?.totalDados ?? -1;
            return {{ total: total }};
        }} catch (e) {{
            return {{ error: e.message }};
        }}
    }}"""

    try:
        result = page.evaluate(js)
        if isinstance(result, dict) and "error" in result:
            status = f"❌ ERRO JS: {result['error'][:80]}"
            total = -1
        else:
            total = result.get("total", -1) if isinstance(result, dict) else -1
            if total == -1:
                status = "❌ totalDados ausente"
            elif total < baseline:
                status = f"✅ FILTROU! {total} < baseline {baseline}  ← FILTRO FUNCIONA"
            elif total == baseline:
                status = f"⚠️  Sem efeito ({total} == baseline)"
            else:
                status = f"⚠️  Aumentou? ({total} > baseline)"
    except Exception as ex:
        status = f"❌ EXCEÇÃO Python: {ex}"
        total = -1

    logger.info(f"  [{status}]  {description}")
    return total


def main():
    if not USER or not PASS:
        logger.error("Credenciais não configuradas!")
        return

    ts_ms, ts_s, dt = get_watermark_from_db(CHAVE)
    ts_iso_full = dt.strftime("%Y-%m-%dT%H:%M:%S")
    ts_iso_date = dt.strftime("%Y-%m-%d")
    ts_iso_z = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    logger.info("=" * 65)
    logger.info("TESTE DE FILTROS DE DATA DE ALTERAÇÃO — API Gercon")
    logger.info("=" * 65)
    logger.info(f"Watermark usado:  {dt}  (ms={ts_ms})")
    logger.info("")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_context().new_page()

        # Login
        page.goto(GERCON_URL, wait_until="networkidle")
        (page.fill("#username", USER),)
        page.fill("#password", PASS)
        page.click("#kc-login")
        page.wait_for_load_state("networkidle")

        # Seleciona Unidade
        try:
            btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
            page.wait_for_selector(f"xpath={btn}", timeout=TIMEOUT)
            page.locator(f"xpath={btn}").click()
            page.wait_for_load_state("networkidle")
        except Exception:
            pass

        # Vai para a aba Fila de Espera
        page.wait_for_selector("xpath=/html/body/div[6]/div/ul/li[1]")
        page.locator("xpath=/html/body/div[6]/div/ul/li[1]").click()
        page.wait_for_selector("table.ng-table tbody tr", timeout=TIMEOUT * 3)
        page.locator(f"a[ng-click*=\"'{CHAVE}'\"]").first.click()
        page.wait_for_timeout(3000)

        # ── 1. BASELINE sem filtro ────────────────────────────────────────
        logger.info("── BASELINE (sem filtro) ─────────────────────────────")
        baseline = probe_filter(page, "Sem filtro (referência)", "", baseline=0)
        logger.info(f"  Baseline = {baseline} registros totais\n")

        # ── 2. Candidatos em timestamp ms ─────────────────────────────────
        logger.info("── TIMESTAMP (ms) ────────────────────────────────────")
        candidates_ms = [
            "dataAlteracaoInicio",
            "dataInicioAlteracao",
            "dataInicioAlteracaoStatus",
            "dataUltimaAlteracaoInicio",
            "dataAlterouUltimaSituacaoInicio",
            "dataModificacaoInicio",
            "dataAtualizacaoInicio",
        ]
        for c in candidates_ms:
            probe_filter(
                page, f"params.{c} = {ts_ms}", f"params.{c} = {ts_ms};", baseline
            )

        logger.info("")
        # ── 3. Candidatos em ISO string ───────────────────────────────────
        logger.info("── ISO STRING (YYYY-MM-DDTHH:MM:SS) ─────────────────")
        for c in candidates_ms:
            probe_filter(
                page,
                f"params.{c} = '{ts_iso_full}'",
                f"params.{c} = '{ts_iso_full}';",
                baseline,
            )

        logger.info("")
        # ── 4. Candidatos em data simples YYYY-MM-DD ──────────────────────
        logger.info("── DATE ONLY (YYYY-MM-DD) ────────────────────────────")
        for c in candidates_ms:
            probe_filter(
                page,
                f"params.{c} = '{ts_iso_date}'",
                f"params.{c} = '{ts_iso_date}';",
                baseline,
            )

        logger.info("")
        # ── 5. Candidatos com Z suffix ────────────────────────────────────
        logger.info("── ISO COM Z (UTC) ───────────────────────────────────")
        for c in candidates_ms:
            probe_filter(
                page,
                f"params.{c} = '{ts_iso_z}'",
                f"params.{c} = '{ts_iso_z}';",
                baseline,
            )

        logger.info("")
        # ── 6. Testa o formato que a própria UI usa ─────────────────────
        logger.info("── INSPEÇÃO: parâmetros que a própria UI usa ─────────")
        inspect_js = f"""async () => {{
            let scope = angular.element(document.querySelector('table.ng-table')).scope();
            let origParams = scope.solicCtrl?.parametros?.['{CHAVE}'];
            return JSON.stringify(origParams || {{}});
        }}"""
        raw_params = page.evaluate(inspect_js)
        logger.info(f"  Params originais da UI:\n  {raw_params}")

        browser.close()

    logger.info("")
    logger.info("=" * 65)
    logger.info("Teste concluído. Procure linhas com ✅ acima.")
    logger.info("=" * 65)


if __name__ == "__main__":
    main()
