import os
import csv
import json
import time
import logging
import math
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# --- CONFIGURAÇÃO DE LOGGING ---
file_handler = logging.FileHandler("multiscraper.log", encoding="utf-8")
file_handler.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[file_handler, console_handler],
)
logger = logging.getLogger(__name__)

# --- CARREGAMENTO DE AMBIENTE ---
load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")
CSV_FILE = "dados_gercon_consolidado.csv"
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")
HEADLESS = os.getenv("HEADLESS", "True").lower() == "true"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
TIMEOUT = int(os.getenv("TIMEOUT", "30000"))

# --- ESTRUTURA DE DADOS (DOMÍNIO) ---
COLUNAS = [
    "Protocolo",
    "Situação",
    "Origem da Lista",
    "Data Solicitação",
    "Data Nascimento",
    "Paciente",
    "CPF",
    "CNS",
    "Especialidade",
    "Complexidade",
    "Risco Cor",
    "Pontuação",
    "CID Código",
    "CID Descrição",
    "Unidade Solicitante",
    "Histórico Quadro Clínico",  # Campo consolidado cronológico
]

LISTAS_ALVO = [
    {"nome": "Agendadas e Confirmadas", "chave": "agendadas"},
    {"nome": "Pendentes", "chave": "pendente"},
    {"nome": "Expiradas", "chave": "cancelada"},
    {"nome": "Fila de Espera", "chave": "filaDeEspera"},
    {"nome": "Outras", "chave": "outras"},
]


def format_protocolo(num):
    if not num:
        return ""
    num = str(num)
    if len(num) == 12:
        return f"{num[0:2]}-{num[2:4]}-{num[4:11]}-{num[11]}"
    return num


def timestamp_to_date(ts):
    if not ts:
        return ""
    try:
        dt = datetime.fromtimestamp(ts / 1000.0)
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return ""


# --- LÓGICA DE EXTRAÇÃO E ACHATAMENTO (ACL/MAPPER) ---
def flatten_solicitacao(j: Dict[Any, Any], origem_lista: str) -> Dict[str, Any]:
    """
    Transforma o JSON complexo em uma linha plana para o CSV.
    Implementa a nova regra de Histórico de Quadro Clínico Cronológico.
    """
    data = {}
    data["Protocolo"] = format_protocolo(j.get("numeroCMCE", ""))
    data["Situação"] = j.get("situacao", "")
    data["Origem da Lista"] = origem_lista
    data["Data Solicitação"] = timestamp_to_date(j.get("dataSolicitacao"))

    u = j.get("usuarioSUS") or {}
    data["Paciente"] = u.get("nomeCompleto", "")
    data["CPF"] = u.get("cpf", "")
    data["CNS"] = u.get("cartaoSus", "")
    data["Data Nascimento"] = (
        timestamp_to_date(u.get("dataNascimento")).split(" ")[0]
        if u.get("dataNascimento")
        else ""
    )

    data["Especialidade"] = (j.get("especialidade") or {}).get("descricao", "")
    data["Complexidade"] = j.get("complexidade", "")
    data["Risco Cor"] = (j.get("classificacaoRisco") or {}).get("cor", "")
    data["Pontuação"] = (j.get("classificacaoRisco") or {}).get("totalPontos", "")

    data["CID Código"] = (j.get("cidPrincipal") or {}).get("codigo", "")
    data["CID Descrição"] = (j.get("cidPrincipal") or {}).get("descricao", "")
    data["Unidade Solicitante"] = (j.get("unidadeSolicitante") or {}).get("nome", "")

    # --- DESAFIO: QUADRO CLÍNICO CRONOLÓGICO ---
    evolucoes = j.get("evolucoes", [])
    evolucoes.sort(key=lambda x: x.get("data", 0))  # Ordem Cronológica (Antiga -> Nova)

    # Captura Situação Atual para o histórico
    j.get("situacao", "N/A")

    historico_textos = []
    for evo in evolucoes:
        dt_evo = timestamp_to_date(evo.get("data"))
        usuario = (evo.get("usuario") or {}).get("nome", "Sistema")
        evo.get("perfil", "")

        try:
            detalhes_str = evo.get("detalhes", "{}")
            detalhes_json = json.loads(detalhes_str)

            # Captura todos os campos de texto relevantes dentro desta evolução
            itens = detalhes_json.get("itensEvolucao", [])
            for item in itens:
                label = item.get("label", item.get("codigo", "Informação"))
                texto = item.get("texto", "").strip()
                if texto:
                    linha_evo = f"[{dt_evo} - {label} - {usuario}]: {texto}"
                    historico_textos.append(linha_evo)
        except Exception:
            continue

    data["Histórico Quadro Clínico"] = " | ".join(historico_textos)

    return data


# --- PERSISTÊNCIA ---
def save_to_csv(data_dict: Dict[str, Any]):
    temp_file = CSV_FILE + ".tmp"
    try:
        with open(temp_file, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in data_dict.values():
                writer.writerow(row)
        os.replace(temp_file, CSV_FILE)
    except Exception as e:
        logger.error(f"Erro ao salvar CSV: {e}")


def load_existing():
    existing = {}
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("Protocolo"):
                    existing[row["Protocolo"]] = row
    return existing


# --- MOTOR DE SCRAPING ---
def run_scraper():
    logger.info("Iniciando Multiscraper Gercon...")
    records = load_existing()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()

        # Login
        page.goto(GERCON_URL, wait_until="networkidle")
        page.fill("#username", USER)
        page.fill("#password", PASS)
        page.click("#kc-login")
        page.wait_for_load_state("networkidle")

        # Seleção de Unidade
        try:
            xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
            page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
            page.locator(f"xpath={xpath_btn}").click()
            page.wait_for_load_state("networkidle")
        except Exception:
            pass

        # Carregar o contexto do Angular
        xpath_item = "/html/body/div[6]/div/ul/li[1]"
        page.wait_for_selector(f"xpath={xpath_item}")
        page.locator(f"xpath={xpath_item}").click()
        page.wait_for_selector("table.ng-table tbody tr", timeout=TIMEOUT)

        last_ping_time = time.time()
        for lista in LISTAS_ALVO:
            nome = lista["nome"]
            chave = lista["chave"]
            logger.info(f">>> Processando Lista: {nome}")

            # --- CLIQUE NA ABA (Bypass de Lazy Loading do Angular) ---
            try:
                # Tenta localizar a aba pela chave ou pelo nome
                selectors = [
                    f"a[ng-click*=\"'{chave}'\"]",
                    f"xpath=//a[contains(., '{nome}')]",
                    f"xpath=//li[contains(., '{nome}')]",
                ]
                tab_found = False
                for sel in selectors:
                    if page.locator(sel).first.is_visible():
                        page.locator(sel).first.click()
                        tab_found = True
                        break

                if tab_found:
                    # Espera a lista (tabela) carregar de fato na tela.
                    # Isso é muito mais robusto que esperar a variável interna.
                    page.wait_for_selector("table.ng-table tbody tr", timeout=15000)
                    # Um pequeno fôlego para o Angular terminar de sincronizar o scope
                    page.wait_for_timeout(500)
                else:
                    logger.warning(
                        f"  Aba '{nome}' não encontrada ou não visível no momento."
                    )
            except Exception as e:
                logger.warning(f"  Aviso ao interagir com a aba '{nome}': {e}")

            curr_page = 1
            total_pages = 1

            while curr_page <= total_pages:
                logger.info(f"  Pagina {curr_page}/{total_pages} de {nome}...")

                js_script = f"""async () => {{
                    try {{
                        let scope = angular.element(document.querySelector('table.ng-table')).scope();
                        let $http = angular.element(document.body).injector().get('$http');
                        
                        let origParams = scope.solicCtrl?.parametros?.['{chave}'];
                        if (!origParams) {{
                            return {{ error: "Chave de parâmetros não encontrada na UI: {chave}" }};
                        }}
                        
                        let params = angular.copy(origParams);
                        // Bypass de limites de data para puxar TUDO
                        delete params.dataInicioConsulta; delete params.dataFimConsulta;
                        delete params.dataInicioAlta; delete params.dataFimAlta;
                        
                        params.pagina = {curr_page};
                        params.tamanhoPagina = {PAGE_SIZE};
                        
                        let pageResponse;
                        try {{
                            pageResponse = await $http.get('/gercon/rest/solicitacoes/paineis', {{ params: params }});
                        }} catch (httpErr) {{
                            return {{ error: "HTTP Error " + (httpErr.status || "Unknown") + " na lista {chave}" }};
                        }}
                        
                        if (!pageResponse || !pageResponse.data) return {{ error: "SEM_DATA" }};
                        
                        let total = pageResponse.data.totalDados || 0;
                        let dados = pageResponse.data.dados || [];
                        let ids = dados.map(item => item.id);
                        
                        console.log("API Response - Total Docs:", total, "IDS nesta pagina:", ids.length);

                        let results = await Promise.all(ids.map(id => 
                            $http.get('/gercon/rest/solicitacoes/' + id)
                                 .then(r => r.data).catch(e => null)
                        ));
                        
                        return {{ jsons: results.filter(x => x), total: total, ids_count: ids.length }};
                    }} catch (e) {{
                        return {{ error: "JS_EXCEPTION: " + e.message }};
                    }}
                }}"""

                try:
                    res = page.evaluate(js_script)
                except Exception as e:
                    logger.warning(
                        f"Conexão perdida ou erro de context na pág {curr_page}: {e}. Tentando refresh..."
                    )
                    try:
                        page.goto(GERCON_URL, wait_until="load", timeout=TIMEOUT)
                        # Re-login se necessário
                        if page.locator("#username").count() > 0:
                            page.fill("#username", USER)
                            page.fill("#password", PASS)
                            page.click("#kc-login")
                            page.wait_for_load_state("networkidle")

                        # Re-seleciona Unidade
                        try:
                            xpath_btn = (
                                "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
                            )
                            page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
                            page.locator(f"xpath={xpath_btn}").click()
                        except Exception:
                            pass

                        # Volta para a aba correta
                        xpath_init = "/html/body/div[6]/div/ul/li[1]"
                        page.wait_for_selector(f"xpath={xpath_init}")
                        page.locator(f"xpath={xpath_init}").click()

                        # Repete clique na aba
                        selectors = [
                            f"a[ng-click*=\"'{chave}'\"]",
                            f"xpath=//a[contains(., '{nome}')]",
                            f"xpath=//li[contains(., '{nome}')]",
                        ]
                        for sel in selectors:
                            if page.locator(sel).first.is_visible():
                                page.locator(sel).first.click()
                                break
                        page.wait_for_selector(
                            "table.ng-table tbody tr", timeout=TIMEOUT
                        )
                        logger.info("Sessão recuperada. Retomando coleta...")
                        continue  # tenta de novo a mesma página
                    except Exception as ex:
                        logger.error(f"Falha ao recuperar sessão: {ex}")
                        break

                if not res:
                    logger.warning(f"Resposta nula da API Angular na pág {curr_page}")
                    break

                if "error" in res:
                    logger.error(f"Erro mapeado dentro da página: {res['error']}")
                    break

                if "jsons" not in res:
                    logger.warning(
                        f"Resposta incorreta ou sem 'jsons' na pág {curr_page}"
                    )
                    break

                total_docs = res["total"]
                itens_recebidos = res["ids_count"]

                logger.info(
                    f"  [Auditoria] Total Itens: {total_docs} | Recebidos: {itens_recebidos}"
                )
                total_pages = math.ceil(total_docs / PAGE_SIZE) if total_docs > 0 else 1

                if not res["jsons"]:
                    logger.info("  Fim da lista atingido.")
                    break

                for item_json in res["jsons"]:
                    flat = flatten_solicitacao(item_json, nome)
                    records[flat["Protocolo"]] = flat

                if curr_page % 2 == 0:
                    save_to_csv(records)

                # Ping preventivo para manter SSO ativo (estratégia dom_scraper)
                if time.time() - last_ping_time > 300:  # 5 minutos
                    try:
                        ping_js = """async () => {
                            let $http = angular.element(document.body).injector().get('$http');
                            return await $http.get('/gercon/rest/solicitacoes/paineis', { params: { pagina: 1, tamanhoPagina: 1 } })
                                .then(r => 'SUCCESS').catch(e => 'ERROR');
                        }"""
                        page.evaluate(ping_js)
                        last_ping_time = time.time()
                    except Exception:
                        pass

                curr_page += 1

            save_to_csv(records)  # Salva ao fim de cada lista
            logger.info(f"--- Concluído: {nome} ---")

        browser.close()
        logger.info("Multiscraping finalizado com sucesso.")


if __name__ == "__main__":
    run_scraper()

print("done!")
