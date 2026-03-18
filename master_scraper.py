import os
import csv
import json
import time
import logging
import math
from datetime import datetime
from typing import Dict, Any, List
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# --- CONFIGURAÇÃO DE LOGGING ---
file_handler = logging.FileHandler("master_scraper.log", encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)

# --- CARREGAMENTO DE AMBIENTE ---
load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")
CSV_FILE = "dados_gercon_master.csv"
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")
HEADLESS = os.getenv("HEADLESS", "True").lower() == "true"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
TIMEOUT = int(os.getenv("TIMEOUT", "30000"))

# --- ESTRUTURA DE DADOS (DOMÍNIO) ---
COLUNAS = [
    "Protocolo", "Situação", "Origem da Lista", "Data Solicitação", 
    "Nome do Paciente", "Data de Nascimento", "Sexo", "Cor", "CPF", 
    "Nome da Mãe", "Cartão SUS", "Logradouro", "Número", "Complemento", 
    "Bairro", "CEP", "Nacionalidade", "Ordem Judicial",
    "Especialidade", "Complexidade", "Risco Cor", "Pontuação",
    "CID Código", "CID Descrição", "Unidade Solicitante",
    "Histórico Quadro Clínico"  # Campo consolidado cronológico
]

LISTAS_ALVO = [
    {"nome": "Agendadas e Confirmadas", "chave": "agendadas"},    
    {"nome": "Pendentes", "chave": "pendente"},     
    {"nome": "Expiradas", "chave": "cancelada"}, 
    {"nome": "Fila de Espera", "chave": "filaDeEspera"},   
    {"nome": "Outras", "chave": "outras"}
]

def format_protocolo(num):
    if not num: return ""
    num = str(num)
    if len(num) == 12:
        return f"{num[0:2]}-{num[2:4]}-{num[4:11]}-{num[11]}"
    return num

def timestamp_to_date(ts):
    if not ts: return ""
    try:
        dt = datetime.fromtimestamp(ts / 1000.0)
        return dt.strftime("%d/%m/%Y %H:%M")
    except:
        return ""

def clean_data_row(data):
    cleaned_row = {}
    for col in COLUNAS:
        v = data.get(col, "")
        if isinstance(v, str):
            cleaned_row[col] = v.replace("\r\n", "\n").replace("\r", "\n")
        else:
            cleaned_row[col] = v
    return cleaned_row

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
    data["Ordem Judicial"] = j.get("liminarOrdemJudicial", "")
    
    u = j.get("usuarioSUS") or {}
    data["Nome do Paciente"] = u.get("nomeCompleto", "")
    data["Data de Nascimento"] = timestamp_to_date(u.get("dataNascimento")).split(" ")[0] if timestamp_to_date(u.get("dataNascimento")) else ""
    data["Sexo"] = u.get("sexo", "")
    data["Cor"] = u.get("racaCor", "")
    data["CPF"] = u.get("cpf", "")
    data["Nome da Mãe"] = u.get("nomeMae", "")
    data["Cartão SUS"] = u.get("cartaoSus", "")
    
    data["Logradouro"] = u.get("logradouro", "")
    data["Número"] = u.get("numero", "")
    data["Complemento"] = u.get("complemento", "")
    data["Bairro"] = u.get("bairro", "")
    data["CEP"] = u.get("cep", "")
    data["Nacionalidade"] = u.get("nacionalidade", "")
    
    data["Especialidade"] = (j.get("especialidade") or {}).get("descricao", "")
    data["Complexidade"] = j.get("complexidade", "")
    data["Risco Cor"] = (j.get("classificacaoRisco") or {}).get("cor", "")
    data["Pontuação"] = (j.get("classificacaoRisco") or {}).get("totalPontos", "")
    
    data["CID Código"] = (j.get("cidPrincipal") or {}).get("codigo", "")
    data["CID Descrição"] = (j.get("cidPrincipal") or {}).get("descricao", "")
    data["Unidade Solicitante"] = (j.get("unidadeSolicitante") or {}).get("nome", "")

    # --- DESAFIO: QUADRO CLÍNICO CRONOLÓGICO ---
    evolucoes = j.get("evolucoes", [])
    evolucoes.sort(key=lambda x: x.get("data", 0)) # Ordem Cronológica (Antiga -> Nova)
    
    historico_textos = []
    for evo in evolucoes:
        dt_evo = timestamp_to_date(evo.get("data"))
        usuario = (evo.get("usuario") or {}).get("nome", "Sistema")
        
        try:
            detalhes_str = evo.get("detalhes", "{}")
            detalhes_json = json.loads(detalhes_str)
            
            # Captura todos os campos de texto relevantes dentro desta evolução
            itens = detalhes_json.get("itensEvolucao", [])
            for item in itens:
                label = item.get("label", item.get("codigo", "Informação"))
                texto = item.get("texto", "").strip()
                if texto:
                    linha_evo = f"\n\n[{dt_evo} | {label} | {usuario}]: {texto}"
                    historico_textos.append(linha_evo)
        except:
            continue
            
    data["Histórico Quadro Clínico"] = " | ".join(historico_textos)
    
    return data

# --- PERSISTÊNCIA ---
def init_csv():
    if not os.path.exists(CSV_FILE):
        logger.info(f"Criando novo arquivo CSV: {CSV_FILE}")
        with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
    else:
        logger.info(f"Arquivo CSV {CSV_FILE} já existe.")

def load_existing():
    existing = {}
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("Protocolo"):
                    existing[row["Protocolo"]] = row
    return existing

def save_all_to_csv(data_dict: Dict[str, Any]):
    temp_file = CSV_FILE + ".tmp"
    try:
        with open(temp_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in data_dict.values():
                writer.writerow(row)
        os.replace(temp_file, CSV_FILE)
    except Exception as e:
        logger.error(f"Erro ao salvar CSV: {e}")

# --- MOTOR DE SCRAPING ---
def main():
    logger.info("==================================================")
    logger.info("Iniciando nova execução do Master Scraper...")
    logger.info("==================================================")
    init_csv()
    records = load_existing()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS, args=['--no-sandbox'])
        context = browser.new_context()
        page = context.new_page()
        
        # Login
        page.goto(GERCON_URL, wait_until="networkidle")
        page.fill('#username', USER)
        page.fill('#password', PASS)
        page.click('#kc-login')
        page.wait_for_load_state("networkidle")
        
        # Seleção de Unidade
        try:
            xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
            page.wait_for_selector(f"xpath={xpath_btn}", timeout=TIMEOUT)
            page.locator(f"xpath={xpath_btn}").click()
            page.wait_for_load_state("networkidle")
        except: pass

        # Carregar o contexto do Angular
        xpath_item = "/html/body/div[6]/div/ul/li[1]"
        page.wait_for_selector(f"xpath={xpath_item}")
        page.locator(f"xpath={xpath_item}").click()
        page.wait_for_selector("table.ng-table tbody tr", timeout=TIMEOUT*10)

        last_ping_time = time.time()
        
        # Reset globals for the entire scrape
        global payload_bytes_total
        if 'payload_bytes_total' not in globals():
            payload_bytes_total = 0
            
        for lista in LISTAS_ALVO:
            nome = lista["nome"]
            chave = lista["chave"]
            logger.info(f">>> Processando Lista: {nome}")
            
            # --- CLIQUE NA ABA (Bypass de Lazy Loading do Angular) ---
            try:
                # Tenta localizar a aba pela chave ou pelo nome
                selectors = [f"a[ng-click*=\"'{chave}'\"]", f"xpath=//a[contains(., '{nome}')]", f"xpath=//li[contains(., '{nome}')]"]
                tab_found = False
                for sel in selectors:
                    if page.locator(sel).first.is_visible():
                        page.locator(sel).first.click()
                        tab_found = True
                        break
                
                if tab_found:
                    # Espera a lista (tabela) carregar de fato na tela.
                    page.wait_for_selector("table.ng-table tbody tr", timeout=TIMEOUT*10)
                    page.wait_for_timeout(500)
                else:
                    logger.warning(f"  Aba '{nome}' não encontrada ou não visível no momento.")
                    continue
            except Exception as e:
                logger.warning(f"  Aviso ao interagir com a aba '{nome}': {e}")
                continue
            
            page_num = 1
            page_size = PAGE_SIZE
            total_pages = None
            start_time_total = time.time()
            
            while True:
                # Trava de segurança para impedir a paginação infinita
                if total_pages is not None and page_num > total_pages:
                    logger.info(f"--- Concluído: Todas as {total_pages} páginas de {nome} projetadas foram extraídas! ---")
                    break

                if total_pages is not None:
                    elapsed_time = time.time() - start_time_total
                    pages_completed = page_num - 1
                    
                    cadastros_completed = pages_completed * page_size
                    avg_time_per_page = elapsed_time / pages_completed if pages_completed > 0 else 0
                    cadastros_por_seg = cadastros_completed / elapsed_time if elapsed_time > 0 else 0
                    
                    remaining_pages = total_pages - pages_completed
                    eta_seconds = max(remaining_pages * avg_time_per_page, 0)
                    total_seconds = elapsed_time + eta_seconds
                    
                    percent = (pages_completed / total_pages * 100) if total_pages > 0 else 0
                    
                    def format_time(secs):
                        h = int(secs // 3600)
                        m = int((secs % 3600) // 60)
                        s = int(secs % 60)
                        return f"{h:02d}h{m:02d}m{s:02d}s"
                    
                    def format_size(bytes_size):
                        if bytes_size < 1024:
                            return f"{bytes_size}B"
                        elif bytes_size < 1024 * 1024:
                            return f"{(bytes_size / 1024):.2f}KB"
                        elif bytes_size < 1024 * 1024 * 1024:
                            return f"{(bytes_size / 1024 / 1024):.2f}MB"
                        else:
                            return f"{(bytes_size / 1024 / 1024 / 1024):.2f}GB"
                    
                    batch_bytes = globals().get('payload_bytes_last_batch', 0)
                    total_bytes = globals().get('payload_bytes_total', 0)
                    
                    payload_str = f"{format_size(batch_bytes)}/{format_size(total_bytes)}" if total_bytes else ""
                    progress_line = f"{percent:.4f}% {page_num}+{remaining_pages} {payload_str} {cadastros_por_seg:.4f}/seg {format_time(elapsed_time)}+{format_time(eta_seconds)}={format_time(total_seconds)}"
                    # print(progress_line)
                    logger.info(progress_line)
                
                # Executa script JavaScript na página que busca os IDs paginados via API
                # e depois busca os JSONs individuais via $http em paralelo
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
                        
                        params.pagina = {page_num};
                        params.tamanhoPagina = {page_size};
                        
                        let queryUrl = '/gercon/rest/solicitacoes/paineis';
                        let pageResponse = await $http.get(queryUrl, {{ params: params }});
                        
                        if (!pageResponse.data || !pageResponse.data.dados || pageResponse.data.dados.length === 0) {{
                            return null; // Paginação chegou ao fim
                        }}
                        
                        let ids = pageResponse.data.dados.map(item => item.id);
                        let totalRegistros = pageResponse.data.totalDados || 0;
                        
                        let totalBytes = 0;
                        let promises = ids.map(id => 
                            $http.get('/gercon/rest/solicitacoes/' + id, {{ transformResponse: [function (data) {{ return data; }}] }})
                                .then(r => {{
                                    let rawString = r.data || "";
                                    totalBytes += new Blob([rawString]).size;
                                    try {{
                                        return JSON.parse(rawString);
                                    }} catch (e) {{
                                        return {{error: id}};
                                    }}
                                }})
                                .catch(e => ({{error: id}}))
                        );
                        
                        let results = await Promise.all(promises);
                        return {{ jsons: results, totalDados: totalRegistros, bytesDownload: totalBytes }};
                    }} catch (e) {{
                        return {{ error: "JS_EXCEPTION: " + e.message }};
                    }}
                }}"""
                
                try:
                    response_data = page.evaluate(js_script)
                except Exception as e:
                    logger.warning(f"Conexão perdida ou erro de context na pág {page_num}: {e}. Tentando refresh...")
                    try:
                        page.goto(GERCON_URL, wait_until="load", timeout=TIMEOUT)
                        # Re-login se necessário
                        if page.locator('#username').count() > 0:
                            page.fill('#username', USER)
                            page.fill('#password', PASS)
                            page.click('#kc-login')
                            page.wait_for_load_state("networkidle")
                        
                        # Re-seleciona Unidade
                        try:
                            xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
                            page.wait_for_selector(f"xpath={xpath_btn}", timeout=TIMEOUT)
                            page.locator(f"xpath={xpath_btn}").click()
                        except: pass

                        # Volta para a aba correta
                        xpath_init = "/html/body/div[6]/div/ul/li[1]"
                        page.wait_for_selector(f"xpath={xpath_init}")
                        page.locator(f"xpath={xpath_init}").click()
                        
                        # Repete clique na aba
                        selectors = [f"a[ng-click*=\"'{chave}'\"]", f"xpath=//a[contains(., '{nome}')]", f"xpath=//li[contains(., '{nome}')]"]
                        for sel in selectors:
                            if page.locator(sel).first.is_visible():
                                page.locator(sel).first.click()
                                break
                        page.wait_for_selector("table.ng-table tbody tr", timeout=TIMEOUT)
                        logger.info("Sessão recuperada. Retomando coleta...")
                        continue # tenta de novo a mesma página
                    except Exception as ex:
                        logger.error(f"Falha ao recuperar sessão: {ex}")
                        break

                if not response_data:
                    logger.info(f"--- Concluído: A fila acabou para {nome}! ---")
                    break
                
                if "error" in response_data:
                    logger.error(f"Erro mapeado dentro da página: {response_data['error']}")
                    break
                    
                jsons = response_data.get("jsons", [])
                if not jsons:
                    logger.warning(f"Resposta incorreta ou sem 'jsons' na pág {page_num}")
                    break
                
                bytes_neste_lote = response_data.get("bytesDownload", 0)
                
                globals()['payload_bytes_total'] += bytes_neste_lote
                globals()['payload_bytes_last_batch'] = bytes_neste_lote
                
                total_dados = response_data.get("totalDados", 0)
                logger.debug(f"[main_loop] Lote com {len(jsons)} registros. Fila total: {total_dados}. Download neste lote: {(bytes_neste_lote / 1024):.2f}KB.")
                
                if total_pages is None and total_dados > 0:
                    total_pages = math.ceil(total_dados / page_size)
                    
                for j in jsons:
                    if j is None: continue
                    if "error" in j: continue
                    
                    data = flatten_solicitacao(j, nome)
                    if data and "Protocolo" in data and data["Protocolo"]:
                        prot = data["Protocolo"]
                        cleaned_row = clean_data_row(data)
                        records[prot] = cleaned_row
                
                # Salva o arquivo CSV atualizado com a página inteira
                save_all_to_csv(records)
                page_num += 1
                
                # Ping preventivo para manter SSO ativo (estratégia dom_scraper)
                if time.time() - last_ping_time > 500: # 5 minutos
                    try:
                        ping_js = """() => {
                            let $http = angular.element(document.body).injector().get('$http');
                            $http.get('/gercon/rest/solicitacoes/paineis', { params: { pagina: 1, tamanhoPagina: 1 } });
                        }"""
                        page.evaluate(ping_js)
                        last_ping_time = time.time()
                    except: pass

        browser.close()
        logger.info("Master Scraper finalizado com sucesso.")

if __name__ == "__main__":
    if USER and PASS:
        main()
    else:
        logger.error("Credenciais não configuradas!")
