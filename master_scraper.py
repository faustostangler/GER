import os
import sqlite3
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
# CSV_FILE removido pois agora é dinâmico por lista.
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")
HEADLESS = os.getenv("HEADLESS", "True").lower() == "true"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
TIMEOUT = int(os.getenv("TIMEOUT", "30000"))

LISTAS_ALVO = [
    {"nome": "Agendadas e Confirmadas", "chave": "agendadas"},    
    {"nome": "Pendentes", "chave": "pendente"},     
    {"nome": "Expiradas", "chave": "cancelada"}, 
    {"nome": "Fila de Espera", "chave": "filaDeEspera"},   
    {"nome": "Outras", "chave": "outras"}
]

# --- ESTRUTURA DE DADOS (DOMÍNIO) ---
COLUNAS = [
    "Protocolo", "Situação", "Origem da Lista", "Data Solicitação", 
    "Nome do Paciente", "Data de Nascimento", "Sexo", "Cor", "CPF", 
    "Nome da Mãe", "Cartão SUS", "Logradouro", "Número", "Complemento", 
    "Bairro", "CEP", "Município de Residência", "Nacionalidade", "Ordem Judicial",
    "Especialidade Mãe", "Especialidade", "Especialidade Descrição Auxiliar", "Especialidade CBO",
    "Tipo de Regulação", "Teleconsulta", "Status da Especialidade",
    "Complexidade", "Risco Cor", "Cor Regulador", "Pontos Gravidade", "Pontos Tempo", "Pontuação", "Situação Final",
    "CID Código", "CID Descrição", "Unidade Solicitante",
    "Operador", "Usuário Solicitante", "Unidade Razão Social", "Unidade Descrição",
    "Central de Regulação", "Origem da Regulação",
    "Data do Cadastro", "Médico Solicitante",
    "Histórico Quadro Clínico"
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
        return dt.strftime("%d/%m/%Y %H:%M:%S")
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
    data["Município de Residência"] = (u.get("municipioResidencia") or {}).get("nome", "")
    data["Nacionalidade"] = u.get("nacionalidade", "")
    
    esp = j.get("especialidade") or {}
    esp_mae = esp.get("especialidadeMae") or {}
    data["Especialidade Mãe"] = esp_mae.get("descricao", "")
    data["Especialidade"] = esp.get("descricao", "")
    data["Especialidade Descrição Auxiliar"] = esp.get("descricaoAuxiliar", "")
    data["Especialidade CBO"] = (esp_mae.get("cbo") or {}).get("descricao", "")
    data["Tipo de Regulação"] = esp.get("tipoRegulacao", "")
    data["Teleconsulta"] = esp.get("teleconsulta", "")
    data["Status da Especialidade"] = esp.get("ativa", "")
    
    risk = j.get("classificacaoRisco") or {}
    data["Complexidade"] = j.get("complexidade", "")
    data["Risco Cor"] = risk.get("cor", "")
    data["Cor Regulador"] = j.get("corRegulador", "")
    data["Pontos Gravidade"] = risk.get("pontosGravidade", "")
    data["Pontos Tempo"] = risk.get("pontosTempo", "")
    data["Pontuação"] = risk.get("totalPontos", "")
    data["Situação Final"] = j.get("situacao", "")
    
    data["CID Código"] = (j.get("cidPrincipal") or {}).get("codigo", "")
    data["CID Descrição"] = (j.get("cidPrincipal") or {}).get("descricao", "")
    data["Unidade Solicitante"] = (j.get("unidadeSolicitante") or {}).get("nome", "")
    
    # Bloco do Operador e Unidade do Operador
    op = j.get("operador") or {}
    data["Operador"] = op.get("nome") or (op.get("profissional") or {}).get("nome", "")
    
    us = j.get("usuarioSolicitante") or {}
    data["Usuário Solicitante"] = us.get("nome") or (us.get("profissional") or {}).get("nome", "")
    
    uop = j.get("unidadeOperador") or {}
    data["Unidade Razão Social"] = uop.get("razaoSocial", "")
    data["Unidade Descrição"] = (uop.get("tipoUnidade") or {}).get("descricao", "")
    
    data["Central de Regulação"] = (j.get("centralRegulacao") or {}).get("nome", "")
    data["Origem da Regulação"] = (j.get("centralRegulacaoOrigem") or {}).get("nome", "")

    # --- DESAFIO: QUADRO CLÍNICO CRONOLÓGICO ---
    evolucoes = j.get("evolucoes", [])
    evolucoes.sort(key=lambda x: x.get("data", 0)) # Ordem Cronológica (Antiga -> Nova)
    
    data["Data do Cadastro"] = ""
    data["Médico Solicitante"] = ""
    
    historico_textos = []
    first_evo_found = False
    
    for evo in evolucoes:
        dt_evo = timestamp_to_date(evo.get("data"))
        usuario = (evo.get("usuario") or {}).get("nome", "Sistema")
        
        try:
            detalhes_str = evo.get("detalhes", "{}")
            detalhes_json = json.loads(detalhes_str)
            
            # Captura todos os campos de texto relevantes dentro desta evolução
            itens = detalhes_json.get("itensEvolucao", [])
            has_valid_text = False
            for item in itens:
                label = item.get("label", item.get("codigo", "Informação"))
                texto = str(item.get("texto", "")).strip()
                if texto:
                    has_valid_text = True
                    linha_evo = f"\n\n[{dt_evo} | {label} | {usuario}]: {texto}"
                    historico_textos.append(linha_evo)
            
            if has_valid_text and not first_evo_found:
                data["Data do Cadastro"] = dt_evo
                data["Médico Solicitante"] = usuario
                first_evo_found = True
        except Exception:
            continue
            
    data["Histórico Quadro Clínico"] = " | ".join(historico_textos)
    
    return data

STATE_FILE = "scraper_state.json"

def load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {}

def save_state(state: Dict[str, Any]):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=4, ensure_ascii=False)

# --- PERSISTÊNCIA EM BANCO DE DADOS (RAW STORE) ---
DB_FILE = "gercon_raw_data.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS solicitacoes_raw (
            protocolo TEXT PRIMARY KEY,
            data_captura TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_alteracao INTEGER,
            conteudo_json TEXT,
            origem_lista TEXT
        )
    """)
    # Migração suave para bancos existentes (sem a coluna data_alteracao)
    try:
        cursor.execute("ALTER TABLE solicitacoes_raw ADD COLUMN data_alteracao INTEGER")
    except Exception:
        pass  # Coluna já existe
    conn.commit()
    conn.close()

def get_watermark(chave: str) -> int:
    """Retorna o timestamp (ms) da alteração mais recente já salva para esta lista.
    
    Retorna 0 se não houver registros (primeira execução = full scrape).
    """
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(data_alteracao) FROM solicitacoes_raw WHERE origem_lista = ?",
            (chave,)
        )
        result = cursor.fetchone()
        conn.close()
        return result[0] if result and result[0] else 0
    except Exception as e:
        logger.warning(f"Erro ao consultar watermark para '{chave}': {e}")
        return 0

def save_raw_batch(jsons: List[Dict[str, Any]], origem: str):
    if not jsons: return
    
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    data_to_insert = []
    
    for j in jsons:
        if not j or "error" in j: continue
        # Usa o numeroCMCE como chave única
        prot = str(j.get("numeroCMCE", "SEM_PROTOCOLO_" + str(time.time())))
        data_alt = j.get("dataAlterouUltimaSituacao")  # Timestamp em ms
        data_to_insert.append((prot, data_alt, json.dumps(j, ensure_ascii=False), origem))
        
    cursor.executemany("""
        INSERT OR REPLACE INTO solicitacoes_raw (protocolo, data_alteracao, conteudo_json, origem_lista)
        VALUES (?, ?, ?, ?)
    """, data_to_insert)
    
    conn.commit()
    conn.close()

# --- PERSISTÊNCIA EM CSV ---
def get_csv_filename(chave: str) -> str:
    return f"dados_gercon_{chave}.csv"

def init_csv(filename: str):
    if not os.path.exists(filename):
        logger.info(f"Criando novo arquivo CSV: {filename}")
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
    else:
        logger.info(f"Arquivo CSV {filename} já existe.")

def load_existing(filename: str):
    existing = {}
    if os.path.exists(filename):
        try:
            with open(filename, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("Protocolo"):
                        existing[row["Protocolo"]] = row
        except Exception as e:
            logger.warning(f"Erro ao carregar CSV existente {filename}: {e}")
    return existing

def save_all_to_csv(data_dict: Dict[str, Any], filename: str):
    temp_file = filename + ".tmp"
    try:
        with open(temp_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in data_dict.values():
                writer.writerow(row)
        os.replace(temp_file, filename)
    except Exception as e:
        logger.error(f"Erro crítico ao salvar CSV {filename}: {e}")

# --- MOTOR DE SCRAPING ---
def main():
    logger.info("==================================================")
    logger.info("Iniciando nova execução do Master Scraper...")
    logger.info("==================================================")
    
    # Inicializa o banco de dados Raw
    init_db()
    
    # Registros serão carregados por lista individualmente
    # init_csv e load_existing agora ocorrem dentro do LISTAS_ALVO loop
    
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
            filename = get_csv_filename(chave)
            
            # --- GERENCIAMENTO DE ESTADO E MODOS (ESTRATEGIA HIBRIDA) ---
            global_state = load_state()
            list_state = global_state.get(chave, {
                "full_sync_completed": False,
                "last_page": 1,
                "mode": "FULL_SYNC"
            })
            
            # Watermark para modo incremental
            watermark = get_watermark(chave)
            
            if not list_state["full_sync_completed"]:
                # Modo histórico: Oldest first para paginação estável
                sort_order_js = "params.ordenacao = ['dataAlterouUltimaSituacao'];"
                logger.info(f">>> [{nome}] Modo FULL SYNC (Ascendente) — construindo histórico estável")
                if list_state["last_page"] > 1:
                    logger.info(f"    Retomando da página {list_state['last_page']}...")
            else:
                # Modo recorrente: Newest first para velocidade
                sort_order_js = "params.ordenacao = ['-dataAlterouUltimaSituacao'];"
                wm_str = timestamp_to_date(watermark) if watermark > 0 else "N/A"
                logger.info(f">>> [{nome}] Modo INCREMENTAL (Descendente) — buscando registros após {wm_str}")

            logger.info(f"    CSV: {filename}")
            
            # Inicializa CSV específico para esta lista
            init_csv(filename)
            records = load_existing(filename)
            
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
                    page.wait_for_timeout(1000) # Deixa o Angular respirar
                else:
                    logger.warning(f"  Aba '{nome}' não encontrada ou não visível no momento.")
                    continue
            except Exception as e:
                logger.warning(f"  Aviso ao interagir com a aba '{nome}': {e}")
                continue
            
            page_num = list_state.get("last_page", 1)
            target_page_size = PAGE_SIZE
            current_page_size = target_page_size
            total_pages = None
            stop_scraping = False 
            start_time_total = time.time()
            
            while True:
                # Trava de segurança para impedir a paginação infinita
                if total_pages is not None and page_num > total_pages:
                    logger.info(f"--- Concluído: Todas as {total_pages} páginas de {nome} projetadas foram extraídas! ---")
                    
                    # Se era full sync, marca como concluído
                    if not list_state["full_sync_completed"]:
                        list_state["full_sync_completed"] = True
                        list_state["last_page"] = 1 # volta para 1 p/ o incremental futuro
                        global_state[chave] = list_state
                        save_state(global_state)
                    break

                if total_pages is not None:
                    elapsed_time = time.time() - start_time_total
                    pages_completed = page_num - 1
                    
                    cadastros_completed = pages_completed * current_page_size
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
                
                # Script JavaScript de coleta definitiva
                js_script = f"""async () => {{
                    try {{
                        if (typeof angular === 'undefined') return {{ error: "Angular não carregado" }};
                        let table = document.querySelector('table.ng-table');
                        if (!table) return {{ error: "Tabela não encontrada" }};
                        let scope = angular.element(table).scope();
                        let $http = angular.element(document.body).injector().get('$http');
                        
                        let origParams = scope.solicCtrl?.parametros?.['{chave}'];
                        if (!origParams) return {{ error: "Falta parâmetros '{chave}'" }};
                        
                        let params = angular.copy(origParams);
                        delete params.dataInicioConsulta; delete params.dataFimConsulta;
                        delete params.dataInicioAlta; delete params.dataFimAlta;
                        
                        // Força ordenação por data de alteração com prefixo obrigatório
                        {sort_order_js.replace("'dataAlterouUltimaSituacao'", "'+dataAlterouUltimaSituacao'").replace("'-dataAlterouUltimaSituacao'", "'-dataAlterouUltimaSituacao'")}
                        
                        params.pagina = {page_num};
                        params.tamanhoPagina = {current_page_size};
                        
                        let pageResponse = await $http.get('/gercon/rest/solicitacoes/paineis', {{ params: params }});
                        if (!pageResponse.data?.dados?.length) return null;
                        
                        let ids = pageResponse.data.dados.map(item => item.id);
                        let totalRegistros = pageResponse.data.totalDados || 0;
                        let totalBytesBatch = 0;
                        
                        let promises = ids.map(id => 
                            $http.get('/gercon/rest/solicitacoes/' + id, {{ transformResponse: [data => data] }})
                                .then(r => {{
                                    totalBytesBatch += new Blob([r.data || ""]).size;
                                    try {{ return JSON.parse(r.data); }} catch (e) {{ return {{ error: id }}; }}
                                }})
                                .catch(e => ({{ error: id }}))
                        );

                        const TIMEOUT_MS = 240000;
                        let timeout = new Promise((_, rej) => setTimeout(() => rej(new Error('TIMEOUT_LOTE')), TIMEOUT_MS));
                        
                        let results = await Promise.race([Promise.all(promises), timeout]);
                        return {{ jsons: results, totalDados: totalRegistros, bytesDownload: totalBytesBatch }};
                    }} catch (e) {{
                        return {{ error: e.message || e.toString() }};
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
                        page.wait_for_selector("table.ng-table tbody tr", timeout=TIMEOUT*10)
                        # Aguarda o Angular estabilizar completamente o scope antes do proximo evaluate
                        # Sem isso o contexto pode ser destruido logo no inicio do proximo evaluate
                        page.wait_for_load_state("networkidle")
                        page.wait_for_timeout(3000)
                        logger.info("Sessão recuperada. Retomando coleta...")
                        continue # tenta de novo a mesma página
                    except Exception as ex:
                        logger.error(f"Falha ao recuperar sessão: {ex}")
                        break

                if not response_data:
                    logger.info(f"--- Concluído: A fila acabou para {nome}! ---")
                    break
                
                if "error" in response_data:
                    err_msg = response_data['error']
                    # TIMEOUT_LOTE = Promise.all demorou mais de 4 min: reduz lote e retenta
                    if "TIMEOUT_LOTE" in str(err_msg):
                        new_size = max(10, current_page_size // 2)
                        logger.warning(f"  Timeout do lote JS na pág {page_num}. Reduzindo lote {current_page_size} -> {new_size} e retentando...")
                        current_page_size = new_size
                        total_pages = None  # Força recalculo total
                        continue
                    logger.error(f"Erro mapeado dentro da página: {err_msg}")
                    break
                    
                jsons = response_data.get("jsons", [])
                if not jsons:
                    logger.warning(f"Resposta incorreta ou sem 'jsons' na pág {page_num}")
                    break
                
                # NOVIDADE: Salva os JSONs brutos no SQLite antes de processar
                save_raw_batch(jsons, nome)
                
                bytes_neste_lote = response_data.get("bytesDownload", 0)
                
                globals()['payload_bytes_total'] += bytes_neste_lote
                globals()['payload_bytes_last_batch'] = bytes_neste_lote
                
                total_dados = response_data.get("totalDados", 0)
                logger.debug(f"[main_loop] Lote com {len(jsons)} registros. Fila total: {total_dados}. Download neste lote: {(bytes_neste_lote / 1024):.2f}KB.")
                
                if total_pages is None and total_dados > 0:
                    total_pages = math.ceil(total_dados / current_page_size)
                    
                novos_neste_lote = 0
                for j in jsons:
                    if j is None: continue
                    if "error" in j: continue
                    
                    # --- CONDIÇÃO DE PARADA ---
                    # No modo INCREMENTAL (Descendente), paramos ao chegar no watermark anterior.
                    # No modo FULL SYNC (Ascendente), não existe parada antecipada até o final da fila (hoje).
                    if list_state["full_sync_completed"] and watermark > 0:
                        data_alt = j.get("dataAlterouUltimaSituacao", 0) or 0
                        if data_alt > 0 and data_alt <= watermark:
                            logger.info(f"  [INCREMENTAL] Alcançou registro já conhecido ({timestamp_to_date(data_alt)}). Parando paginação.")
                            stop_scraping = True
                            break
                    
                    data = flatten_solicitacao(j, nome)
                    if data and "Protocolo" in data and data["Protocolo"]:
                        prot = data["Protocolo"]
                        cleaned_row = clean_data_row(data)
                        records[prot] = cleaned_row
                        novos_neste_lote += 1
                
                if novos_neste_lote > 0:
                    logger.debug(f"  {novos_neste_lote} registros novos/atualizados neste lote.")
                
                # Salva o arquivo CSV atualizado com a página inteira
                save_all_to_csv(records, filename)
                page_num += 1
                
                # Salva o progresso para permitir retomada exata se cair
                if not list_state["full_sync_completed"]:
                    list_state["last_page"] = page_num
                    global_state[chave] = list_state
                    save_state(global_state)

                # RECUPERAÇÃO GRADUAL: se o lote estava pequeno devido a erros, dobra a cada sucesso
                if current_page_size < target_page_size:
                    old_size = current_page_size
                    current_page_size = min(target_page_size, current_page_size * 2)
                    logger.info(f"  Lote bem-sucedido. Recuperando performance: {old_size} -> {current_page_size}")
                    total_pages = None # Recalcula progresso na proxima volta
                
                if stop_scraping:
                    logger.info(f"--- Concluído INCREMENTAL: {nome} atualizado. ---")
                    break
                
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
