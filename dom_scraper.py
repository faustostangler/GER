import os
import csv
import json
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# Configuração de Logging Verboso
file_handler = logging.FileHandler("dom_scraper.log", encoding='utf-8')
file_handler.setLevel(logging.DEBUG)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.DEBUG, # O Root Logger aceita tudo
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)

load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")

CSV_FILE = os.getenv("CSV_FILE", "dados_gercon.csv")
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")
HEADLESS = os.getenv("HEADLESS", "True").lower() == "true"
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10"))

COLUNAS = [
    "Protocolo", "Especialidade Mãe", "Especialidade", "Especialidade Descrição",
    "Especialista", "CID Código", "CID Principal", 

    "Situação da Solicitação", "Data da Solicitação",
    "Data do Primeiro Agendamento", "Pontuação", "Pontuação Cor", "Complexidade",
    "Cor do Regulador", 

    "Unidade Solicitante", "Médico Solicitante", "Médico Solicitante Email", 

    "Quadro Clínico",

    "Nome do Paciente",
    "Data de Nascimento", "Sexo", "Cor", "CPF", "Nome da Mãe", "Cartão SUS", 
    
    "Logradouro", "Número", "Complemento", "Bairro", "CEP", 
    "Nacionalidade", 
    
    "Ordem Judicial"
    
]

def init_csv():
    if not os.path.exists(CSV_FILE):
        logger.info(f"Criando novo arquivo CSV: {CSV_FILE}")
        with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
    else:
        logger.info(f"Arquivo CSV {CSV_FILE} já existe.")

def load_existing_protocols():
    logger.info("Carregando protocolos existentes do CSV...")
    existing = {}
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            try:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("Protocolo"):
                        existing[row["Protocolo"]] = row
                logger.info(f"{len(existing)} protocolos carregados com sucesso.")
            except Exception as e:
                logger.error(f"Erro ao ler CSV existente: {e}")
    else:
        logger.warning(f"Arquivo {CSV_FILE} não encontrado para carregamento inicial.")
    return existing

def clean_data_row(data):
    # Trata todos os dados iterando sobre eles. Se for string livre (texto), unifica as quebras de linha.
    logger.debug(f"[clean_data_row] Sanitizando quebras de linha para o Protocolo: {data.get('Protocolo')}")
    cleaned_row = {}
    for col in COLUNAS:
        v = data.get(col, "")
        if isinstance(v, str):
            cleaned_row[col] = v.replace("\r\n", "\n").replace("\r", "\n")
        else:
            cleaned_row[col] = v
    return cleaned_row

def save_all_to_csv(data_dict):
    # Salva todos os dados num arquivo temporário e substitui o original de uma vez (Update)
    temp_file = CSV_FILE + ".tmp"
    logger.debug(f"Salvando {len(data_dict)} registros no CSV (via arquivo temporário)...")
    try:
        with open(temp_file, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in data_dict.values():
                writer.writerow(row)
        os.replace(temp_file, CSV_FILE)
        logger.debug("CSV atualizado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar CSV: {e}")

def format_protocolo(num):
    logger.debug(f"[format_protocolo] Original: '{num}'")
    if not num: 
        logger.debug("[format_protocolo] Vazio.")
        return ""
    num = str(num)
    if len(num) == 12:
        res = f"{num[0:2]}-{num[2:4]}-{num[4:11]}-{num[11]}"
        logger.debug(f"[format_protocolo] Formatado: '{res}'")
        return res
    logger.debug(f"[format_protocolo] Mantido inalterado: '{num}'")
    return num

def timestamp_to_date(ts):
    logger.debug(f"[timestamp_to_date] Recebido: '{ts}'")
    if not ts: return ""
    try:
        dt = datetime.fromtimestamp(ts / 1000.0)
        res = dt.strftime("%d/%m/%Y %H:%M")
        logger.debug(f"[timestamp_to_date] Convertido para: '{res}'")
        return res
    except Exception as e:
        logger.debug(f"[timestamp_to_date] Falha ao converter: {e}")
        return ""

def calculate_age(ts):
    if not ts: return ""
    try:
        born = datetime.fromtimestamp(ts / 1000.0)
        today = datetime.today()
        years = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
        months = (today.month - born.month) % 12
        return f"{years} anos e {months} meses"
    except:
        return ""

def extract_data_from_json(j):
    if "error" in j:
        logger.error(f"Recebido JSON com erro: {j.get('error')}")
        return None
        
    prot_raw = j.get("numeroCMCE", "Desconhecido")
    logger.debug(f"Processando extração para protocolo bruto: {prot_raw}")
    data = {}
    
    # Protocolo
    logger.debug("[extract_data_from_json] Estruturando blocos principais (Especialidade, Situação, CIDs)...")
    data["Protocolo"] = format_protocolo(j.get("numeroCMCE", ""))
    
    especialidade = j.get("especialidade") or {}
    data["Especialidade"] = especialidade.get("descricao", "")
    data["Especialidade Descrição"] = especialidade.get("descricaoAuxiliar", "")
    
    especialidade_mae = especialidade.get("especialidadeMae") or {}
    data["Especialidade Mãe"] = especialidade_mae.get("descricao", "")
    data["Especialidade Mãe Descrição"] = especialidade_mae.get("descricaoAuxiliar", "")
    data["Especialista"] = especialidade_mae.get("cbo", {}).get("descricao", "")
    
    data["Situação da Solicitação"] = j.get("situacao", "")
    data["Complexidade"] = j.get("complexidade", "")
    data["Cor do Regulador"] = j.get("corRegulador", "")
    data["Ordem Judicial"] = j.get("liminarOrdemJudicial", "")
    
    # CIDs
    cid = j.get("cidPrincipal") or {}
    data["CID Principal"] = cid.get("descricao", "")
    data["CID Código"] = cid.get("codigo", "")
    
    data["Data da Solicitação"] = timestamp_to_date(j.get("dataSolicitacao"))
    data["Data do Cadastro"] = timestamp_to_date(j.get("dataCadastro"))
    data["Data do Primeiro Agendamento"] = timestamp_to_date(j.get("dataPrimeiroAgendamento"))
    
    # Pontuação
    class_risco = j.get("classificacaoRisco") or {}
    data["Pontuação"] = class_risco.get("totalPontos", "")
    data["Pontuação Cor"] = class_risco.get("cor", "")
         
    # Usuário
    u = j.get("usuarioSUS") or {}
    if u:
        logger.debug("[extract_data_from_json] Estruturando bloco do Paciente e Endereço...")
        data["Nome do Paciente"] = u.get("nomeCompleto", "")
        data["Nome da Mãe"] = u.get("nomeMae", "")
        data["CPF"] = u.get("cpf", "")
        data["Data de Nascimento"] = timestamp_to_date(u.get("dataNascimento")).split(" ")[0] if timestamp_to_date(u.get("dataNascimento")) else ""
        data["Sexo"] = u.get("sexo", "")
        data["Cor"] = u.get("racaCor", "")
        data["Cartão SUS"] = u.get("cartaoSus", "")
        data["Telefone"] = u.get("telefoneSMS", "")
        data["Email"] = u.get("emailContato", "")
        # data["Idade"] = calculate_age(u.get("dataNascimento"))

        data["Logradouro"] = u.get("logradouro", "")
        data["Número"] = u.get("numero", "")
        data["Complemento"] = u.get("complemento", "")
        data["Bairro"] = u.get("bairro", "")
        data["CEP"] = u.get("cep", "")

        mun_res = u.get("municipioResidencia") or {}
        data["Município de Residência"] = mun_res.get("nome", "")
        data["UF de Residência"] = mun_res.get("uf", "")

        mun_nasc = u.get("municipioNascimento") or {}
        data["Município de Nascimento"] = mun_nasc.get("nome", "")
        data["UF de Nascimento"] = mun_nasc.get("uf", "")
        
        data["Nacionalidade"] = u.get("nacionalidade", "")

    # Anamnese / Quadro Clinico  (pega da evo mais antiga que tiver)
    evolucoes = j.get("evolucoes", [])
    logger.debug(f"[extract_data_from_json] Mapeando {len(evolucoes)} blocos de evoluções para capturar Quadro Clínico...")
    evolucoes.sort(key=lambda x: x.get("data", 0)) # ordena da mais antiga para atual
    
    evo_codigos = [] # debugging purposes

    for evo in evolucoes:
        if "detalhes" in evo and evo["detalhes"]:
            try:
                # Transforma a string de detalhes em dicionário
                evo_detalhes = json.loads(evo["detalhes"])
                
                # Itera sobre cada item dentro da lista de itens daquela evolução
                for evo_item in evo_detalhes.get("itensEvolucao", []):
                    evo_codigos.append(evo_item.get("codigo")) # Salva para debug futuro
                    
                    # if evo_item.get("codigo") == "evolucao":
                    #     pass
                    
                    # Extrai o Quadro Clínico / Anamnese
                    if evo_item.get("codigo") == "anamnese":
                        data["Quadro Clínico"] = evo_item.get("texto", "")
                        
                    # Extrai a Unidade Indicada explicitamente
                    if evo_item.get("codigo") == "unidadeIndicada":
                        data["Unidade Indicada"] = evo_item.get("texto", "")
                        
                    # Extrai a Regionalização e faz o split entre Especialidade, Reg. Solicitante e Reg. Referência
                    if evo_item.get("codigo") == "regionalizacao":
                        evo_item_texto_reg = evo_item.get("texto", "")
                        for evo_item_linha in evo_item_texto_reg.split("\n"):
                            # Se a linha contiver ':', divide a linha em Chave=Valor para salvar na respectiva coluna
                            if ":" in evo_item_linha:
                                evo_item_chave, evo_item_valor = evo_item_linha.split(":", 1)
                                data[f"Regionalização {evo_item_chave.strip()}"] = evo_item_valor.strip()
                                
                    # Extrai o Diagnóstico caso exista
                    if evo_item.get("codigo") == "diagnostico":
                        data["Diagnóstico"] = evo_item.get("texto", "")
                        
                # Caso deseje parar a extração logo após achar a anamnese, descomente abaixo
                # if data.get("Quadro Clínico"):
                #     break
            except:
                pass

    # Solicitante e Regulacao
    logger.debug("[extract_data_from_json] Estruturando bloco da Unidade Solicitante e Médico Responsável...")
    usol = j.get("unidadeSolicitante") or {}
    data["Unidade Solicitante"] = usol.get("nome", "")
    data["Unidade Solicitante Descrição"] = (usol.get("tipoUnidade") or {}).get("descricao", "")
    data["Unidade Solicitante Razão Social"] = usol.get("razaoSocial", "")
    data["Município Solicitante"] = (usol.get("municipio") or {}).get("nome", "")
    data["UF Solicitante"] = (usol.get("municipio") or {}).get("uf", "")
    data["Unidade Solicitante Endereço"] = usol.get("endereco", "")
    data["Unidade Solicitante Telefone"] = usol.get("telefone", "")

    med = j.get("usuarioSolicitante") or {}
    data["Médico Solicitante"] = med.get("nome", "")
    data["Médico Solicitante Email"] = med.get("email", "")
    data["Médico Solicitante CPF"] = med.get("cpf", "")
    data["Médico Solicitante CNS"] = med.get("cns", "")

    
    data["Central de Regulação"] = (usol.get("centralRegulacao") or {}).get("nome", "")
    
    return data

def main():
    logger.info("==================================================")
    logger.info("Iniciando nova execução do GERCON Scraper...")
    logger.info("==================================================")
    init_csv()
    
    with sync_playwright() as p:
        logger.info("Iniciando navegador Playwright...")
        browser = p.chromium.launch(headless=HEADLESS, args=['--no-sandbox'])
        context = browser.new_context()
        main_page = context.new_page()
        
        logger.info("Navegando para a página de login do GERCON...")
        main_page.goto(GERCON_URL, wait_until="networkidle", timeout=30000)
        logger.debug("Preenchendo credenciais...")
        main_page.fill('#username', USER)
        main_page.fill('#password', PASS)
        main_page.click('#kc-login')
        main_page.wait_for_load_state("networkidle")
        logger.info("Login aparente realizado. Aguardando carregamento da interface inicial...")
        
        # Unidade (se houver)
        xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
        try:
            main_page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
            logger.debug("Clicando no botão de seleção de unidade (se existir)...")
            main_page.locator(f"xpath={xpath_btn}").click()
            main_page.wait_for_load_state("networkidle")
            logger.info("Unidade selecionada com sucesso.")
        except:
            logger.debug("Botão de unidade não encontrado ou não necessário. Prosseguindo...")
            pass
            
        logger.info("Acessando Menu 'Fila de Espera'...")
        xpath_item = "/html/body/div[6]/div/ul/li[4]"
        main_page.wait_for_selector(f"xpath={xpath_item}", timeout=15000)
        main_page.locator(f"xpath={xpath_item}").click()
        main_page.wait_for_load_state("networkidle")
        
        row_selector = "table.ng-table tbody tr"
        
        try:
            main_page.wait_for_selector(row_selector, timeout=20000)
            logger.info("Tabela carregada! Iniciando raspagem invisível da Fila de Espera...")
        except:
            logger.error("Tabela não carregada ou lista vazia.")
            browser.close()
            return

        page_num = 1
        page_size = PAGE_SIZE  # Podemos raspar 50 (ou até mais) por vez!
        
        existing_protocols = load_existing_protocols()
        if existing_protocols:
            logger.info(f"Encontrados {len(existing_protocols)} protocolos já salvos no CSV. Registros existentes serão atualizados.")

        import math
        total_pages = None
        start_time_total = time.time()
        last_ping_time = time.time()

        while True:
            # Trava de segurança para impedir a paginação infinita além do limite total
            if total_pages is not None and page_num > total_pages:
                logger.info(f"--- Concluído: Todas as {total_pages} páginas projetadas foram extraídas! ---")
                break

            if total_pages is not None:
                elapsed_time = time.time() - start_time_total
                pages_completed = page_num - 1
                
                cadastros_completed = pages_completed * page_size
                avg_time_per_page = elapsed_time / pages_completed if pages_completed > 0 else 0
                cadastros_por_seg = cadastros_completed / elapsed_time if elapsed_time > 0 else 0
                
                remaining_pages = total_pages - pages_completed
                
                # Previne ETA negativo caso o total de páginas flutue na API enquanto raspamos
                eta_seconds = max(remaining_pages * avg_time_per_page, 0)
                total_seconds = elapsed_time + eta_seconds
                
                percent = (pages_completed / total_pages * 100) if total_pages > 0 else 0
                
                def format_time(secs):
                    h = int(secs // 3600)
                    m = int((secs % 3600) // 60)
                    s = int(secs % 60)
                    return f"{h:02d}h{m:02d}m{s:02d}s"
                
                progress_line = f"{percent:.4f}% {page_num}+{remaining_pages} {cadastros_por_seg:.4f}/seg {format_time(elapsed_time)}+{format_time(eta_seconds)}={format_time(total_seconds)}"
                print(progress_line)
                logger.info(progress_line)
            else:
                # print(f"\n[{page_num}] Requisitando dados da página {page_num}...")
                pass
            
            # Executa script JavaScript na página que busca os IDs paginados via API
            # e depois busca os JSONs individuais via $http em paralelo!
            # Isto não afeta a navegação e é super rápido pois usa a sessão do angular
            js_script = f"""async () => {{
                let scope = angular.element(document.querySelector('table.ng-table')).scope();
                let $http = angular.element(document.body).injector().get('$http');
                
                // Pega os filtros que o usuário aplicou na interface
                let params = angular.copy(scope.solicCtrl.parametros.filaDeEspera);
                params.pagina = {page_num};
                params.tamanhoPagina = {page_size};
                
                // 1. Fetch the list of IDs for the current page
                let queryUrl = '/gercon/rest/solicitacoes/paineis';
                let pageResponse = await $http.get(queryUrl, {{ params: params }});
                
                if (!pageResponse.data || !pageResponse.data.dados || pageResponse.data.dados.length === 0) {{
                    return null; // Paginação chegou ao fim
                }}
                
                let ids = pageResponse.data.dados.map(item => item.id);
                let totalRegistros = pageResponse.data.totalDados || 0;
                
                // 2. Fetch the detailed JSON for each ID in parallel
                let results = [];
                // Usar Promise.all ou chunk para carregar os detalhes rapidamente
                let promises = ids.map(id => 
                    $http.get('/gercon/rest/solicitacoes/' + id)
                        .then(r => r.data)
                        .catch(e => ({{error: id}}))
                );
                
                results = await Promise.all(promises);
                return {{ jsons: results, totalDados: totalRegistros }};
            }}"""
            
            # print(f"   -> Lendo {page_size} registros via API de forma assíncrona...")
            
            try:
                logger.debug(f"[main_loop] Injetando payload JS na API Angular para Página {page_num}...")
                response_data = main_page.evaluate(js_script)
            except Exception as e:
                logger.warning(f"Navegação inesperada ou contexto destruído: {e}")
                logger.info("Tentando recuperar a sessão do Angular...")
                try:
                    main_page.goto(GERCON_URL, wait_until="load", timeout=60000)
                    
                    # Verifica se fomos deslogados (Sessão Expirada após ~40 mins)
                    if main_page.locator('#username').count() > 0:
                        logger.warning("Sessão expirada bloqueou as requisições! Refazendo login de forma invisível...")
                        main_page.fill('#username', USER)
                        main_page.fill('#password', PASS)
                        main_page.click('#kc-login')
                        main_page.wait_for_load_state("networkidle")
                        
                        xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
                        try:
                            main_page.wait_for_selector(f"xpath={xpath_btn}", timeout=5000)
                            main_page.locator(f"xpath={xpath_btn}").click()
                            main_page.wait_for_load_state("networkidle")
                        except:
                            pass
                            
                    logger.info("Acessando Menu Fila de Espera novamente...")
                    xpath_item = "/html/body/div[6]/div/ul/li[4]"
                    main_page.wait_for_selector(f"xpath={xpath_item}", timeout=20000)
                    main_page.locator(f"xpath={xpath_item}").click()
                    main_page.wait_for_selector("table.ng-table tbody tr", timeout=30000)
                    logger.info("Sistema reconectado com sucesso! Retomando coleta da página...")
                except Exception as ex:
                    logger.error(f"Falha ao recuperar sessão logada. Encerrando. ({ex})")
                    break
                continue
            
            if not response_data or not response_data.get("jsons"):
                logger.info("--- Concluído: A fila acabou! ---")
                break
                
            jsons = response_data["jsons"]
            total_dados = response_data.get("totalDados", 0)
            logger.debug(f"[main_loop] JS resolvido! Lote com {len(jsons)} registros capturado. Fila total aferida: {total_dados}")
            
            if total_pages is None and total_dados > 0:
                total_pages = math.ceil(total_dados / page_size)
                # print(f"   -> Total de registros detectados na fila: {total_dados} ({total_pages} páginas de {page_size} itens cada)")

            # print(f"   -> Processando {len(jsons)} registros recebidos e salvando...")
            for j in jsons:
                logger.debug(f"[main_loop] Desempacotando 1 registro do Array JSON...")
                data = extract_data_from_json(j)
                if data and "Protocolo" in data and data["Protocolo"]:
                    prot = data["Protocolo"]
                    
                    # Limpa e sempre atualiza ou insere o registro na memória
                    cleaned_row = clean_data_row(data)
                    existing_protocols[prot] = cleaned_row
                    
                else:
                    err_id = j.get("error", "Desconhecido")
                    # print(f"      [ERRO] Falha ao processar paciente ID {err_id}")
            
            # Salva o arquivo CSV atualizado com a página inteira
            save_all_to_csv(existing_protocols)
            
            page_num += 1
            
            # Ping preventivo a cada 5 minutos para não deixar a sessão do servidor expirar (Super Rápido/Invisível)
            if time.time() - last_ping_time > 1000:  # 300 segundos = 5 minutos
                try:
                    # Um simples "fetch" na URL base já renova o tempo de inatividade no servidor
                    # sem recarregar a tela, sem piscar o DOM e demorando apenas milissegundos!
                    main_page.evaluate("fetch(window.location.href).catch(() => {})")
                    last_ping_time = time.time()  # Reseta o timer
                except Exception:
                    pass
            
        browser.close()

if __name__ == "__main__":
    if USER and PASS:
        main()
    else:
        logger.error("Credenciais não configuradas!")
