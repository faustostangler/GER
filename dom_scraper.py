import os
import csv
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv(".env/creds.env")

USER = os.getenv("username")
PASS = os.getenv("password")

CSV_FILE = "dados_gercon.csv"

# COLUNAS = [
#     "Protocolo", "Especialidade", "Especialidade Descrição", "Especialidade Mãe", "Especialidade Mãe Descrição", "Especialista", 
#     "Situação da Solicitação", "Complexidade", "Cor do Regulador", "Ordem Judicial",
#     "CID Principal", "CID Código", 
#     "Data da Solicitação", "Data do Cadastro", "Data do Primeiro Agendamento",
#     "Tempo na Fila de Espera", "Média de Espera nesta Fila", 
#     "Pontuação", "Pontuação Cor", 
#     "Nome do Paciente", "Nome da Mãe", "CPF", "Data de Nascimento", "Idade", "Sexo", "Cor", 
#     "Cartão SUS", "Telefone", "Email", 
#     "Logradouro", "Número", "Complemento", "Bairro", "CEP", 
#     "Município de Residência", "UF de Residência", 
#     "Município de Nascimento", "UF de Nascimento", "Nacionalidade",
#     "Quadro Clínico", "Unidade Indicada", "Regionalização", "Diagnóstico",
#     "Regionalização Especialidade", "Regionalização Solicitante", "Regionalização Referência",
#     "Unidade Solicitante", "Unidade Solicitante Descrição", "Unidade Solicitante Razão Social",
#     "Município Solicitante", "UF Solicitante", "Unidade Solicitante Endereço", "Unidade Solicitante Telefone",
#     "Médico Solicitante", "Médico Solicitante Email", "Médico Solicitante CPF", "Médico Solicitante CNS",
#     "Central de Regulação", "Central de Regulação de Origem", "Unidade de Referência"
# ]

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
        with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
            writer.writeheader()

def load_existing_protocols():
    existing = set()
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            try:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("Protocolo"):
                        existing.add(row["Protocolo"])
            except:
                pass
    return existing

def save_to_csv(data):
    # Trata todos os dados iterando sobre eles. Se for string livre (texto), unifica as quebras de linha.
    cleaned_row = {}
    for col in COLUNAS:
        v = data.get(col, "")
        if isinstance(v, str):
            cleaned_row[col] = v.replace("\r\n", "\n").replace("\r", "\n")
        else:
            cleaned_row[col] = v
            
    with open(CSV_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLUNAS, quoting=csv.QUOTE_ALL)
        writer.writerow(cleaned_row)

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
        return None
        
    data = {}
    
    # Protocolo
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
    print("Iniciando Extração Acelerada via API Angular...")
    init_csv()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = browser.new_context()
        main_page = context.new_page()
        
        print("Navegando e logando...")
        main_page.goto("https://gercon.procempa.com.br/gerconweb/", wait_until="networkidle", timeout=30000)
        main_page.fill('#username', USER)
        main_page.fill('#password', PASS)
        main_page.click('#kc-login')
        main_page.wait_for_load_state("networkidle")
        
        # Unidade (se houver)
        xpath_btn = "/html/body/div[5]/div/div[1]/form/div[2]/span/button"
        try:
            main_page.wait_for_selector(f"xpath={xpath_btn}", timeout=10000)
            main_page.locator(f"xpath={xpath_btn}").click()
            main_page.wait_for_load_state("networkidle")
        except:
            pass
            
        print("Acessando Menu Fila de Espera...")
        xpath_item = "/html/body/div[6]/div/ul/li[4]"
        main_page.wait_for_selector(f"xpath={xpath_item}", timeout=15000)
        main_page.locator(f"xpath={xpath_item}").click()
        main_page.wait_for_load_state("networkidle")
        
        row_selector = "table.ng-table tbody tr"
        
        try:
            main_page.wait_for_selector(row_selector, timeout=20000)
            print("Tabela carregada! Iniciando raspagem invisível da Fila de Espera...")
        except:
            print("Tabela não carregada ou lista vazia.")
            browser.close()
            return

        page_num = 1
        page_size = 10  # Podemos raspar 50 (ou até mais) por vez!
        
        existing_protocols = load_existing_protocols()
        if existing_protocols:
            print(f"Encontrados {len(existing_protocols)} protocolos já salvos no CSV. Duplicatas serão ignoradas.")

        import math
        total_pages = None
        start_time_total = time.time()

        while True:
            # Trava de segurança para impedir a paginação infinita além do limite total
            if total_pages is not None and page_num > total_pages:
                print(f"--- Concluído: Todas as {total_pages} páginas projetadas foram extraídas! ---")
                break

            if total_pages is not None:
                elapsed_time = time.time() - start_time_total
                pages_completed = page_num - 1
                avg_time_per_page = elapsed_time / pages_completed if pages_completed > 0 else 0
                remaining_pages = total_pages - pages_completed
                
                # Previne ETA negativo caso o total de páginas flutue na API enquanto raspamos
                eta_seconds = max(remaining_pages * avg_time_per_page, 0)
                
                print(f"\n[{page_num}/{total_pages}] Buscando página {page_num}...")
                print(f"   -> Tempo decorrido: {int(elapsed_time//60)}m {int(elapsed_time%60)}s | ETA: {int(eta_seconds//60)}m {int(eta_seconds%60)}s")
            else:
                print(f"\n[{page_num}] Requisitando dados da página {page_num}...")
            
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
                response_data = main_page.evaluate(js_script)
            except Exception as e:
                print(f"   -> [AVISO] Navegação inesperada ou contexto destruído: {e}")
                print("   -> Tentando recuperar a sessão do Angular...")
                try:
                    main_page.goto("https://gercon.procempa.com.br/gerconweb/", wait_until="load", timeout=60000)
                    
                    # Verifica se fomos deslogados (Sessão Expirada após ~40 mins)
                    if main_page.locator('#username').count() > 0:
                        print("   -> Sessão expirada bloqueou as requisições! Refazendo login de forma invisível...")
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
                            
                    print("   -> Acessando Menu Fila de Espera novamente...")
                    xpath_item = "/html/body/div[6]/div/ul/li[4]"
                    main_page.wait_for_selector(f"xpath={xpath_item}", timeout=20000)
                    main_page.locator(f"xpath={xpath_item}").click()
                    main_page.wait_for_selector("table.ng-table tbody tr", timeout=30000)
                    print("   -> Sistema reconectado com sucesso! Retomando coleta da página...")
                except Exception as ex:
                    print(f"   -> Falha ao recuperar sessão logada. Encerrando. ({ex})")
                    break
                continue
            
            if not response_data or not response_data.get("jsons"):
                print("--- Concluído: A fila acabou! ---")
                break
                
            jsons = response_data["jsons"]
            total_dados = response_data.get("totalDados", 0)
            
            if total_pages is None and total_dados > 0:
                total_pages = math.ceil(total_dados / page_size)
                print(f"   -> Total de registros detectados na fila: {total_dados} ({total_pages} páginas de {page_size} itens cada)")

            print(f"   -> Processando {len(jsons)} registros recebidos e salvando...")
            for j in jsons:
                data = extract_data_from_json(j)
                if data and "Protocolo" in data and data["Protocolo"]:
                    prot = data["Protocolo"]
                    if prot in existing_protocols:
                        # Ignora silenciosamente ou avisa se preferir
                        # print(f"      [PULADO] {prot} já existente")
                        continue
                        
                    save_to_csv(data)
                    existing_protocols.add(prot) # Registra pra não duplicar futuramente
                    # print(f"      [OK] {prot} {data['Especialidade']} | {data.get('Nome do Paciente', '')}")
                else:
                    err_id = j.get("error", "Desconhecido")
                    print(f"      [ERRO] Falha ao processar paciente ID {err_id}")
            
            page_num += 1
            
        browser.close()

if __name__ == "__main__":
    if USER and PASS:
        main()
    else:
        print("Credenciais não configuradas!")
