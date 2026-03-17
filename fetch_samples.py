import os
import json
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv("env/creds.env")
load_dotenv("env/config.env")

USER = os.getenv("username")
PASS = os.getenv("password")
GERCON_URL = os.getenv("GERCON_URL", "https://gercon.procempa.com.br/gerconweb/")

LISTAS = [
    "agendadas",
    "pendente",
    "cancelada",
    "filaDeEspera",
    "outras"
]

all_samples = {}

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

    for target in LISTAS:
        print(f"Buscando até 10 registros para a lista: {target}")
        
        js_script = f"""async () => {{
            let scope = angular.element(document.querySelector('table.ng-table')).scope();
            let $http = angular.element(document.body).injector().get('$http');
            
            let params = angular.copy(scope.solicCtrl.parametros['{target}']);
            if (params) {{
                delete params.dataInicioConsulta; 
                delete params.dataFimConsulta;
                delete params.dataInicioAlta;
                delete params.dataFimAlta;
                
                params.pagina = 1;
                params.tamanhoPagina = 10;
                
                let queryUrl = '/gercon/rest/solicitacoes/paineis';
                let pageResponse = await $http.get(queryUrl, {{ params: params }});
                
                if (!pageResponse.data || !pageResponse.data.dados || pageResponse.data.dados.length === 0) {{
                    return [];
                }}
                
                let ids = pageResponse.data.dados.map(item => item.id);
                
                let promises = ids.map(id => 
                    $http.get('/gercon/rest/solicitacoes/' + id, {{ transformResponse: [function (data) {{ return data; }}] }})
                        .then(r => {{
                            try {{
                                return JSON.parse(r.data);
                            }} catch (e) {{
                                return {{error: id}};
                            }}
                        }})
                        .catch(e => ({{error: id}}))
                );
                
                return await Promise.all(promises);
            }}
            return [];
        }}"""
        
        try:
            results = main_page.evaluate(js_script)
            all_samples[target] = results
            print(f"Obtidos {len(results)} registros para {target}")
        except Exception as e:
            print(f"Erro ao buscar {target}: {e}")
            all_samples[target] = []

    browser.close()

with open("samples.json", "w", encoding="utf-8") as f:
    json.dump(all_samples, f, indent=2, ensure_ascii=False)
print("Amostras salvas em samples.json")
