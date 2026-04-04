import os
import subprocess
import pytest
import requests
import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt
from playwright.sync_api import Page, expect

@pytest.fixture(scope="module", autouse=True)
def seed_test_database():
    """Gera um arquivo Parquet válido para o Streamlit consumir no E2E."""
    test_file = "gercon_consolidado.parquet"
    
    # Criação do DataFrame mínimo que satisfaz o Data Contract do DuckDB
    df = pd.DataFrame({
        "numeroCMCE": ["E2E-001", "E2E-002"],
        "entidade_classificacaoRisco_cor": ["VERMELHO", "AMARELO"],
        "entidade_especialidade_descricao": ["Cardiologia", "Ortopedia"],
        "dataSolicitacao": ["2026-04-01T10:00:00Z", "2026-04-02T10:00:00Z"],
        "dataCadastro": ["2026-04-01T09:00:00Z", "2026-04-02T09:00:00Z"],
        # --- COLUNAS ANTECIPADAS PARA O DUCKDB / UI ---
        "situacao": ["PENDENTE", "AGENDADO"],
        "idade": [45, 60],
        "municipio_residencia": ["Porto Alegre", "Canoas"],
        "origem": ["UBS A", "UBS B"],
        "sexo": ["M", "F"],
        "lead_time": [10, 25],
        "entidade_especialidade_especialidadeMae_descricao": ["Clínica Médica", "Cirurgia"]
    })
    
    # Salva no disco de forma determinística
    df.to_parquet(test_file)
    
    yield # O teste E2E roda aqui e consome o arquivo real
    
    # Cleanup: remove o arquivo efêmero após os testes
    if os.path.exists(test_file):
        os.remove(test_file)

@retry(wait=wait_fixed(1), stop=stop_after_attempt(15))
def wait_for_streamlit(url="http://localhost:8509/_stcore/health"):
    try:
        response = requests.get(url, timeout=1)
        response.raise_for_status()
    except Exception as e:
        raise Exception(f"Streamlit not ready: {e}")

@pytest.fixture(scope="module")
def streamlit_server():
    env = os.environ.copy()
    env["ENVIRONMENT"] = "dev"  # Bypass IAP Proxy
    env["PYTHONPATH"] = "src"
    
    # Start the Streamlit application in a background process
    process = subprocess.Popen(
        ["uv", "run", "streamlit", "run", "app_analytics.py", "--server.headless", "true", "--server.port", "8509"],
        env=env
    )
    
    # Wait for the healthcheck to be successful
    try:
        wait_for_streamlit()
    except Exception as e:
        process.kill()
        raise e
        
    yield "http://localhost:8509"
    
    # Teardown
    process.terminate()
    process.wait(timeout=5)

def test_dashboard_e2e_flow(page: Page, streamlit_server: str):
    # Navigate to the local server
    page.goto(streamlit_server)
    
    # Wait for the main app container to load
    app_container = page.locator(".block-container").first
    expect(app_container).to_be_visible(timeout=30000)
    
    # Assert that a metric eventually gets rendered
    metric = page.locator("[data-testid='stMetricValue']").first
    try:
        expect(metric).to_be_visible(timeout=30000)
    except Exception as e:
        print("Page text content:", page.locator("body").inner_text())
        raise e
    
    # Verify that the title is loaded
    expect(page).to_have_title("Gercon Analytics | RCA", timeout=5000)

