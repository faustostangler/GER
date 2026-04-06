import os
import subprocess
import pytest
import requests
from tenacity import retry, wait_fixed, stop_after_attempt
from playwright.sync_api import Page, expect
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
    env["ENVIRONMENT"] = "dev"             # Bypass IAP Proxy (1/2)
    env["ALLOW_UNAUTHENTICATED_DEV"] = "true"  # Bypass IAP Proxy (2/2) — guarda dupla
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

