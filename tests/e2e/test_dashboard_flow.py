import os
import subprocess
import pytest
from playwright.async_api import async_playwright, expect

# WHY: Standardize on async but avoid pytest-playwright fixture conflicts
pytestmark = pytest.mark.asyncio


def wait_for_streamlit_sync(url="http://localhost:8509/_stcore/health"):
    import requests
    import time
    for _ in range(15):
        try:
            response = requests.get(url, timeout=1)
            response.raise_for_status()
            return
        except Exception:
            time.sleep(1)
    raise Exception("Streamlit not ready")


@pytest.fixture(scope="module")
def streamlit_server():
    env = os.environ.copy()
    env["ENVIRONMENT"] = "dev"  # Bypass IAP Proxy (1/2)
    env["ALLOW_UNAUTHENTICATED_DEV"] = "true"  # Bypass IAP Proxy (2/2) — guarda dupla
    env["PYTHONPATH"] = "src"

    # Start the Streamlit application in a background process
    process = subprocess.Popen(
        [
            "uv",
            "run",
            "streamlit",
            "run",
            "app_analytics.py",
            "--server.headless",
            "true",
            "--server.port",
            "8509",
        ],
        env=env,
    )

    # Wait for the healthcheck to be successful
    try:
        wait_for_streamlit_sync()
    except Exception as e:
        process.kill()
        raise e

    yield "http://localhost:8509"

    # Teardown
    process.terminate()
    process.wait(timeout=5)


async def test_dashboard_e2e_flow(streamlit_server: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context()
        page = await context.new_page()

        # Navigate to the local server
        await page.goto(streamlit_server)

        # Wait for the main app container to load
        app_container = page.locator(".block-container").first
        await expect(app_container).to_be_visible(timeout=30000)

        # Assert that a metric eventually gets rendered
        metric = page.locator(".kpi-value").first
        try:
            await expect(metric).to_be_visible(timeout=30000)
        except Exception as e:
            body_text = await page.locator("body").inner_text()
            print("Page text content:", body_text)
            raise e

        # Verify that the title is loaded
        await expect(page).to_have_title("Gercon Analytics | RCA", timeout=5000)

        await browser.close()
