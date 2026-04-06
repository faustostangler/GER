from streamlit.testing.v1 import AppTest
from unittest.mock import patch, MagicMock
from domain.models import AnalyticKPIs
import os

if __name__ == "__main__":
    with patch("application.use_cases.analytics_use_case.AnalyticsUseCase") as mock_uc:
        instance = MagicMock()
        instance.get_global_bounds.return_value = (1, 100)
        instance.get_dynamic_options.return_value = ["A", "B"]
        instance.get_kpis.return_value = AnalyticKPIs(
            pacientes=100, eventos=500, esp_mae=10, sub_esp=20, medicos=30,
            cids=40, origens=5, lead_time=15.5, max_lead_time=100, span_dias=30,
            pac_urgentes=10, pac_vencidos=5, p90_lead_time=45.2, p90_esquecido=60.1
        )
        mock_uc.return_value = instance

        with patch("src.infrastructure.repositories.duckdb_repository.DuckDBAnalyticsRepository") as mock_db:
            with patch.dict(os.environ, {"ENVIRONMENT": "dev", "ALLOW_UNAUTHENTICATED_DEV": "true"}):
                with patch("os.path.exists", return_value=True):
                    print("Rodando AppTest...")
                    at = AppTest.from_file("app_analytics.py").run(timeout=10)
                    print("AppTest terminou!")
                    print("Exceções:", at.exception)
