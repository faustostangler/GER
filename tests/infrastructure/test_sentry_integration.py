"""
TDD: Sentry Integration Tests.

Validates that the Sentry initialization module:
1. Is a no-op when DSN is None (local dev).
2. Filters LGPD-sensitive data from breadcrumbs.
3. Doesn't crash the application on initialization failure.
"""
import pytest
from unittest.mock import patch, MagicMock

from infrastructure.telemetry.sentry import init_sentry, _filter_health_data_breadcrumb


class TestSentryInit:
    """Validates graceful Sentry initialization."""

    def test_no_op_when_dsn_is_none(self):
        """Em dev local, init_sentry não deve fazer nada."""
        # Should not raise, should not call sentry_sdk.init
        init_sentry(dsn=None, environment="local", release="test")

    def test_no_op_when_dsn_is_empty_string(self):
        """DSN vazio também é no-op."""
        init_sentry(dsn="", environment="local", release="test")

    def test_initializes_with_valid_dsn(self):
        """Com DSN válido, deve chamar sentry_sdk.init com os parâmetros corretos."""
        mock_sdk = MagicMock()
        with patch.dict("sys.modules", {"sentry_sdk": mock_sdk, "sentry_sdk.integrations.logging": MagicMock()}):
            init_sentry(
                dsn="https://key@sentry.io/123",
                environment="production",
                release="abc1234",
            )
            mock_sdk.init.assert_called_once()
            call_kwargs = mock_sdk.init.call_args
            assert call_kwargs.kwargs["dsn"] == "https://key@sentry.io/123"
            assert call_kwargs.kwargs["environment"] == "production"
            assert call_kwargs.kwargs["release"] == "gercon-analytics@abc1234"
            assert call_kwargs.kwargs["send_default_pii"] is False

    def test_does_not_crash_on_init_failure(self):
        """Se o SDK falhar, deve logar warning mas não crashar."""
        mock_sdk = MagicMock()
        mock_sdk.init.side_effect = Exception("Network error")
        with patch.dict("sys.modules", {"sentry_sdk": mock_sdk, "sentry_sdk.integrations.logging": MagicMock()}):
            # Should NOT raise
            init_sentry(
                dsn="https://key@sentry.io/123",
                environment="production",
                release="test",
            )


class TestLGPDBreadcrumbFilter:
    """Validates LGPD compliance in breadcrumb filtering."""

    def test_query_breadcrumbs_are_redacted(self):
        """SQL queries devem ser redatadas para não vazar dados de pacientes."""
        crumb = {
            "category": "query",
            "message": "SELECT * FROM gercon WHERE paciente_cpf = '123.456.789-00'",
        }
        result = _filter_health_data_breadcrumb(crumb, {})
        assert result["message"] == "[REDACTED - LGPD]"

    def test_non_query_breadcrumbs_pass_through(self):
        """Breadcrumbs que não são queries devem passar intactos."""
        crumb = {
            "category": "http",
            "message": "GET /dashboard/ 200",
        }
        result = _filter_health_data_breadcrumb(crumb, {})
        assert result["message"] == "GET /dashboard/ 200"

    def test_breadcrumb_without_category_passes_through(self):
        """Breadcrumbs sem category não devem ser filtrados."""
        crumb = {"message": "something happened"}
        result = _filter_health_data_breadcrumb(crumb, {})
        assert result["message"] == "something happened"
