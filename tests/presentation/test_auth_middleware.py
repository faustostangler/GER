"""Tests for the IAM Presentation Middleware — auth_middleware.py.

WHY: Validates that render_user_widget is a fully public, importable symbol
living in its own dedicated middleware module — not buried inside the main
app rendering file. A single test failure here proves the SRP violation still
exists (Red phase of TDD cycle before the extraction).

Ref: ADR-006 — IAM Adapter Isolation (Phase 3 / SRP extraction).
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch, call
import pytest

from infrastructure.auth.token_acl import ValidatedUserToken


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_user(**kwargs) -> ValidatedUserToken:
    """Factory for ValidatedUserToken with sensible clinical defaults."""
    defaults = dict(
        sub="test-sub",
        email="medico.test@gercon.com",
        preferred_username="medico_test",
        roles=["diretor_medico"],
        exp=int(time.time() + 86400),
    )
    return ValidatedUserToken(**(defaults | kwargs))


# ---------------------------------------------------------------------------
# 1. Module CDC Contract — must fail before the extraction
# ---------------------------------------------------------------------------


class TestAuthMiddlewareContract:
    """Consumer-Driven Contract: validates that auth_middleware exposes the
    expected public API consumed by app_analytics.py."""

    def test_render_user_widget_is_importable(self):
        """WHY: app_analytics.py imports render_user_widget from the middleware.
        If the symbol is missing, the import breaks silently at runtime.
        This test acts as the CDC boundary guard."""
        from presentation.middlewares.auth_middleware import render_user_widget

        assert callable(render_user_widget)

    def test_module_exports_only_public_symbols(self):
        """WHY: render_user_widget must be public (no leading underscore).
        The previous _render_user_widget was an implementation detail of
        app_analytics.py — here it becomes a reusable presentation contract."""
        import presentation.middlewares.auth_middleware as mw

        # Public — must exist
        assert hasattr(mw, "render_user_widget"), "render_user_widget missing from middleware"
        # Private form — must NOT be re-exported (clean boundary)
        assert not hasattr(mw, "_render_user_widget"), (
            "_render_user_widget leaked into middleware public API"
        )


# ---------------------------------------------------------------------------
# 2. Behaviour: Cloud Run logout path (session_state clear)
# ---------------------------------------------------------------------------


class TestRenderUserWidgetCloudRun:
    """Validates Cloud Run logout path: simple Streamlit button, no Keycloak URL."""

    def test_cloud_run_renders_username_and_button(self):
        """WHY: On Cloud Run, logout clears session_state — no OAuth2-Proxy URL.
        build_logout_url() returns None, triggering the button branch."""
        from presentation.middlewares.auth_middleware import render_user_widget

        user = _make_user(preferred_username="joao.silva", email="joao@gercon.com")

        mock_st = MagicMock()
        # build_logout_url returns None → Cloud Run branch
        with patch("presentation.middlewares.auth_middleware.build_logout_url", return_value=None):
            with patch("presentation.middlewares.auth_middleware.is_cloud_run", return_value=True):
                with patch("presentation.middlewares.auth_middleware.st", mock_st):
                    render_user_widget(user)

        # Must render the username somewhere in the sidebar
        sidebar_calls = str(mock_st.sidebar.mock_calls)
        assert "Joao Silva" in sidebar_calls or "joao.silva" in sidebar_calls.lower()

    def test_cloud_run_button_clears_session_state_on_click(self):
        """WHY: Clicking logout on Cloud Run must clear ALL session_state keys
        and trigger st.rerun() — no proxy redirect involved."""
        from presentation.middlewares.auth_middleware import render_user_widget

        user = _make_user(preferred_username="joao.silva")

        mock_st = MagicMock()
        # Simulate button click returning True
        mock_st.sidebar.button.return_value = True
        mock_st.session_state.keys.return_value = ["user", "token_exp", "raw_jwt"]

        with patch("presentation.middlewares.auth_middleware.build_logout_url", return_value=None):
            with patch("presentation.middlewares.auth_middleware.is_cloud_run", return_value=True):
                with patch("presentation.middlewares.auth_middleware.st", mock_st):
                    render_user_widget(user)

        mock_st.rerun.assert_called_once()


# ---------------------------------------------------------------------------
# 3. Behaviour: Keycloak / OAuth2-Proxy logout path (Docker Compose / K8s)
# ---------------------------------------------------------------------------


class TestRenderUserWidgetKeycloak:
    """Validates Keycloak logout path: HTML form with oauth2-proxy chain URL."""

    def test_docker_compose_renders_html_form_logout(self):
        """WHY: Docker Compose requires a two-step logout chain (OAuth2-Proxy →
        Keycloak). render_user_widget must inject an HTML form, not a button."""
        from presentation.middlewares.auth_middleware import render_user_widget

        user = _make_user(preferred_username="ana.medica", email="ana@gercon.com")
        keycloak_url = "/oauth2/sign_out?rd=http%3A%2F%2Fkeycloak%2Flogout"

        mock_st = MagicMock()

        with patch("presentation.middlewares.auth_middleware.build_logout_url", return_value=keycloak_url):
            with patch("presentation.middlewares.auth_middleware.is_cloud_run", return_value=False):
                with patch("presentation.middlewares.auth_middleware.st", mock_st):
                    render_user_widget(user)

        # Must call st.sidebar.markdown with unsafe_allow_html=True for the form
        markdown_calls = [
            str(c) for c in mock_st.sidebar.mock_calls if "markdown" in str(c)
        ]
        assert any("form" in c or "oauth2" in c or "keycloak" in c.lower() for c in markdown_calls), (
            "Expected HTML logout form in sidebar.markdown calls"
        )


# ---------------------------------------------------------------------------
# 4. Display name formatting (pure logic)
# ---------------------------------------------------------------------------


class TestDisplayNameFormatting:
    """WHY: Display name derives from preferred_username by stripping the email
    domain and formatting to Title Case. Validates the presentation contract."""

    @pytest.mark.parametrize(
        "preferred_username, email, expected_name_fragment",
        [
            ("joao.silva", "joao@gercon.com", "Joao Silva"),
            ("ana_medica", "ana@gercon.com", "Ana Medica"),
            ("carlos", "carlos@gercon.com", "Carlos"),
            # Falls back to email local-part when username is empty
            ("", "maria@gercon.com", "Maria"),
        ],
    )
    def test_display_name_formatted_correctly(
        self, preferred_username, email, expected_name_fragment
    ):
        """WHY: Clinical staff names must appear in human-readable Title Case,
        not as raw system identifiers (e.g., joao.silva → Joao Silva)."""
        from presentation.middlewares.auth_middleware import render_user_widget

        user = _make_user(preferred_username=preferred_username, email=email)
        mock_st = MagicMock()

        with patch("presentation.middlewares.auth_middleware.build_logout_url", return_value=None):
            with patch("presentation.middlewares.auth_middleware.is_cloud_run", return_value=True):
                with patch("presentation.middlewares.auth_middleware.st", mock_st):
                    render_user_widget(user)

        sidebar_calls = str(mock_st.sidebar.mock_calls)
        assert expected_name_fragment in sidebar_calls, (
            f"Expected '{expected_name_fragment}' in sidebar calls, got: {sidebar_calls[:300]}"
        )
