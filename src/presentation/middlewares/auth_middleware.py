"""IAM Presentation Middleware — User Identity Widget.

WHY: Extracted from ``app_analytics.py`` to enforce the Single Responsibility
Principle. The main Streamlit entry point had at least 6 distinct reasons to
change; the identity widget rendering is now a dedicated, testable concern.

Responsibility boundary:
- This module ONLY renders the user identity card in the sidebar.
- It reads *what to render* from an already-authenticated ``ValidatedUserToken``.
- It reads *how to build the logout URL* from the IAM Adapter (streamlit_auth.py).
- It makes zero decisions about authentication — that is the adapter's contract.

Consumers:
- ``app_analytics.py`` → ``from presentation.middlewares.auth_middleware import render_user_widget``

Ref: ADR-006 — IAM Adapter Isolation (Phase 3 / SRP extraction).
"""
from __future__ import annotations

import streamlit as st
from typing import Optional

from infrastructure.auth.token_acl import ValidatedUserToken


def render_user_widget(user: ValidatedUserToken, logout_url: Optional[str]) -> None:
    """Renders the authenticated user identity card at the top of the sidebar.

    WHY: Adapts logout flow depending on the runtime via ``logout_url``
    from the IAM Adapter — zero runtime-detection logic lives here.

    - **Cloud Run**: simple Streamlit button that clears ``session_state``.
    - **Docker Compose / K8s**: HTML form posting to the OAuth2-Proxy → Keycloak
      two-step logout chain (``/oauth2/sign_out?rd=<keycloak_logout_url>``).

    Args:
        user: Validated domain token carrying clinical identity claims.
        logout_url: The precomputed logout URL from the identity service. If None, assumes Cloud Run logout.

    Ref: ADR-006 — IAM Adapter Isolation.
    """
    username = getattr(user, "preferred_username", None) or getattr(user, "email", "?")
    display_name = username.split("@")[0].replace(".", " ").replace("_", " ").title()

    if logout_url is None:
        # Cloud Run: Simple logout — clears Streamlit session_state (no proxy/Keycloak).
        st.sidebar.markdown(f"👤 **{display_name}**")
        if st.sidebar.button("🚪 Logout", use_container_width=True, key="cloud_run_logout"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    else:
        # Docker Compose / K8s: Logout with OAuth2-Proxy → Keycloak redirect chain.
        # WHY: Logout requires two steps — clear OAuth2-Proxy cookie AND destroy
        # Keycloak SSO session. build_logout_url() already builds the complete
        # chain with post_logout_redirect_uri encoded into the ``rd`` parameter.
        rd_value = logout_url.split("rd=", 1)[1] if "rd=" in logout_url else ""
        form_action = logout_url.split("?")[0]
        st.sidebar.markdown(
            f"""
            <form action="{form_action}" method="GET" style="margin: 10px 0;">
                <input type="hidden" name="rd" value="{rd_value}" />
                <button type="submit" style="
                    display: block;
                    width: 100%;
                    text-align: center;
                    background-color: transparent;
                    color: #ef4444;
                    text-decoration: none;
                    padding: 8px 12px;
                    border-radius: 8px;
                    font-weight: 500;
                    font-size: 0.9rem;
                    border: 1px solid #ef4444;
                    cursor: pointer;
                    font-family: 'Source Sans Pro', sans-serif;
                    transition: all 0.2s ease-in-out;
                ">
                    🚨 Logout &mdash; {display_name}
                </button>
            </form>
            """,
            unsafe_allow_html=True,
        )

    st.sidebar.divider()
