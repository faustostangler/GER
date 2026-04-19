import os
import streamlit as st
from domain.specifications import FiltroAvancadoSpecBuilder
from infrastructure.repositories.criteria_translator import DuckDBCriteriaTranslator
from presentation.components.kpi_board import render_kpi_board
from infrastructure.config import settings
from infrastructure.telemetry.sentry import init_sentry
from presentation.components.filters import clear_filter_state
from presentation.builders.sidebar_builder import build_sidebar
from presentation.components.alerts import render_amber_alert
from presentation.components.macro_strategy import render_macro_strategy
from presentation.components.clinical_intelligence import render_clinical_intelligence
from presentation.components.audit_micro import render_audit_micro
from domain.constants import MAPA_CORES_RISCO, MAPA_NOMENCLATURAS
from presentation.di_container import get_use_case, get_identity_service

def setup_ui():
    # --- 0. SENTRY INITIALIZATION (Antes de qualquer renderização) ---
    init_sentry(
        dsn=settings.SENTRY_DSN,
        environment=settings.ENVIRONMENT,
        release=settings.GIT_SHA,
    )

    # --- 1. CONFIGURAÇÃO DA PÁGINA E DX ---
    st.set_page_config(
        page_title="Gercon Analytics | RCA",
        page_icon="🎯",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    inject_custom_css()


def inject_custom_css():
    css_path = os.path.join("src", "presentation", "static", "custom_style.css")
    if os.path.exists(css_path):
        with open(css_path, "r") as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    else:
        # Fallback minimal style
        st.markdown(
            "<style>body { background-color: #020617; color: white; }</style>",
            unsafe_allow_html=True,
        )


# --- 2. INFRASTRUCTURE: USE CASE & DI ---


def get_dynamic_options(column: str, current_where: str, current_user) -> list:
    return get_use_case().get_dynamic_options(column, current_where, current_user)


@st.cache_data(ttl=3600)
def get_global_bounds(column: str, is_date=False):
    return get_use_case().get_global_bounds(column, is_date)



# --- 4.5 BFF: IDENTITY AWARE PROXY (IAP) & BFF MOCK ---
# Extracted to presentation.di_container

from presentation.middlewares.auth_middleware import render_user_widget  # noqa: E402


# --- 5. MAIN APP ---
def main():
    """Entry point for the Streamlit application.

    WHY: Follows the canonical Phase 3 pattern — three single responsibilities:
    1. setup_ui()             — Boot Infra/DX (CSS, Sentry, config page)
    2. require_authentication() — Identity Gatekeeper (IAM Adapter, ADR-006)
    3. render_dashboard(user) — Domain Execution (clinical rendering)

    No IAM logic lives here. All identity decisions are in
    adapter streamlit_auth.py (Facade + Humble Object + Implicit Strategy).
    Ref: ADR-006 — IAM Adapter Isolation.
    """
    # 1. Boot Infra/DX
    # WHY: cache_resource.clear() removed from loop — destroyed DuckDB connection
    # on every rerun, cascading expired session failures. Cache managed
    # by @st.cache_resource/@st.cache_data decorators.
    setup_ui()

    identity = get_identity_service()
    user = identity.get_current_user()

    # 3. Domain Execution
    inject_custom_css()
    # WHY: os.path.exists() returns True for directories too — when Docker bind-mounts
    # a non-existent host path, it auto-creates an empty dir. isfile() is the correct
    # Fail-Fast guard: it will catch both "missing" and "is a directory" cases.
    if not os.path.isfile(settings.OUTPUT_FILE):
        st.error(f"⚠️ Parquet database not found or invalid ({settings.OUTPUT_FILE}).")
        st.info(
            "Execute the consolidation pipeline: `docker exec ger_analytics python sqlite_to_parquet.py`"
        )
        return

    # ==========================================
    # SIDEBAR: USER IDENTITY WIDGET (IAP / Keycloak)
    # ==========================================
    logout_url = identity.get_logout_url()
    render_user_widget(user, logout_url)

    # --- DIGITAL SURGEON PREMIUM HEADER ---
    st.markdown(
        """
        <div class="sre-header-container">
            <h1 class="sre-title">GERCON SRE</h1>
            <div class="sre-subtitle">Advanced Root Cause Analysis & Prediction</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # MAPA_CORES_RISCO and MAPA_NOMENCLATURAS are now imported from domain.constants

    builder = FiltroAvancadoSpecBuilder()
    curr_where = "1=1"
    use_case = get_use_case()

    # ==========================================
    # SRE FIX: MASTER DICTIONARY (KEEPS UI/UX ORDER CONSISTENCY)
    # ==========================================
    # 🪄 A mágica arquitetural: Centenas de linhas viraram uma só.
    ui_filters, state_keys, curr_where = build_sidebar(use_case, builder, st.session_state.user)


    # ==========================================
    # VISUALIZE AND CLEAR ACTIVE FILTERS (TOP BAR)
    # ==========================================
    has_active_filters = any(len(v) > 0 for v in ui_filters.values())

    if has_active_filters:
        total_count = sum(len(v) for v in ui_filters.values())

        with st.expander(f"🔍 Active Filters ({total_count})", expanded=True):
            for category, filters in ui_filters.items():
                if filters:
                    # 1. TITLE ON ITS OWN LINE
                    st.markdown(
                        f"<div class='cat-title'>{category}</div>",
                        unsafe_allow_html=True,
                    )

                    # 2. FILTERS GROUPED ON THE NEXT LINE
                    with st.container():
                        st.markdown(
                            "<div class='filter-row-marker' style='display:none;'></div>",
                            unsafe_allow_html=True,
                        )
                        for i, f in enumerate(filters):
                            st.button(
                                f"{f['text']}",
                                key=f"clr_item_{category}_{i}",
                                on_click=clear_filter_state,
                                args=(f["keys"],),
                            )

            # 3. CLEAR ALL ISOLATED AT THE END
            st.write("")  # Natural micro-spacing
            all_keys = [key for sublist in state_keys.values() for key in sublist]
            st.button(
                "🗑️ Clear All Filters",
                key="btn_clear_all",
                on_click=clear_filter_state,
                args=(all_keys,),
                type="primary",
            )

        st.write(" ")  # A micro-spacing right before KPIs to breathe

    # ==========================================
    # DASHBOARD TABS: STRUCTURED BY DECISION LEVEL
    # ==========================================
    # SRE FIX: Added the KPIs tab as the first one (t_kpi) for Executive Summary
    t_kpi, t_macro, t_clin, t_micro = st.tabs(
        [
            "📊 Overview (KPIs)",
            "📈 Strategy (Macro)",
            "🩺 Clinical Intelligence",
            "🔎 Audit (Micro)",
        ]
    )

    # ==========================================
    # CLÁUSULA FINAL E PROCESSAMENTO (KPIs)
    # ==========================================
    # WHY: `builder` (FiltroAvancadoSpecBuilder) accumulates all semantic filter specs
    # as the sidebar renders each widget. builder.build() produces the FiltroAvancadoSpec
    # that the DuckDBCriteriaTranslator converts to SQL inside get_kpis().
    # FINAL_WHERE (string) is kept for raw-SQL tab queries that bypass the use case.
    # The old `clauses` list was removed during the builder migration — filters now
    # flow exclusively through the semantic Specification pattern (ADR-004).
    filters = builder.build()
    FINAL_WHERE = DuckDBCriteriaTranslator.translate(filters)



    with st.spinner(
        "Processing Read Model (OLAP) and Tail Latency (P90)..."
    ):
        dashboard_state = use_case.get_executive_summary(filters, st.session_state.user)
        kpi_data = dashboard_state.kpis
        policy = dashboard_state.policy

    # --- Amber Alert: Data Freshness SLA Monitor ---
    # WHY: Delegates to is_stale() (Domain layer) — no arithmetic here (Humble Object).
    render_amber_alert(kpi_data, policy)

    # ==========================================
    # ABA 1: VISÃO GERAL (EXECUTIVE SUMMARY)
    # ==========================================
    with t_kpi:
        render_kpi_board(kpi_data, st, policy=policy)

        # --- BLOCO 2 CONSOLIDADO: ANATOMIA COMPARATIVA E RISCO ---
        df_dist = use_case.get_distribution_analysis(filters, st.session_state.user)
        from presentation.components.comparative_anatomy import render_comparative_anatomy
        render_comparative_anatomy(df_dist, kpi_data)

        st.divider()

    with t_macro:
        render_macro_strategy(
            use_case=use_case,
            filters=filters,
            FINAL_WHERE=FINAL_WHERE,
            MAPA_NOMENCLATURAS=MAPA_NOMENCLATURAS,
            MAPA_CORES_RISCO=MAPA_CORES_RISCO,
        )

    with t_clin:
        render_clinical_intelligence(
            use_case=use_case,
            filters=filters,
            FINAL_WHERE=FINAL_WHERE,
            MAPA_NOMENCLATURAS=MAPA_NOMENCLATURAS,
        )

    with t_micro:
        render_audit_micro(
            use_case=use_case,
            filters=filters,
            FINAL_WHERE=FINAL_WHERE,
            MAPA_CORES_RISCO=MAPA_CORES_RISCO,
        )


if __name__ == "__main__":
    main()
