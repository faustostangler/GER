import os
import streamlit as st
from domain.specifications import FiltroAvancadoSpecBuilder
from infrastructure.repositories.criteria_translator import DuckDBCriteriaTranslator
from presentation.components.kpi_board import render_kpi_board
from infrastructure.config import settings
from infrastructure.telemetry.sentry import init_sentry
from presentation.components.filters import clear_filter_state, render_include_exclude, render_boolean_radio, render_presence_radio, render_dual_slider, render_age_slider, render_smart_date_range, render_advanced_text_search, render_outcome_type_filter, render_pending_reasons_filter
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
    ui_filters = {
        "🩺 Clinical & Regulation": [],
        "🏛️ Governance & Actors": [],
        "📅 Lifecycle (Dates)": [],
        "🌍 Demographics & Network": [],
        "⚠️ Triage & Risk Classification": [],
        "🎯 Outcomes, Bottlenecks & SLA": [],
    }
    state_keys = {k: [] for k in ui_filters.keys()}

    # ==========================================
    # CASCADING SIDEBAR (OPTIMIZED TOP-DOWN FLOW)
    # ==========================================
    st.sidebar.header("🎛️ Cascading Filters")

    cat = "🩺 Clinical & Regulation"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude(use_case, 
            "Parent Specialty",
            "entidade_especialidade_especialidadeMae_descricao", builder,
            curr_where,
            "espm",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Fine Specialty",
            "entidade_especialidade_descricao", builder,
            curr_where,
            "espf",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "CBO Specialty",
            "entidade_especialidade_cbo_descricao", builder,
            curr_where,
            "esp_cbo",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Auxiliary Description",
            "entidade_especialidade_descricaoAuxiliar", builder,
            curr_where,
            "esp_aux",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Requesting Doctor",
            "medicoSolicitante", builder,
            curr_where,
            "med_sol",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Operating Unit",
            "entidade_unidadeOperador_nome", builder,
            curr_where,
            "usol",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Main ICD (Code)",
            "entidade_cidPrincipal_codigo", builder,
            curr_where,
            "cid_cod",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_advanced_text_search(
            "Main ICD (Description)",
            "entidade_cidPrincipal_descricao", builder,
            "txt_cid_desc",
            ui_filters[cat],
            state_keys[cat],
        )
        # MOVED CLINICAL MAGIC: Aggregation by integer numeroCMCE
        st.markdown("---")
        curr_where = render_advanced_text_search(
            "Patient Evolutions",
            "historico_quadro_clinico", builder,
            "txt_evo",
            ui_filters[cat],
            state_keys[cat],
            aggregate_by="numeroCMCE",
        )
        curr_where = DuckDBCriteriaTranslator.translate(builder.build())

    cat = "🏛️ Governance & Actors"
    with st.sidebar.expander(cat, expanded=False):
        # Actors moved from the old Evolutions tab
        curr_where = render_advanced_text_search(
            "Information Type",
            "historico_evolucoes_completo", builder,
            "txt_tinf",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_advanced_text_search(
            "Information Source",
            "evolucoes_json", builder,
            "txt_orig_inf",
            ui_filters[cat],
            state_keys[cat],
        )
        st.markdown("---")

        curr_where = render_include_exclude(use_case, 
            "Source (List)",
            "origem_lista", builder,
            curr_where,
            "lst",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
            default_in=["Fila de Espera"],
        )
        curr_where = render_include_exclude(use_case, 
            "Current Situation",
            "situacao", builder,
            curr_where,
            "sit",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Regulation Type",
            "entidade_especialidade_tipoRegulacao", builder,
            curr_where,
            "treg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Active Specialty",
            "entidade_especialidade_ativa", builder,
            curr_where,
            "stesp",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_presence_radio(
            "Injunction / Court Order",
            "liminarOrdemJudicial", builder,
            "oj",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Operator",
            "operador_nome", builder,
            curr_where,
            "op_nome",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Requesting User",
            "usuarioSolicitante_nome", builder,
            curr_where,
            "usu_sol_nome",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Regulation Center",
            "entidade_centralRegulacao_nome", builder,
            curr_where,
            "cent_reg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Operating Unit Reg. Center",
            "entidade_unidadeOperador_centralRegulacao_nome", builder,
            curr_where,
            "uni_op_cent",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Reference Unit",
            "entidade_unidadeReferencia_nome", builder,
            curr_where,
            "uni_ref",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_boolean_radio(
            "Has DITA",
            "entidade_possuiDita", builder,
            "dita",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Outside Regionalization",
            "entidade_foraDaRegionalizacao", builder,
            "freg",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Access Regularization",
            "regularizacaoAcesso", builder,
            "reg_acc",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Accepts Teleconsultation",
            "entidade_especialidade_teleconsulta", builder,
            "tele",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Matrix Support",
            "entidade_especialidade_matriciamento", builder,
            "matri",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Unclassified",
            "entidade_semClassificacao", builder,
            "sem_class",
            ui_filters[cat],
            state_keys[cat],
        )

        curr_where = DuckDBCriteriaTranslator.translate(builder.build())

    cat = "📅 Lifecycle (Dates)"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_smart_date_range(
            "Request Date",
            "dataSolicitacao", builder,
            "dt_solic",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Registration Date",
            "dataCadastro", builder,
            "dt_cad",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Evolution Date",
            "dataCadastro", builder,
            "dt_evo",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "First Appointment",
            "dataPrimeiroAgendamento", builder,
            "dt_pagend",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "First Authorization",
            "dataPrimeiraAutorizacao", builder,
            "dt_paut",
            ui_filters[cat],
            state_keys[cat],
        )

    cat = "🌍 Demographics & Network"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_advanced_text_search(
            "Search: Patient Name",
            "usuarioSUS_nomeCompleto", builder,
            "txt_pac_nome",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = DuckDBCriteriaTranslator.translate(builder.build())
        st.markdown("---")

        curr_where = render_include_exclude(use_case, 
            "Municipality of Residence",
            "usuarioSUS_municipioResidencia_nome", builder,
            curr_where,
            "mun",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Neighborhood",
            "usuarioSUS_bairro", builder,
            curr_where,
            "bai",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        # Logradouro with conditional injecting numbering inside Deep Search
        curr_where = render_advanced_text_search(
            "Street",
            "usuarioSUS_logradouro", builder,
            "txt_logr",
            ui_filters[cat],
            state_keys[cat],
        )
        if st.session_state.get("txt_logr_toggle", False):
            st.markdown(
                "<div class='sre-filter-group'>",
                unsafe_allow_html=True,
            )
            state_keys[cat].extend(["num_min", "num_max"])
            # SRE FIX: Initializes state before widget to avoid value mismatch
            if "num_min" not in st.session_state:
                st.session_state["num_min"] = 0
            if "num_max" not in st.session_state:
                st.session_state["num_max"] = 99999
            col_nmin, col_nmax = st.columns(2)
            v_nmin = col_nmin.number_input(
                "Min No.",
                min_value=0,
                max_value=99999,
                step=10,
                key="num_min",
                label_visibility="collapsed",
            )
            v_nmax = col_nmax.number_input(
                "Max No.",
                min_value=0,
                max_value=99999,
                step=100,
                key="num_max",
                label_visibility="collapsed",
            )
            if v_nmin > 0 or v_nmax < 99999:
                ui_filters[cat].append(
                    {
                        "text": f"Street Number: {v_nmin} to {v_nmax}",
                        "keys": ["num_min", "num_max"],
                    }
                )
                builder.add_clausula_legado(
                    f'TRY_CAST("usuarioSUS_numero" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}'
                )
            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()  # --- Visual Separator for Personal Identification ---

        curr_where = DuckDBCriteriaTranslator.translate(builder.build())
        curr_where = render_include_exclude(use_case, 
            "Sex",
            "usuarioSUS_sexo", builder,
            curr_where,
            "sex",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        # Component that injects entidade_idade_idadeInteiro (with Dual Slider)
        curr_where = render_age_slider(use_case, 
            "Age Group (Age)", builder, "f_idade", ui_filters[cat], state_keys[cat]
        )

        curr_where = render_include_exclude(use_case, 
            "Race/Color",
            "usuarioSUS_racaCor", builder,
            curr_where,
            "cor",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Nationality",
            "usuarioSUS_nacionalidade", builder,
            curr_where,
            "nac",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

    cat = "⚠️ Triage & Risk Classification"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude(use_case, 
            "Complexity",
            "entidade_complexidade", builder,
            curr_where,
            "cpx",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Risk Color (Current)",
            "entidade_classificacaoRisco_cor", builder,
            curr_where,
            "r_cor",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Regulator Color",
            "corRegulador", builder,
            curr_where,
            "c_reg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_boolean_radio(
            "Reclassified by Requester",
            "entidade_classificacaoRisco_reclassificadaSolicitante", builder,
            "r_recl",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        curr_where = render_dual_slider(use_case, 
            "Gravity Points",
            "entidade_classificacaoRisco_pontosGravidade", builder,
            "pt_grav",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Time Points",
            "entidade_classificacaoRisco_pontosTempo", builder,
            "pt_tmp",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Total Points",
            "entidade_classificacaoRisco_totalPontos", builder,
            "pt_tot",
            ui_filters[cat],
            state_keys[cat],
        )

    cat = "🎯 Outcomes, Bottlenecks & SLA"
    with st.sidebar.expander(cat, expanded=False):
        # 1. Outcome Type — includes "IN PROGRESS" for cases without outcome yet
        curr_where = render_outcome_type_filter(
            use_case,
            "Outcome Type",
            "SLA_Tipo_Desfecho",
            builder,
            curr_where,
            "sla_tipo",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        curr_where = render_include_exclude(use_case, 
            "Provisional Status",
            "statusProvisorio", builder,
            curr_where,
            "st_prov",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        # Pending Reason — extracts 4 fields from JSON via DuckDB json_extract_string
        # WHY: get_dynamic_options("{expr}") wraps the argument with double quotes,
        # making the SQL expression invalid. The query is made directly in the use_case.
        curr_where = render_pending_reasons_filter(
            use_case,
            "📦 Pending Reason",
            builder,
            curr_where,
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Cancellation Reason",
            "motivoCancelamento", builder,
            curr_where,
            "mot_canc",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(use_case, 
            "Closure Reason",
            "motivoEncerramento", builder,
            curr_where,
            "mot_enc",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        # 2. Textos de Justificativa (Deep Search)  (Keep comments largely in English if desired, but focus on the UI strings)
        curr_where = render_advanced_text_search(
            "Return Justification",
            "justificativaRetorno", builder,
            "txt_retorno",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        # 3. Marcos de Sucesso (Booleans)
        curr_where = render_boolean_radio(
            "1. Was Authorized?",
            "SLA_Marco_Autorizada", builder,
            "m_aut",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "2. Was Scheduled?",
            "SLA_Marco_Agendada", builder,
            "m_agd",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "3. Was Accomplished?",
            "SLA_Marco_Realizada", builder,
            "m_rea",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Queue Finished? (Timer Stopped)",
            "SLA_Desfecho_Atingido", builder,
            "m_fim",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        # 4. Sliders de SLA (Métricas calculadas em dias e interações)
        curr_where = render_dual_slider(use_case, 
            "Total Lead Time (Days)",
            "SLA_Lead_Time_Total_Dias", builder,
            "sla_tot",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Time with Regulator (Days)",
            "SLA_Tempo_Regulador_Dias", builder,
            "sla_reg",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Time with Requester (Days)",
            "SLA_Tempo_Solicitante_Dias", builder,
            "sla_sol",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Interaction Volume (Ping-Pong)",
            "SLA_Interacoes_Regulacao", builder,
            "sla_int",
            ui_filters[cat],
            state_keys[cat],
        )

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
