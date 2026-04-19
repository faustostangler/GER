import os
import streamlit as st
from domain.specifications import FiltroAvancadoSpec, FiltroAvancadoSpecBuilder
from infrastructure.repositories.criteria_translator import DuckDBCriteriaTranslator
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
from infrastructure.config import settings
from infrastructure.telemetry.sentry import init_sentry


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
@st.cache_resource
def get_use_case():
    """WHY: Builds ClinicaPolicy from AppSettings here (presentation/infra),
    injecting it into AnalyticsUseCase via DI. The Use Case and Domain remain
    agnostic of settings — only the composition layer (this function) knows both.
    Ref: ADR-005 — Business Policy Extraction.
    """
    from infrastructure.repositories.duckdb_repository import (
        DuckDBAnalyticsRepository,
    )
    from application.use_cases.analytics_use_case import AnalyticsUseCase
    from domain.policies import ClinicaPolicy

    try:
        from infrastructure.telemetry.metrics import init_prometheus

        init_prometheus(port=8001)
    except Exception:
        # WHY: Streamlit reruns this block on every interaction. The Prometheus
        # HTTP server raises OSError if the port is already bound (second run).
        # Metrics themselves are guarded by _get_or_create in metrics.py.
        pass

    # WHY: Wire-up of the business policy from configurable settings.
    # Domain has safe defaults; .env only overrides if necessary.
    policy = ClinicaPolicy(
        idade_min=settings.AGE_MIN,
        idade_max=settings.AGE_MAX,
        sla_dias_vencimento=settings.SLA_DIAS_VENCIMENTO,
        mes_comercial_dias=settings.MES_COMERCIAL_DIAS,
        data_sla_threshold_horas=settings.DATA_SLA_THRESHOLD,
        cores_urgencia=settings.CORES_URGENCIA,
    )

    try:
        repo = DuckDBAnalyticsRepository(settings.OUTPUT_FILE)
    except ValueError as e:
        st.error(f"🔌 **Circuit Breaker Triggered:** {e}")
        st.stop()

    return AnalyticsUseCase(repo, policy=policy)


def get_dynamic_options(column: str, current_where: str, current_user) -> list:
    return get_use_case().get_dynamic_options(column, current_where, current_user)


@st.cache_data(ttl=3600)
def get_global_bounds(column: str, is_date=False):
    return get_use_case().get_global_bounds(column, is_date)


# --- 3. STATE MANAGEMENT ---
def clear_filter_state(keys_to_clear: list):
    """
    Clears the filter state in Streamlit's session_state.
    WHY: Deep Search text_inputs use two pairs of keys:
      - `{key}_or_val` / `{key}_and_val` / `{key}_not_val` → backing store (logical value)
      - `{key}_or`     / `{key}_and`     / `{key}_not`     → Streamlit widget key

    Both must be cleared simultaneously so the sidebar reflects the clearing.
    If only _val is cleared, the Streamlit widget keeps the old text on the next render.
    If only the widget key is deleted, the next render creates a new empty widget
    but _val is still filled, causing phantom re-filtering.
    """
    for key in keys_to_clear:
        if key in st.session_state:
            if key.endswith("_in") or key.endswith("_ex"):
                st.session_state[key] = []
            elif key.endswith("_val"):
                # SRE FIX: Clears both backing store AND corresponding widget key (without _val suffix)
                st.session_state[key] = ""
                widget_key = key[:-4]  # Remove "_val" → get widget key
                if widget_key in st.session_state:
                    st.session_state[widget_key] = ""
            elif key.endswith("_toggle"):
                st.session_state[key] = False
            elif key.endswith("_or") or key.endswith("_and") or key.endswith("_not"):
                # Widget key direct from text_input — clears visible text in sidebar
                st.session_state[key] = ""
                # Also clears the corresponding backing store _val (mirror)
                val_key = f"{key}_val"
                if val_key in st.session_state:
                    st.session_state[val_key] = ""
            elif key == "num_min":
                st.session_state[key] = 0
            elif key == "num_max":
                st.session_state[key] = 99999
            elif key == "oj_radio":
                st.session_state[key] = "Both"
            else:
                try:
                    del st.session_state[key]
                except Exception:
                    pass


# --- 4. UI COMPONENTS (DOMAIN FILTERS & TRACKING) ---
def render_kpi(container, label_with_icon, value, help_text="", alert=False):
    alert_class = "alert" if alert else ""
    help_clean = str(help_text).replace('"', "&quot;")

    # Extração de ícone caso exista (ex: "⏱️ Fila" -> ("⏱️", "Fila"))
    import re

    icon_match = re.match(r"^([^\w\s]+)\s*(.*)$", label_with_icon)
    if icon_match:
        icon, label = icon_match.groups()
        icon_html = f'<div class="kpi-icon">{icon}</div>'
    else:
        icon_html = ""
        label = label_with_icon

    html = f"""
    <div class="kpi-card {alert_class}" title="{help_clean}">
        <div class="kpi-card-header">
            {icon_html}
            <div class="kpi-label">{label}</div>
        </div>
        <div class="kpi-value-container">
            <div class="kpi-value">{value}</div>
        </div>
    </div>
    """
    container.markdown(html, unsafe_allow_html=True)


def render_include_exclude(
    label: str,
    column: str,
    builder: FiltroAvancadoSpecBuilder,
    current_where: str,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    current_user,
    default_in: list = None,
):
    cat_keys.extend([f"{key}_in", f"{key}_ex"])
    options = get_dynamic_options(column, current_where, current_user)
    if not options:
        return current_where

    # SRE FIX: Default state injection only on cold boot
    if f"{key}_in" not in st.session_state:
        if default_in:
            st.session_state[f"{key}_in"] = [v for v in default_in if v in options]
        else:
            st.session_state[f"{key}_in"] = []

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )
    incl = st.multiselect(
        "✅ Include",
        options,
        key=f"{key}_in",
        label_visibility="collapsed",
        placeholder="✅ Include...",
    )
    excl = st.multiselect(
        "❌ Exclude",
        options,
        key=f"{key}_ex",
        label_visibility="collapsed",
        placeholder="❌ Exclude...",
    )

    def sanitize(v):
        return str(v).replace("'", "''")

    if incl:
        # STATE ARCHITECTURE: Now we store the Visual Text and Associated Keys
        ui_tracker.append(
            {
                "text": f"✅ {label}: {', '.join([str(v) for v in incl])}",
                "keys": [f"{key}_in"],
            }
        )
        sanitized_incl = [str(v) for v in incl]
        builder.add_inclusao(column, sanitized_incl)

    if excl:
        # STATE ARCHITECTURE: Now we store the Visual Text and Associated Keys
        ui_tracker.append(
            {
                "text": f"❌ {label}: {', '.join([str(v) for v in excl])}",
                "keys": [f"{key}_ex"],
            }
        )
        sanitized_excl = [str(v) for v in excl]
        builder.add_exclusao(column, sanitized_excl)

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_boolean_radio(
    label: str, column: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """SRE Component for boolean fields (True/False/Null)"""
    cat_keys.append(f"{key}_radio")

    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Both"

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )
    val = st.radio(
        label,
        ["Both", "Yes", "No"],
        horizontal=True,
        key=f"{key}_radio",
        label_visibility="collapsed",
    )

    if val == "Yes":
        ui_tracker.append({"text": f"{label}: Yes", "keys": [f"{key}_radio"]})
        builder.add_booleano(column, True)
    elif val == "No":
        ui_tracker.append({"text": f"{label}: No", "keys": [f"{key}_radio"]})
        # Safe handling for False or Nulls
        builder.add_booleano_nullable(column, False)

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_presence_radio(
    label: str, column: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """SRE Component for text/ID fields where value presence validates true flag."""
    cat_keys.append(f"{key}_radio")

    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Both"

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )
    val = st.radio(
        label,
        ["Both", "Yes", "No"],
        horizontal=True,
        key=f"{key}_radio",
        label_visibility="collapsed",
    )

    if val == "Yes":
        ui_tracker.append({"text": f"{label}: Yes", "keys": [f"{key}_radio"]})
        builder.add_presenca(column, True)
    elif val == "No":
        ui_tracker.append({"text": f"{label}: No", "keys": [f"{key}_radio"]})
        builder.add_presenca(column, False)

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_dual_slider(
    label: str, column: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """SRE UX FIX: Bidirectional slider synchronized with numeric inputs for surgical precision."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    vmin, vmax = get_global_bounds(column, is_date=False)

    # SRE FIX: Using pd.notna() to protect against missing values (<NA>) from the database
    if pd.notna(vmin) and pd.notna(vmax) and vmin != vmax:
        vmin_val, vmax_val = int(vmin), int(vmax)

        # Initializes state with database limits if it doesn't exist
        if f"{key}_min" not in st.session_state:
            st.session_state[f"{key}_min"] = vmin_val
        if f"{key}_max" not in st.session_state:
            st.session_state[f"{key}_max"] = vmax_val
        if f"{key}_sld" not in st.session_state:
            st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)

        st.write(
            f"<span class='sre-label'>{label}</span>",
            unsafe_allow_html=True,
        )

        # State Synchronization Callbacks (Avoids infinite loops)
        def sync_slider():
            st.session_state[f"{key}_min"] = st.session_state[f"{key}_sld"][0]
            st.session_state[f"{key}_max"] = st.session_state[f"{key}_sld"][1]

        def sync_num():
            # Protection against inverted values (min > max)
            safe_min = min(
                st.session_state[f"{key}_min"], st.session_state[f"{key}_max"]
            )
            safe_max = max(
                st.session_state[f"{key}_min"], st.session_state[f"{key}_max"]
            )
            st.session_state[f"{key}_sld"] = (safe_min, safe_max)

        c1, c2 = st.columns(2)
        c1.number_input(
            "Minimum",
            min_value=vmin_val,
            max_value=vmax_val,
            key=f"{key}_min",
            on_change=sync_num,
            label_visibility="collapsed",
        )
        c2.number_input(
            "Maximum",
            min_value=vmin_val,
            max_value=vmax_val,
            key=f"{key}_max",
            on_change=sync_num,
            label_visibility="collapsed",
        )

        val = st.slider(
            label,
            vmin_val,
            vmax_val,
            key=f"{key}_sld",
            on_change=sync_slider,
            label_visibility="collapsed",
        )

        if val[0] > vmin_val or val[1] < vmax_val:
            ui_tracker.append(
                {
                    "text": f"{label}: {val[0]} a {val[1]}",
                    "keys": [f"{key}_sld", f"{key}_min", f"{key}_max"],
                }
            )
            builder.add_limite_numerico(column, val[0], val[1])

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_age_slider(
    label: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """Domain Component for Age: Converts visible Age Range to DATEDIFF in OLAP SQL."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    # WHY: Reads age range from ClinicaPolicy via use case — no longer from raw settings.
    # Ref: ADR-005.
    _policy = get_use_case()._policy
    vmin_val, vmax_val = _policy.idade_min, _policy.idade_max

    if f"{key}_min" not in st.session_state:
        st.session_state[f"{key}_min"] = vmin_val
    if f"{key}_max" not in st.session_state:
        st.session_state[f"{key}_max"] = vmax_val
    if f"{key}_sld" not in st.session_state:
        st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )

    def sync_slider_age():
        st.session_state[f"{key}_min"] = st.session_state[f"{key}_sld"][0]
        st.session_state[f"{key}_max"] = st.session_state[f"{key}_sld"][1]

    def sync_num_age():
        safe_min = min(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
        safe_max = max(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
        st.session_state[f"{key}_sld"] = (safe_min, safe_max)

    c1, c2 = st.columns(2)
    c1.number_input(
        "Min Age",
        min_value=vmin_val,
        max_value=vmax_val,
        key=f"{key}_min",
        on_change=sync_num_age,
        label_visibility="collapsed",
    )
    c2.number_input(
        "Max Age",
        min_value=vmin_val,
        max_value=vmax_val,
        key=f"{key}_max",
        on_change=sync_num_age,
        label_visibility="collapsed",
    )

    val = st.slider(
        label,
        vmin_val,
        vmax_val,
        key=f"{key}_sld",
        on_change=sync_slider_age,
        label_visibility="collapsed",
    )

    if val[0] > vmin_val or val[1] < vmax_val:
        ui_tracker.append(
            {
                "text": f"{label}: {val[0]} to {val[1]} years",
                "keys": [f"{key}_sld", f"{key}_min", f"{key}_max"],
            }
        )
        # SRE FIX: Uses precalculated column entidade_idade_idadeInteiro from Parquet
        builder.add_limite_numerico("entidade_idade_idadeInteiro", val[0], val[1])

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_smart_date_range(
    label: str,
    column: str,
    builder: FiltroAvancadoSpecBuilder,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    default_to_30_days: bool = False,
):
    """SRE UX FIX: Exclusively uses the native Streamlit selector, which already brings Range and Presets built-in."""
    cat_keys.append(key)

    # Initialize dynamic state (Cold Start Optimization vs Cross-Sectional UX)
    if key not in st.session_state:
        if default_to_30_days:
            hoje = date.today()
            st.session_state[key] = (hoje - timedelta(days=30), hoje)
        else:
            st.session_state[key] = ()

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )

    # Renders the input directly in the sidebar. No popovers, no extra buttons.
    val = st.date_input(label, key=key, label_visibility="collapsed")

    # OLAP Constructor
    if isinstance(val, tuple) and len(val) == 2:
        ui_tracker.append(
            {
                "text": f"{label}: {val[0].strftime('%Y-%m-%d')} to {val[1].strftime('%Y-%m-%d')}",
                "keys": [key],
            }
        )
        builder.add_limite_data(column, val[0].strftime('%Y-%m-%d'), val[1].strftime('%Y-%m-%d'))

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_advanced_text_search(
    label: str,
    column: str,
    builder: FiltroAvancadoSpecBuilder,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    aggregate_by: str = None,
    default_toggle: bool = False,
):
    """
    Renders a Toggle with Boolean logic, Accent tolerance, and Wildcard (*) support.
    If aggregate_by is passed, uses 'bool_or' (Single-pass OLAP).
    Added 'default_toggle' to allow Deep Search already open (Ex: Evolutions).
    """
    cat_keys.extend(
        [f"{key}_toggle", f"{key}_and_val", f"{key}_or_val", f"{key}_not_val"]
    )

    if f"{key}_toggle" not in st.session_state:
        st.session_state[f"{key}_toggle"] = default_toggle

    for suffix in ["and", "or", "not"]:
        if f"{key}_{suffix}_val" not in st.session_state:
            st.session_state[f"{key}_{suffix}_val"] = ""

    icon = "🧠" if aggregate_by else "🔎"
    is_active = st.toggle(f"{icon} Deep Search: {label}", key=f"{key}_toggle")

    if is_active:
        col_indent, col_content = st.columns([0.05, 0.95])

        with col_content:
            if aggregate_by:
                st.markdown(
                    "<div class='aggregate-search-bar'>Global Search: Looks into <b>all clinical history</b>.</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div class='deep-search-bar'>Search within Event.</div>",
                    unsafe_allow_html=True,
                )

            st.caption(
                r"Separate by comma ( , ). Use **\*** as wildcard (ex: *cardio\**). Accents are ignored."
            )

            or_terms = st.text_input(
                "✅ Contains ANY (OR)",
                value=st.session_state[f"{key}_or_val"],
                key=f"{key}_or",
            )
            and_terms = st.text_input(
                "⚠️ Contains ALL (AND)",
                value=st.session_state[f"{key}_and_val"],
                key=f"{key}_and",
            )
            not_terms = st.text_input(
                "❌ DOES NOT contain (NOT)",
                value=st.session_state[f"{key}_not_val"],
                key=f"{key}_not",
            )

            # SRE FIX: Only syncs _val ← widget if the backing store hasn't been cleared
            # by clear_filter_state. This prevents phantom refilling after clearing.
            if st.session_state.get(f"{key}_or_val", "") or or_terms:
                st.session_state[f"{key}_or_val"] = or_terms
            if st.session_state.get(f"{key}_and_val", "") or and_terms:
                st.session_state[f"{key}_and_val"] = and_terms
            if st.session_state.get(f"{key}_not_val", "") or not_terms:
                st.session_state[f"{key}_not_val"] = not_terms

            # --- LEXICAL PARSER EXTRACTED TO ADAPTER ---
            from presentation.adapters.parsers import parse_term

            if and_terms or or_terms or not_terms:
                _or = [w for w in or_terms.split(",") if w.strip()] if or_terms else []
                _and = [w for w in and_terms.split(",") if w.strip()] if and_terms else []
                _not = [w for w in not_terms.split(",") if w.strip()] if not_terms else []

                if _or:
                    ui_tracker.append({"text": f"✅ {label}: {or_terms}", "keys": [f"{key}_or_val", f"{key}_or", f"{key}_toggle"]})
                if _and:
                    ui_tracker.append({"text": f"⚠️ AND {label}: {and_terms}", "keys": [f"{key}_and_val", f"{key}_and", f"{key}_toggle"]})
                if _not:
                    ui_tracker.append({"text": f"❌ {label}: {not_terms}", "keys": [f"{key}_not_val", f"{key}_not", f"{key}_toggle"]})

                builder.add_busca_avancada(
                    column=column,
                    or_terms=_or,
                    and_terms=_and,
                    not_terms=_not,
                    aggregate_by=aggregate_by if aggregate_by else None
                )

    return DuckDBCriteriaTranslator.translate(builder.build())


# --- 4.5 BFF: IDENTITY AWARE PROXY (IAP) & BFF MOCK ---
# WHY: All IAM logic lives in the specialised adapter (streamlit_auth.py).
# The user identity widget rendering is the middleware's responsibility.
# This file (presentation entry point) must only compose — not decide.
# Ref: ADR-006 — IAM Adapter Isolation (Phase 3 / SRP extraction).
from presentation.adapters.streamlit_auth import require_authentication  # noqa: E402
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

    # 2. Identity Gatekeeper (IAM Adapter)
    # WHY: Unique Facade — all session logic, expiration and identity resolution
    # lives in presentation.adapters.streamlit_auth.require_authentication().
    # If unauthenticated or expired, it calls st.stop() internally.
    user = require_authentication()

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
    render_user_widget(user)

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

    # ==========================================
    # SRE FIX: NOMENCLATURE DICTIONARY (UBIQUITOUS LANGUAGE)
    # ==========================================
    MAPA_NOMENCLATURAS = {
        "entidade_especialidade_especialidadeMae_descricao": "Parent Specialty",
        "entidade_especialidade_descricao": "Fine Specialty",
        "entidade_especialidade_cbo_descricao": "CBO Specialty",
        "entidade_cidPrincipal_codigo": "Main ICD (Code)",
        "entidade_cidPrincipal_descricao": "Main ICD (Description)",
        "origem_lista": "Source (List)",
        "situacao": "Current Situation",
        "entidade_especialidade_tipoRegulacao": "Regulation Type",
        "entidade_especialidade_ativa": "Active Specialty",
        "entidade_especialidade_teleconsulta": "Accepts Teleconsultation",
        "entidade_centralRegulacao_nome": "Regulation Center",
        "entidade_unidadeOperador_centralRegulacao_nome": "Operating Unit Reg. Center",
        "liminarOrdemJudicial": "Injunction / Court Order",
        "entidade_unidadeOperador_nome": "Operating Unit",
        "entidade_unidadeOperador_razaoSocial": "Operating Unit (Corporate Name)",
        "entidade_unidadeOperador_tipoUnidade_descricao": "Operating Unit Type",
        "medicoSolicitante": "Requesting Doctor",
        "operador_nome": "Operator",
        "usuarioSolicitante_nome": "Requesting User",
        "evolucoes_json": "Information Source",
        "historico_evolucoes_completo": "Information Type",
        "entidade_complexidade": "Complexity",
        "entidade_classificacaoRisco_cor": "Risk Classification Color",
        "corRegulador": "Regulator Color",
        "usuarioSUS_municipioResidencia_nome": "Municipality of Residence",
        "usuarioSUS_bairro": "Neighborhood",
        "usuarioSUS_sexo": "Sex",
        "usuarioSUS_racaCor": "Race/Color",
        "usuarioSUS_nacionalidade": "Nationality",
    }

    # ==========================================
    # SRE FIX: MASTER COLOR DICTIONARY (GLOBAL)
    # ==========================================
    MAPA_CORES_RISCO = {
        "VERMELHO": "#ef4444",
        "LARANJA": "#f97316",
        "AMARELO": "#eab308",
        "VERDE": "#22c55e",
        "AZUL": "#3b82f6",
        "BRANCO": "#e5e7eb",
        "Não Informado": "#9ca3af",
    }

    builder = FiltroAvancadoSpecBuilder()
    curr_where = "1=1"

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
        curr_where = render_include_exclude(
            "Parent Specialty",
            "entidade_especialidade_especialidadeMae_descricao", builder,
            curr_where,
            "espm",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Fine Specialty",
            "entidade_especialidade_descricao", builder,
            curr_where,
            "espf",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "CBO Specialty",
            "entidade_especialidade_cbo_descricao", builder,
            curr_where,
            "esp_cbo",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Auxiliary Description",
            "entidade_especialidade_descricaoAuxiliar", builder,
            curr_where,
            "esp_aux",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(
            "Requesting Doctor",
            "medicoSolicitante", builder,
            curr_where,
            "med_sol",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Operating Unit",
            "entidade_unidadeOperador_nome", builder,
            curr_where,
            "usol",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(
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

        curr_where = render_include_exclude(
            "Source (List)",
            "origem_lista", builder,
            curr_where,
            "lst",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
            default_in=["Fila de Espera"],
        )
        curr_where = render_include_exclude(
            "Current Situation",
            "situacao", builder,
            curr_where,
            "sit",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Regulation Type",
            "entidade_especialidade_tipoRegulacao", builder,
            curr_where,
            "treg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
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
        curr_where = render_include_exclude(
            "Operator",
            "operador_nome", builder,
            curr_where,
            "op_nome",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Requesting User",
            "usuarioSolicitante_nome", builder,
            curr_where,
            "usu_sol_nome",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_include_exclude(
            "Regulation Center",
            "entidade_centralRegulacao_nome", builder,
            curr_where,
            "cent_reg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Operating Unit Reg. Center",
            "entidade_unidadeOperador_centralRegulacao_nome", builder,
            curr_where,
            "uni_op_cent",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
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

        curr_where = render_include_exclude(
            "Municipality of Residence",
            "usuarioSUS_municipioResidencia_nome", builder,
            curr_where,
            "mun",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
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
        curr_where = render_include_exclude(
            "Sex",
            "usuarioSUS_sexo", builder,
            curr_where,
            "sex",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        # Component that injects entidade_idade_idadeInteiro (with Dual Slider)
        curr_where = render_age_slider(
            "Age Group (Age)", builder, "f_idade", ui_filters[cat], state_keys[cat]
        )

        curr_where = render_include_exclude(
            "Race/Color",
            "usuarioSUS_racaCor", builder,
            curr_where,
            "cor",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
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
        curr_where = render_include_exclude(
            "Complexity",
            "entidade_complexidade", builder,
            curr_where,
            "cpx",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Risk Color (Current)",
            "entidade_classificacaoRisco_cor", builder,
            curr_where,
            "r_cor",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
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
        curr_where = render_dual_slider(
            "Gravity Points",
            "entidade_classificacaoRisco_pontosGravidade", builder,
            "pt_grav",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Time Points",
            "entidade_classificacaoRisco_pontosTempo", builder,
            "pt_tmp",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Total Points",
            "entidade_classificacaoRisco_totalPontos", builder,
            "pt_tot",
            ui_filters[cat],
            state_keys[cat],
        )

    cat = "🎯 Outcomes, Bottlenecks & SLA"
    with st.sidebar.expander(cat, expanded=False):
        # 1. Outcome Type — includes "IN PROGRESS" for cases without outcome yet
        _desfecho_options_raw = get_dynamic_options(
            "SLA_Tipo_Desfecho", curr_where, st.session_state.user
        )
        _desfecho_options = sorted(set([o for o in _desfecho_options_raw if o])) + [
            "IN PROGRESS"
        ]
        cat_keys_desfecho = ["sla_tipo_in", "sla_tipo_ex"]
        state_keys[cat].extend(cat_keys_desfecho)
        _sla_incl = st.multiselect(
            "Outcome Type ✅",
            _desfecho_options,
            key="sla_tipo_in",
            label_visibility="collapsed",
            placeholder="✅ Include Outcome...",
        )
        _sla_excl = st.multiselect(
            "Outcome Type ❌",
            _desfecho_options,
            key="sla_tipo_ex",
            label_visibility="collapsed",
            placeholder="❌ Exclude Outcome...",
        )
        st.write(
            "<span style='font-size:0.9em;font-weight:600;color:#4B5563;'>Outcome Type</span>",
            unsafe_allow_html=True,
        )
        if _sla_incl:
            ui_filters[cat].append(
                {
                    "text": f"✅ Outcome Type: {', '.join(_sla_incl)}",
                    "keys": ["sla_tipo_in"],
                }
            )
            _parts = []
            if "IN PROGRESS" in _sla_incl:
                _rest = [v for v in _sla_incl if v != "IN PROGRESS"]
                _parts.append(
                    '("SLA_Tipo_Desfecho" IS NULL OR "SLA_Tipo_Desfecho" = \'\')'
                )
                if _rest:
                    _safe = "', '".join(v.replace("'", "''") for v in _rest)
                    _parts.append(f"\"SLA_Tipo_Desfecho\" IN ('{_safe}')")
            else:
                _safe = "', '".join(v.replace("'", "''") for v in _sla_incl)
                _parts.append(f"\"SLA_Tipo_Desfecho\" IN ('{_safe}')")
            builder.add_clausula_legado(f"({' OR '.join(_parts)})")
            curr_where = DuckDBCriteriaTranslator.translate(builder.build())
        if _sla_excl:
            ui_filters[cat].append(
                {
                    "text": f"❌ Outcome Type: {', '.join(_sla_excl)}",
                    "keys": ["sla_tipo_ex"],
                }
            )
            _parts_ex = []
            if "IN PROGRESS" in _sla_excl:
                _rest_ex = [v for v in _sla_excl if v != "IN PROGRESS"]
                _parts_ex.append(
                    '("SLA_Tipo_Desfecho" IS NOT NULL AND "SLA_Tipo_Desfecho" != \'\')'
                )
                if _rest_ex:
                    _safe_ex = "', '".join(v.replace("'", "''") for v in _rest_ex)
                    _parts_ex.append(f"\"SLA_Tipo_Desfecho\" NOT IN ('{_safe_ex}')")
            else:
                _safe_ex = "', '".join(v.replace("'", "''") for v in _sla_excl)
                _parts_ex.append(f"\"SLA_Tipo_Desfecho\" NOT IN ('{_safe_ex}')")
            builder.add_clausula_legado(f"({' AND '.join(_parts_ex)})")
            curr_where = DuckDBCriteriaTranslator.translate(builder.build())

        curr_where = render_include_exclude(
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
        st.write(
            "<span style='font-size:0.9em;font-weight:600;color:#4B5563;'>📦 Pending Reason</span>",
            unsafe_allow_html=True,
        )
        _pend_fields = [
            (
                "Type",
                "json_extract_string(\"motivoPendencia\", '$.tipo')",
                "mot_pend_tipo",
            ),
            (
                "Reason",
                "json_extract_string(\"motivoPendencia\", '$.motivo')",
                "mot_pend_mot",
            ),
            (
                "Description",
                "json_extract_string(\"motivoPendencia\", '$.descricao')",
                "mot_pend_desc",
            ),
            (
                "Status",
                "json_extract_string(\"motivoPendencia\", '$.status')",
                "mot_pend_sta",
            ),
        ]
        _where_for_pend = curr_where if curr_where.strip() else "1=1"
        _uc = get_use_case()
        for _pf_label, _pf_expr, _pf_key in _pend_fields:
            try:
                _pf_sql = (
                    f"SELECT DISTINCT {_pf_expr} AS val "
                    f"FROM gercon "
                    f"WHERE {_where_for_pend} "
                    f"AND {_pf_expr} IS NOT NULL "
                    f"AND {_pf_expr} != '' "
                    f"ORDER BY 1"
                )
                _pf_raw = _uc.execute_custom_query(_pf_sql, None, st.session_state.user)
                _pf_opts = _pf_raw["val"].dropna().tolist() if not _pf_raw.empty else []
            except Exception:
                _pf_opts = []

            if not _pf_opts:
                continue

            state_keys[cat].extend([f"{_pf_key}_in", f"{_pf_key}_ex"])
            st.caption(_pf_label)
            _pf_incl = st.multiselect(
                f"{_pf_label} ✅",
                sorted(set(str(o) for o in _pf_opts)),
                key=f"{_pf_key}_in",
                label_visibility="collapsed",
                placeholder=f"✅ Incluir {_pf_label}...",
            )
            _pf_excl = st.multiselect(
                f"{_pf_label} ❌",
                sorted(set(str(o) for o in _pf_opts)),
                key=f"{_pf_key}_ex",
                label_visibility="collapsed",
                placeholder=f"❌ Excluir {_pf_label}...",
            )
            if _pf_incl:
                _pf_safe = "', '".join(v.replace("'", "''") for v in _pf_incl)
                builder.add_clausula_legado(f"{_pf_expr} IN ('{_pf_safe}')")
                ui_filters[cat].append(
                    {
                        "text": f"✅ Pendência {_pf_label}: {', '.join(_pf_incl)}",
                        "keys": [f"{_pf_key}_in"],
                    }
                )
                curr_where = DuckDBCriteriaTranslator.translate(builder.build())
            if _pf_excl:
                _pf_safe_ex = "', '".join(v.replace("'", "''") for v in _pf_excl)
                builder.add_clausula_legado(f"{_pf_expr} NOT IN ('{_pf_safe_ex}')")
                ui_filters[cat].append(
                    {
                        "text": f"❌ Pendência {_pf_label}: {', '.join(_pf_excl)}",
                        "keys": [f"{_pf_key}_ex"],
                    }
                )
                curr_where = DuckDBCriteriaTranslator.translate(builder.build())

        st.markdown("---")
        curr_where = render_include_exclude(
            "Cancellation Reason",
            "motivoCancelamento", builder,
            curr_where,
            "mot_canc",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
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
        curr_where = render_dual_slider(
            "Total Lead Time (Days)",
            "SLA_Lead_Time_Total_Dias", builder,
            "sla_tot",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Time with Regulator (Days)",
            "SLA_Tempo_Regulador_Dias", builder,
            "sla_reg",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Time with Requester (Days)",
            "SLA_Tempo_Solicitante_Dias", builder,
            "sla_sol",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
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
    use_case = get_use_case()

    with st.spinner(
        "Processing Read Model (OLAP) and Tail Latency (P90)..."
    ):
        kpi_data = use_case.get_executive_summary(filters, st.session_state.user)

    # --- SRE FIX: DATA FRESHNESS SLA MONITOR ---
    if kpi_data.last_sync_at > 0:
        import time

        age_hours = (time.time() - kpi_data.last_sync_at) / 3600
        # WHY: Freshness threshold is domain invariant (ClinicaPolicy),
        # not infra config. Ref: ADR-005.
        _policy = get_use_case()._policy
        if age_hours > _policy.data_sla_threshold_horas:
            # SOTA Alert: Digital Surgeon Aesthetic
            alert_html = f"""
            <div class="amber-alert-container">
                <div class="amber-alert-icon">⚠️</div>
                <div class="amber-alert-content">
                    <div class="amber-alert-title">Amber Alert: Data Freshness SLA Violation</div>
                    <div class="amber-alert-text">
                        The displayed data has a delay of <b>{age_hours:.1f} hours</b>. 
                        The Worker Scraper might be inactive or failed in the last cycle.
                    </div>
                </div>
            </div>
            """
            st.markdown(alert_html, unsafe_allow_html=True)

    # --- Extração Segura das Variáveis Absolutas ---
    pacientes = kpi_data.pacientes
    eventos = kpi_data.eventos
    esp_mae = kpi_data.esp_mae
    sub_esp = kpi_data.sub_esp
    medicos = kpi_data.medicos
    origens = kpi_data.origens
    lead_time = kpi_data.lead_time
    max_lead_time = kpi_data.max_lead_time

    # Extração das Métricas P90 (Tolerância a falhas caso não haja dados)
    p90_lead_time = int(kpi_data.p90_lead_time)
    p90_esquecido = int(kpi_data.p90_esquecido)

    # --- Cálculos Derivados SOTA (Prevenção contra divisão por zero) ---
    evo_por_paciente = kpi_data.evo_por_paciente
    sub_por_esp = kpi_data.sub_por_esp
    cid_por_medico = kpi_data.cid_por_medico
    evo_por_medico = kpi_data.evo_por_medico

    # SRE FIX: Motor de Taxa de Ingestão (Cadastros por Mês)
    cad_por_mes = kpi_data.cad_por_mes
    taxa_urgencia = kpi_data.taxa_urgencia
    taxa_vencidos = kpi_data.taxa_vencidos

    # ==========================================
    # ABA 1: VISÃO GERAL (EXECUTIVE SUMMARY)
    # ==========================================
    with t_kpi:
        st.markdown(
            "<div class='sre-section-title'>Performance Dashboard (SLA and Load)</div>",
            unsafe_allow_html=True,
        )

        # --- LINHA 1: Volume, Carga e Esforço ---
        r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
        render_kpi(
            r1_c1,
            "🏢 Gercon Sources",
            f"{origens:,}".replace(",", "."),
            help_text="Number of distinct entry points/source systems.",
        )
        render_kpi(
            r1_c2,
            "👥 Patients",
            f"{pacientes:,}".replace(",", "."),
            help_text="Total number of unique patients selected.",
        )
        render_kpi(
            r1_c3,
            "📋 Evolutions",
            f"{eventos:,}".replace(",", "."),
            help_text="Total number of events in the clinical history.",
        )
        render_kpi(
            r1_c4,
            "📈 Evolutions/Patient",
            f"{evo_por_paciente}".replace(".", ","),
            help_text="Average number of times the patient was moved or evaluated.",
        )

        st.write(" ")

        # --- LINHA 2: Complexidade Clínica e SLA ---
        r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
        render_kpi(
            r2_c1,
            "🏛️ Specialties (Parent)",
            f"{esp_mae:,}".replace(",", "."),
            help_text="Broad clinical areas covered (E.g.: SURGERY).",
        )
        render_kpi(
            r2_c2,
            "🎯 Subspecialties",
            f"{sub_esp:,}".replace(",", "."),
            help_text="Fine specialties covered (E.g.: HAND SURGERY).",
        )
        render_kpi(
            r2_c3,
            "🔀 Subs/Specialty",
            f"{sub_por_esp}".replace(".", ","),
            help_text="Average branches per broad clinical area.",
        )

        lead_str = (
            f"{lead_time} dias | {max_lead_time} dias"
            if pd.notna(lead_time)
            else "0 dias"
        )
        render_kpi(
            r2_c4,
            "⏱️ Queue: Average | Worst",
            lead_str,
            help_text="Average Time vs Time of the oldest patient.",
        )

        st.write(" ")

        # --- LINHA 3: Governança e Comportamento Médico ---
        r3_c1, r3_c2, r3_c3, r3_c4 = st.columns(4)
        render_kpi(
            r3_c1,
            "👨⚕️ Requesting Doctors",
            f"{medicos:,}".replace(",", "."),
            help_text="Total distinct doctors who inserted patients in this queue.",
        )
        render_kpi(
            r3_c2,
            "📅 Registrations/Month",
            f"{cad_por_mes}".replace(".", ","),
            help_text="Historical monthly average of new patients added to the queue (based on the filtered window).",
        )
        render_kpi(
            r3_c3,
            "🧠 Diagnostic Dispersion",
            f"{cid_por_medico}".replace(".", ","),
            help_text="Average distinct ICDs used per doctor.",
        )
        render_kpi(
            r3_c4,
            "⚙️ Load/Doctor",
            f"{evo_por_medico}".replace(".", ","),
            help_text="Average volume of administrative evolutions generated per doctor.",
        )

        st.divider()

        # --- BLOCO 2 CONSOLIDADO: ANATOMIA COMPARATIVA E RISCO ---
        st.markdown(
            "<div class='sre-section-title'>Comparative Anatomy: Dispersion and Waiting Scale</div>",
            unsafe_allow_html=True,
        )

        st.markdown(
            """
            <div style="font-size: 0.85rem; color: #6b7280; margin-bottom: 1.5rem; line-height: 1.4;">
                <b>How to read the charts:</b> The <b>center line</b> in the box is the median (typical patient). 
                The <b>box</b> groups 50% of the queue. The <b>dashed lines</b> show the SRE boundaries: 
                <b>P10</b> (the 10% fastest/efficiency) and <b>P90</b> (the 90% network limit/guarantee). 
                The <b>individual points</b> on the right are outliers (non-standard critical cases).
            </div>
        """,
            unsafe_allow_html=True,
        )

        df_dist = use_case.get_distribution_analysis(filters, st.session_state.user)

        if not df_dist.empty:
            # Função para limpar extremos e calcular estatísticas SRE (Decis P10/P90)
            def get_sre_stats(df, col):
                q1 = df[col].quantile(0.25)
                q3 = df[col].quantile(0.75)
                iqr = q3 - q1
                # Limpeza para escala (3.0x IQR)
                df_clean = df[df[col] <= (q3 + 3.0 * iqr)]
                # Cálculos de Percentis (Decile Border)
                p10 = df[col].quantile(0.10)
                p90 = df[col].quantile(0.90)
                return df_clean, p10, p90

            # --- SRE UX FIX: Função para Anotação Integrada no Design do BoxPlot ---
            def annotate_boxplot(fig, df_clean, col, p10, p90, line_color):
                # Calculamos os quartis exatamente como o Plotly faz internamente
                q1 = df_clean[col].quantile(0.25)
                med = df_clean[col].median()
                q3 = df_clean[col].quantile(0.75)
                iqr = q3 - q1

                # Encontra os valores de Fences (Min/Max excluindo outliers de 1.5x IQR)
                min_fence = df_clean[df_clean[col] >= q1 - 1.5 * iqr][col].min()
                max_fence = df_clean[df_clean[col] <= q3 + 1.5 * iqr][col].max()

                if pd.isna(min_fence):
                    min_fence = df_clean[col].min()
                if pd.isna(max_fence):
                    max_fence = df_clean[col].max()

                # Separação Top/Bottom em zigue-zague para os textos NUNCA colidirem na UI
                stats_top = {"Min": min_fence, "Q1": q1, "Q3": q3, "Max": max_fence}
                stats_bot = {"P10": p10, "Med": med, "P90": p90}

                # Aplica cor BRANCA aos valores
                for label, val in stats_top.items():
                    if pd.notna(val):
                        fig.add_annotation(
                            x=val,
                            y=0.58,
                            yref="paper",
                            text=f"{label}<br><b>{int(val)}</b>",
                            showarrow=False,
                            font=dict(size=11, color="white"),
                            yanchor="bottom",
                            align="center",
                        )

                for label, val in stats_bot.items():
                    if pd.notna(val):
                        fig.add_annotation(
                            x=val,
                            y=0.42,
                            yref="paper",
                            text=f"<b>{int(val)}</b><br>{label}",
                            showarrow=False,
                            font=dict(size=11, color="white"),
                            yanchor="top",
                            align="center",
                        )

                # Desenha os fences pontilhados discretos para P10 e P90 usando a cor original da linha do plot
                for val in [p10, p90]:
                    if pd.notna(val):
                        fig.add_shape(
                            type="line",
                            x0=val,
                            x1=val,
                            y0=0.35,
                            y1=0.65,
                            yref="paper",
                            line=dict(color=line_color, width=2, dash="dot"),
                        )

            df_plot_fila, p10_fila, p90_fila = get_sre_stats(df_dist, "dias_fila")
            df_plot_esq, p10_esq, p90_esq = get_sre_stats(df_dist, "dias_esquecido")

            # Escala Unificada para comparação direta (Adicionamos margem negativa para os textos não cortarem)
            max_val = (
                max(
                    df_plot_fila["dias_fila"].max(), df_plot_esq["dias_esquecido"].max()
                )
                if not df_plot_fila.empty and not df_plot_esq.empty
                else 100
            )
            limite_x = [-max_val * 0.08, max_val * 1.08]

            # --- RENDERIZAÇÃO: BOXPLOT ABANDONO (VERMELHO) ---
            # SRE Performance Fix: Amostragem para evitar MessageSizeError (OOM do FrontEnd via Websocket de >200MB)
            df_render_esq = (
                df_plot_esq.sample(n=min(10000, len(df_plot_esq)), random_state=42)
                if not df_plot_esq.empty
                else df_plot_esq
            )
            fig_esq = px.box(
                df_render_esq,
                x="dias_esquecido",
                title="Abandonment: Days without Evolution",
                points="outliers",
                color_discrete_sequence=["#ef4444"],
                range_x=limite_x,
            )

            # Aplica a anotação SOTA
            annotate_boxplot(
                fig_esq, df_plot_esq, "dias_esquecido", p10_esq, p90_esq, "#ef4444"
            )

            # Remove Hover (SRE UX: Zero Distraction)
            fig_esq.update_traces(hoverinfo="skip", hovertemplate=None)
            fig_esq.update_layout(
                hovermode=False,
                height=200,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=0, t=40, b=40),
            )
            fig_esq.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#f1f5f9")
            st.plotly_chart(fig_esq, width="stretch", config={"displayModeBar": False})

            # --- RENDERIZAÇÃO: BOXPLOT CADASTRO (AZUL) ---
            df_render_fila = (
                df_plot_fila.sample(n=min(10000, len(df_plot_fila)), random_state=42)
                if not df_plot_fila.empty
                else df_plot_fila
            )
            fig_fila = px.box(
                df_render_fila,
                x="dias_fila",
                title="Registration: Days Waiting",
                points="outliers",
                color_discrete_sequence=["#3b82f6"],
                range_x=limite_x,
            )

            # Aplica a anotação SOTA
            annotate_boxplot(
                fig_fila, df_plot_fila, "dias_fila", p10_fila, p90_fila, "#3b82f6"
            )

            # Remove Hover (SRE UX: Zero Distraction)
            fig_fila.update_traces(hoverinfo="skip", hovertemplate=None)
            fig_fila.update_layout(
                hovermode=False,
                height=200,
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=0, r=0, t=40, b=40),
            )
            fig_fila.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#f1f5f9")
            st.plotly_chart(fig_fila, width="stretch", config={"displayModeBar": False})

            if len(df_dist) > len(df_plot_fila) or len(df_dist) > len(df_plot_esq):
                st.caption(
                    "ℹ️ Optimized scale (extreme outliers hidden from viewport for easier visualization). Statistics preserved."
                )

            # --- 2. INDICADORES P90 (PADRÃO ST.METRIC PARA CONSISTÊNCIA VISUAL) ---
            st.write(" ")
            g_p90_1, g_p90_2 = st.columns(2)

            with g_p90_1:
                render_kpi(
                    g_p90_1,
                    label_with_icon="⏳ P90 Forgotten Time",
                    value=f"{p90_esquecido} dias",
                    help_text="90% of the network has not received clinical updates up to this day limit.",
                )

            with g_p90_2:
                render_kpi(
                    g_p90_2,
                    label_with_icon="⏱️ P90 Queue Time",
                    value=f"{p90_lead_time} dias",
                    help_text="90% of the network waits up to this day limit from registration to appointment.",
                )

            # 3. GAUGES (FINAL DA SEÇÃO)
            st.write(" ")
            g1, g2 = st.columns(2)
            with g1:
                fig_gauge1 = go.Figure(
                    go.Indicator(
                        mode="gauge+number+delta",
                        value=taxa_urgencia,
                        number={"suffix": "%", "font": {"color": "#4B5563"}},
                        title={"text": "Severity Index", "font": {"size": 14}},
                        gauge={
                            "axis": {"range": [0, 100]},
                            "bar": {
                                "color": "#ef4444" if taxa_urgencia > 30 else "#f97316"
                            },
                            "bgcolor": "rgba(0,0,0,0)",
                            "steps": [
                                {"range": [0, 15], "color": "#dcfce7"},
                                {"range": [15, 30], "color": "#fef08a"},
                                {"range": [30, 100], "color": "#fee2e2"},
                            ],
                        },
                    )
                )
                fig_gauge1.update_layout(
                    height=220,
                    margin=dict(l=20, r=20, t=40, b=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(
                    fig_gauge1,
                    width="stretch",
                    config={"displayModeBar": False},
                )
            with g2:
                fig_gauge2 = go.Figure(
                    go.Indicator(
                        mode="gauge+number",
                        value=taxa_vencidos,
                        number={"suffix": "%", "font": {"color": "#4B5563"}},
                        title={"text": "SLA Breach (>180d)", "font": {"size": 14}},
                        gauge={
                            "axis": {"range": [0, 100]},
                            "bar": {"color": "#1e293b"},
                            "bgcolor": "rgba(0,0,0,0)",
                            "steps": [
                                {"range": [0, 10], "color": "#dcfce7"},
                                {"range": [10, 25], "color": "#fef08a"},
                                {"range": [25, 100], "color": "#fee2e2"},
                            ],
                        },
                    )
                )
                fig_gauge2.update_layout(
                    height=220,
                    margin=dict(l=20, r=20, t=40, b=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                )
                st.plotly_chart(
                    fig_gauge2,
                    width="stretch",
                    config={"displayModeBar": False},
                )

        st.divider()

    with t_macro:
        # --- BLOCO 1: EXPLORADOR DINÂMICO SOTA (EXPLOSÃO SOLAR BIVARIADA) ---
        st.subheader(
            "📊 Dynamic Queue Explorer: Bivariate (Load vs Latency/Risk)"
        )

        st.info(
            "💡 **How to read (SRE Bivariate Chart):** \n"
            "- **Slice Size:** Represents **Load (Volume)**. Wide slices indicate many patients waiting.\n"
            "- **Slice Color:** Represents the selected **Risk/Latency** metric. Warm tones (red) reveal bottlenecks, critical patients, or advanced age groups, while cool tones (blue) indicate fast flow or low risk."
        )

        # Dividimos a tela para os dois controles do usuário
        c_hier, c_metric = st.columns([0.7, 0.3])

        with c_hier:
            niveis_sunburst = st.multiselect(
                "Select Data Hierarchy (Max: 5 levels):",
                options=[
                    # --- Clínico & Regulação ---
                    "entidade_especialidade_especialidadeMae_descricao",
                    "entidade_especialidade_descricao",
                    "entidade_especialidade_cbo_descricao",
                    "entidade_cidPrincipal_codigo",
                    "entidade_cidPrincipal_descricao",
                    "origem_lista",
                    "situacao",
                    "entidade_especialidade_tipoRegulacao",
                    "entidade_especialidade_ativa",
                    "entidade_especialidade_teleconsulta",
                    "entidade_centralRegulacao_nome",
                    "entidade_unidadeOperador_centralRegulacao_nome",
                    # --- Governança & Atores ---
                    "liminarOrdemJudicial",
                    "entidade_unidadeOperador_nome",
                    "entidade_unidadeOperador_razaoSocial",
                    "entidade_unidadeOperador_tipoUnidade_descricao",
                    "medicoSolicitante",
                    "operador_nome",
                    "usuarioSolicitante_nome",
                    "evolucoes_json",
                    "historico_evolucoes_completo",
                    # --- Triagem & Classificação de Risco ---
                    "entidade_complexidade",
                    "entidade_classificacaoRisco_cor",
                    "corRegulador",
                    # --- Demografia & Rede ---
                    "usuarioSUS_municipioResidencia_nome",
                    "usuarioSUS_bairro",
                    "usuarioSUS_sexo",
                    "usuarioSUS_racaCor",
                    "usuarioSUS_nacionalidade",
                ],
                default=[
                    "entidade_especialidade_especialidadeMae_descricao",
                    "entidade_especialidade_descricao",
                    "entidade_cidPrincipal_descricao",
                ],
                max_selections=5,
                help="Drag and drop tags to reorder the funnel (path) of the chart.",
                format_func=lambda col: MAPA_NOMENCLATURAS.get(col, col),
            )

        with c_metric:
            st.write(" ")  # Alinhamento visual com o label do multiselect
            # Dicionário SRE: Mapeia a UX para a query OLAP
            METRICAS_COR = {
                "⏳ Wait Time (Queue)": {
                    "sql": "ROUND(AVG(SLA_Lead_Time_Total_Dias), 1)",
                    "unit": "days",
                },
                "⚠️ Forgotten Time (No Evolution)": {
                    "sql": "ROUND(AVG(SLA_Tempo_Regulador_Dias), 1)",
                    "unit": "days",
                },
                "🚨 Severity Points": {
                    "sql": "ROUND(AVG(entidade_classificacaoRisco_pontosGravidade), 1)",
                    "unit": "pts",
                },
                "⏱️ Time Points": {
                    "sql": "ROUND(AVG(entidade_classificacaoRisco_pontosTempo), 1)",
                    "unit": "pts",
                },
                "🔥 Total Score": {
                    "sql": "ROUND(AVG(entidade_classificacaoRisco_totalPontos), 1)",
                    "unit": "pts",
                },
                "🎂 Average Age (Demographics)": {
                    "sql": "ROUND(AVG(date_diff('year', TRY_CAST(usuarioSUS_dataNascimento AS DATE), CURRENT_DATE)), 1)",
                    "unit": "years",
                },
            }

            cor_selecionada = st.selectbox(
                "Color Metric (Temperature):",
                options=list(METRICAS_COR.keys()),
                index=0,
                help="Defines what the color of each slice represents. The size will always be the volume of patients.",
            )

        if niveis_sunburst:
            # Variáveis dinâmicas para a Query e para a UI
            levels_sql = ", ".join([f'"{n}"' for n in niveis_sunburst])
            sql_cor = METRICAS_COR[cor_selecionada]["sql"]
            unidade_cor = METRICAS_COR[cor_selecionada]["unit"]
            nome_metrica = cor_selecionada.split(" ", 1)[
                1
            ]  # Extrai apenas o texto sem o emoji para o gráfico

            # SQL OLAP Dinâmico: DuckDB calcula o cruzamento em tempo real
            df_plot_sun = use_case.execute_custom_query(
                f"""
                SELECT 
                    {levels_sql}, 
                    COUNT(DISTINCT numeroCMCE) as Vol,
                    {sql_cor} as Metrica_Cor
                FROM gercon
                WHERE {FINAL_WHERE}
                GROUP BY {levels_sql}
            """,
                filters,
                st.session_state.user,
            )

            if not df_plot_sun.empty:
                # SRE FIX: Prevenção contra Nós Folha Vazios no Plotly
                for col in niveis_sunburst:
                    df_plot_sun[col] = (
                        df_plot_sun[col]
                        .replace("", "Not Informed")
                        .fillna("Not Informed")
                    )

                # Paleta divergente universal (Azul = Baixo Risco/Rápido, Vermelho = Alto Risco/Atraso)
                paleta = "RdYlBu_r"

                fig_sun = px.sunburst(
                    df_plot_sun,
                    path=niveis_sunburst,
                    values="Vol",
                    color="Metrica_Cor",
                    color_continuous_scale=paleta,
                    title=f"Bivariate Analysis: Size (Load) vs Color ({nome_metrica})",
                    labels={"Vol": "Patients", "Metrica_Cor": nome_metrica},
                )

                # SRE UX: Injeta dinamicamente a unidade correta (dias, pts ou anos) e remove bordas
                fig_sun.update_traces(
                    hovertemplate=f"<b>%{{label}}</b><br>Patients (Load): %{{value}}<br>{nome_metrica}: %{{color}} {unidade_cor}<extra></extra>",
                    marker=dict(line=dict(width=0)),
                )

                fig_sun.update_layout(height=700, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(
                    fig_sun, width="stretch", config={"displayModeBar": False}
                )
            else:
                st.warning(
                    "⚠️ No data available for the Sunburst with the current filters."
                )
        else:
            st.warning("⚠️ Select at least 1 level to render the chart.")

        st.markdown("---")
        st.subheader("⏱️ Golden Signals: Governance and Flow Health")
        c1, c2 = st.columns([0.4, 0.6])

        with c1:
            # Matriz de Risco (Donut)
            df_risco = use_case.execute_custom_query(
                f"SELECT entidade_classificacaoRisco_cor, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND entidade_classificacaoRisco_cor != '' GROUP BY 1",
                spec=filters,
                current_user=st.session_state.user,
            )
            if not df_risco.empty:
                # SRE FIX: Usando a nova variável global MAPA_CORES_RISCO
                st.plotly_chart(
                    px.pie(
                        df_risco,
                        values="Vol",
                        names="entidade_classificacaoRisco_cor",
                        hole=0.5,
                        color="entidade_classificacaoRisco_cor",
                        color_discrete_map=MAPA_CORES_RISCO,
                        title="Risk Matrix (Priority)",
                    ),
                    width="stretch",
                    config={"displayModeBar": False},
                )

        with c2:
            # Funil de Jornada (Conversão)
            df_funil = use_case.execute_custom_query(
                f"""
                SELECT '1. Requested' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE}
                UNION ALL
                SELECT '2. Triage' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND entidade_classificacaoRisco_cor != ''
                UNION ALL
                SELECT '3. Scheduled' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND situacao ILIKE '%AGENDADA%'
                UNION ALL
                SELECT '4. Accomplished' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND (situacao ILIKE '%ATENDIDO%' OR situacao ILIKE '%REALIZADO%')
            """,
                filters,
                st.session_state.user,
            )
            st.plotly_chart(
                px.funnel(
                    df_funil,
                    x="Vol",
                    y="Etapa",
                    title="Journey Funnel: Bottlenecks and Abandonment",
                ),
                width="stretch",
                config={"displayModeBar": False},
            )

        df_sit = use_case.execute_custom_query(
            f"SELECT situacao, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1 ORDER BY 2 DESC",
            spec=filters,
            current_user=st.session_state.user,
        )
        st.plotly_chart(
            px.bar(
                df_sit,
                x="situacao",
                y="Vol",
                title="Overall Network Situation",
                color="situacao",
                template="plotly_white",
            ),
            width="stretch",
            config={"displayModeBar": False},
        )

    with t_clin:
        st.subheader("Clinical Intelligence & Demographic Profile")

        import time

        try:
            from infrastructure.telemetry.metrics import RENDER_LATENCY, SILENT_ERRORS
        except ImportError:
            # Degradação graciosa para ambientes de teste sem telemetria total
            RENDER_LATENCY, SILENT_ERRORS = None, None

        c1, c2 = st.columns(2)
        with c1:
            try:
                start_treemap = time.time()
                # Geometria da Demanda (Treemap)
                df_mun = use_case.execute_custom_query(
                    f"SELECT usuarioSUS_municipioResidencia_nome, usuarioSUS_bairro, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND usuarioSUS_municipioResidencia_nome != '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30",
                    spec=filters,
                    current_user=st.session_state.user,
                )

                # --- SRE FIX: Prevenção contra Nós Folha Vazios no Plotly ---
                if not df_mun.empty:
                    df_mun["usuarioSUS_bairro"] = (
                        df_mun["usuarioSUS_bairro"]
                        .replace("", "Not Informed")
                        .fillna("Not Informed")
                    )
                    st.plotly_chart(
                        px.treemap(
                            df_mun,
                            path=[
                                "usuarioSUS_municipioResidencia_nome",
                                "usuarioSUS_bairro",
                            ],
                            values="Vol",
                            title="Geometry: Municipality ➔ usuarioSUS_bairro",
                            color="Vol",
                            color_continuous_scale="Viridis",
                        ),
                        width="stretch",
                        config={"displayModeBar": False},
                    )
                if RENDER_LATENCY:
                    RENDER_LATENCY.labels(component="t_clin_treemap").observe(
                        time.time() - start_treemap
                    )
            except Exception:
                if SILENT_ERRORS:
                    SILENT_ERRORS.labels(component="t_clin_treemap").inc()
                st.warning("⚠️ Insufficient or malformed data for the Treemap.")

        with c2:
            try:
                start_hist = time.time()
                # SRE FIX: Cálculo de Idade blindado (TRY_CAST para evitar Conversion Error)
                df_demo = use_case.execute_custom_query(
                    f"""
                    SELECT Idade_Int, usuarioSUS_sexo, COUNT(DISTINCT numeroCMCE) as Vol
                    FROM (
                        SELECT 
                            date_diff('year', TRY_CAST(usuarioSUS_dataNascimento AS DATE), CURRENT_DATE) as Idade_Int, 
                            usuarioSUS_sexo, 
                            numeroCMCE
                        FROM gercon 
                        WHERE {FINAL_WHERE}
                    ) 
                    WHERE Idade_Int IS NOT NULL AND Idade_Int >= 0
                    GROUP BY 1, 2
                """,
                    filters,
                    st.session_state.user,
                )

                if not df_demo.empty:
                    fig_demo = px.histogram(
                        df_demo,
                        x="Idade_Int",
                        y="Vol",
                        color="usuarioSUS_sexo",
                        barmode="group",
                        color_discrete_map={
                            "Feminino": "#ec4899",
                            "Masculino": "#3b82f6",
                        },
                        title="Demographic Profile (Age vs usuarioSUS_sexo)",
                        labels={
                            "Idade_Int": "Approximate Age",
                            "Vol": "Patient Volume",
                        },
                    )
                    st.plotly_chart(
                        fig_demo, width="stretch", config={"displayModeBar": False}
                    )
                if RENDER_LATENCY:
                    RENDER_LATENCY.labels(component="t_clin_demographics").observe(
                        time.time() - start_hist
                    )
            except Exception:
                if SILENT_ERRORS:
                    SILENT_ERRORS.labels(component="t_clin_demographics").inc()
                st.warning("⚠️ Silent error caught in demographic rendering.")

        # Throughput vs Capacidade (Temporal)
        df_fluxo = use_case.execute_custom_query(
            f"SELECT CAST(dataSolicitacao AS DATE) as Dia, origem_lista, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND dataSolicitacao IS NOT NULL GROUP BY 1, 2 ORDER BY 1",
            spec=filters,
            current_user=st.session_state.user,
        )
        st.plotly_chart(
            px.area(
                df_fluxo,
                x="Dia",
                y="Vol",
                color="origem_lista",
                title="Temporal Throughput: Patient Volume by Source",
            ),
            width="stretch",
            config={"displayModeBar": False},
        )

        st.markdown("---")
        st.subheader("🕵️ Clinical Pattern Audit (Location / Actor × Diagnosis)")

        # --- SELETORES DE DIMENSÃO ---
        # WHY: Hardcodar medicoSolicitante × CID limita a análise.
        # Com seletores livres o gestor pode parear qualquer eixo:
        # "UBS × Especialidade Mãe" revela gargalos regionais;
        # "Médico × CID" mantém a auditoria individual original.
        _OPCOES_DIAGNOSTICO = {
            "ICD — Description": "entidade_cidPrincipal_descricao",
            "ICD — Code": "entidade_cidPrincipal_codigo",
            "Parent Specialty": "entidade_especialidade_especialidadeMae_descricao",
            "Fine Specialty": "entidade_especialidade_descricao",
            "CBO (Specialty)": "entidade_especialidade_cbo_descricao",
        }
        _OPCOES_ATOR = {
            "Requesting Doctor": "medicoSolicitante",
            "Operating Unit (UBS)": "entidade_unidadeOperador_nome",
            "Operating Unit (Corporate Name)": "entidade_unidadeOperador_razaoSocial",
            "Unit Type": "entidade_unidadeOperador_tipoUnidade_descricao",
            "Regulation Center": "entidade_centralRegulacao_nome",
            "Operator (Regulator)": "operador_nome",
            "Requesting User": "usuarioSolicitante_nome",
        }

        c_dim1, c_dim2 = st.columns(2)
        with c_dim1:
            _label_ator = st.selectbox(
                "📍 X-Axis — Location / Actor:",
                options=list(_OPCOES_ATOR.keys()),
                index=0,
                help="Defines who or what location appears on the heatmap's X-axis.",
            )
        with c_dim2:
            _label_diag = st.selectbox(
                "🔬 Y-Axis — Diagnosis / Clinical Dimension:",
                options=list(_OPCOES_DIAGNOSTICO.keys()),
                index=0,
                help="Defines which clinical dimension appears on the heatmap's Y-axis.",
            )

        _col_ator = _OPCOES_ATOR[_label_ator]
        _col_diag = _OPCOES_DIAGNOSTICO[_label_diag]

        # --- 1. DEFINIÇÃO ESTRITA DE VARIÁVEIS DE ESTADO ---
        OPT_CID = "Horizontal Analysis (Peer Comparison)"
        OPT_MED = "Vertical Analysis (Individual Profile)"

        # --- 2. UI UX FIX: Controles Analíticos (Sliders Independentes) ---
        c_top1, c_top2, c_metric = st.columns([0.15, 0.15, 0.7])
        with c_top1:
            top_x_med = st.slider(
                f"Top {_label_ator.split('(')[0].strip()}:",
                min_value=5,
                max_value=100,
                value=15,
                step=1,
                help=f"Define the amount of '{_label_ator}' items on the X-axis.",
            )
        with c_top2:
            top_x_cid = st.slider(
                f"Top {_label_diag.split('(')[0].strip()}:",
                min_value=5,
                max_value=100,
                value=15,
                step=1,
                help=f"Define the amount of '{_label_diag}' items on the Y-axis.",
            )
        with c_metric:
            st.write(" ")
            modo_heatmap = st.radio(
                "Analytical Visualization Metric (Standard Deviation):",
                options=[OPT_CID, OPT_MED],
                horizontal=True,
            )

        # --- CAIXA DE EXPLICAÇÃO DINÂMICA DE LEITURA (UX) ---
        if modo_heatmap == OPT_CID:
            st.info(
                f"💡 **Hint: Horizontal Analysis (Peer Comparison):** Evaluates the same **{_label_diag} (row)** across all '{_label_ator}'. "
                f"Warm tones (red) indicate that the actor in question has a frequency **statistically much higher than their peers' average**."
            )
        else:
            st.info(
                f"💡 **Hint: Vertical Analysis (Individual Profile):** Evaluates the routine of a single **{_label_ator} (column)** by comparing all the clinical dimensions they present. "
                f"Warm tones (red) reveal which '{_label_diag}' are anomalies that deviate from that specific actor's normal pattern."
            )

        # --- 3. EXTRACÇÃO OLAP (DuckDB com Limites Independentes) ---
        df_heatmap = use_case.execute_custom_query(
            f"""
            WITH TopAtores AS (
                SELECT "{_col_ator}" FROM gercon
                WHERE {FINAL_WHERE} AND "{_col_ator}" != '' AND "{_col_ator}" IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT numeroCMCE) DESC LIMIT {top_x_med}
            ),
            TopDiags AS (
                SELECT "{_col_diag}" FROM gercon
                WHERE {FINAL_WHERE} AND "{_col_diag}" != '' AND "{_col_diag}" IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT numeroCMCE) DESC LIMIT {top_x_cid}
            )
            SELECT
                "{_col_ator}"  AS _ator,
                "{_col_diag}"  AS _diag,
                COUNT(DISTINCT numeroCMCE) as Vol
            FROM gercon
            WHERE {FINAL_WHERE}
              AND "{_col_ator}" IN (SELECT "{_col_ator}" FROM TopAtores)
              AND "{_col_diag}" IN (SELECT "{_col_diag}" FROM TopDiags)
            GROUP BY 1, 2
            """,
            filters,
            st.session_state.user,
        )

        # WHY: O heatmap usa aliases internos (_ator, _diag) e pivot_table — operações
        # que podem falhar se o mock/query retornar DataFrame sem essas colunas ou com
        # dados insuficientes para o cálculo de desvio padrão. Degradação graciosa.
        try:
            if (
                not df_heatmap.empty
                and "_diag" in df_heatmap.columns
                and "_ator" in df_heatmap.columns
            ):
                df_heatmap["_diag_curto"] = df_heatmap["_diag"].apply(
                    lambda x: x[:45] + "..." if len(str(x)) > 45 else x
                )

                # Cria a Matriz Base (Volumes Absolutos para hover)
                df_pivot_vol = df_heatmap.pivot_table(
                    index="_diag_curto",
                    columns="_ator",
                    values="Vol",
                    fill_value=0,
                )
                df_math = df_pivot_vol.copy().astype(float)

                # --- 4. MOTOR ESTATÍSTICO (Vetorização Pandas) ---
                paleta_heatmap = "RdBu_r"

                if modo_heatmap == OPT_CID:
                    medias_linhas = df_math.mean(axis=1)
                    desvios_linhas = df_math.std(axis=1).replace(0, 1)
                    df_math = df_math.sub(medias_linhas, axis=0).div(
                        desvios_linhas, axis=0
                    )
                elif modo_heatmap == OPT_MED:
                    medias_colunas = df_math.mean(axis=0)
                    desvios_colunas = df_math.std(axis=0).replace(0, 1)
                    df_math = df_math.sub(medias_colunas, axis=1).div(
                        desvios_colunas, axis=1
                    )

                # --- 5. FORMATADOR DE TEXTO VISUAL (Apenas Z-Score) ---
                df_text = df_math.apply(lambda col: col.map(lambda x: f"{x:+.1f}"))

                # --- 6. RENDERIZAÇÃO MATRICIAL SOTA (px.imshow) ---
                fig_heat = px.imshow(
                    df_math,
                    aspect="auto",
                    color_continuous_scale=paleta_heatmap,
                    color_continuous_midpoint=0,
                    title=f"Deviation Matrix (Z-Score): Top {top_x_cid} {_label_diag} × Top {top_x_med} {_label_ator}",
                    labels=dict(x=_label_ator, y=_label_diag, color="Z-Score"),
                )

                fig_heat.update_traces(
                    text=df_text.values,
                    texttemplate="%{text}",
                    customdata=df_pivot_vol.values,
                    hovertemplate=f"<b>{_label_ator}:</b> %{{x}}<br><b>{_label_diag}:</b> %{{y}}<br><b>Real Volume:</b> %{{customdata}} patients<br><b>Z-Score:</b> %{{text}} deviations<extra></extra>",
                )

                altura_dinamica = max(500, top_x_cid * 35)
                fig_heat.update_layout(
                    xaxis_tickangle=-45,
                    height=altura_dinamica,
                    margin=dict(l=250, b=120),
                )
                st.plotly_chart(
                    fig_heat, width="stretch", config={"displayModeBar": False}
                )
        except Exception:
            st.warning(
                "⚠️ Insufficient data to generate the clinical audit heatmap."
            )

        # --- GRÁFICO 2: TREEMAP HIERÁRQUICO DE PERFIL (Ator ➔ Diagnóstico) ---
        df_perfil_med = use_case.execute_custom_query(
            f"""
            SELECT "{_col_ator}" AS _ator, "{_col_diag}" AS _diag, COUNT(DISTINCT numeroCMCE) as Vol
            FROM gercon
            WHERE {FINAL_WHERE}
              AND "{_col_ator}" != '' AND "{_col_ator}" IS NOT NULL
              AND "{_col_diag}" != '' AND "{_col_diag}" IS NOT NULL
            GROUP BY 1, 2 HAVING COUNT(DISTINCT numeroCMCE) >= 3 ORDER BY 3 DESC LIMIT 150
            """,
            filters,
            st.session_state.user,
        )

        try:
            if (
                not df_perfil_med.empty
                and "_ator" in df_perfil_med.columns
                and "_diag" in df_perfil_med.columns
            ):
                df_perfil_med["_ator"] = df_perfil_med["_ator"].replace(
                    "", f"{_label_ator} Not Informed"
                )
                df_perfil_med["_diag"] = df_perfil_med["_diag"].replace(
                    "", f"{_label_diag} Not Informed"
                )
                fig_tree_med = px.treemap(
                    df_perfil_med,
                    path=["_ator", "_diag"],
                    values="Vol",
                    color="Vol",
                    color_continuous_scale="Teal",
                    title=f"Profile: {_label_ator} ➔ {_label_diag} (Click to expand)",
                )
                fig_tree_med.update_layout(
                    height=500, margin=dict(t=40, l=10, r=10, b=10)
                )
                st.plotly_chart(
                    fig_tree_med, width="stretch", config={"displayModeBar": False}
                )
        except Exception:
            st.warning("⚠️ Insufficient data to generate the clinical profile treemap.")

    with t_micro:
        st.subheader("Audit of Outliers & Top Offenders (SRE)")

        c1, c2 = st.columns([0.7, 0.3])
        with c1:
            # Matriz de Outliers (Scatter Plot)
            st.markdown("### 🔍 SLA Outlier Detection")
            df_outliers = use_case.execute_custom_query(
                f"""
                SELECT numeroCMCE, entidade_classificacaoRisco_cor, TRY_CAST(entidade_classificacaoRisco_totalPontos AS INTEGER) as Pontos, 
                    DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) as DiasFila,
                    situacao, entidade_especialidade_descricao
                FROM gercon 
                WHERE {FINAL_WHERE} AND dataSolicitacao IS NOT NULL AND situacao NOT ILIKE '%ENCERRADA%'
                ORDER BY DiasFila DESC, Pontos DESC
                LIMIT 3000
            """,
                filters,
                st.session_state.user,
            )
            try:
                if (
                    not df_outliers.empty
                    and "DiasFila" in df_outliers.columns
                    and "Pontos" in df_outliers.columns
                ):
                    # 2. Prevenção de Nós Vazios
                    df_outliers["entidade_classificacaoRisco_cor"] = (
                        df_outliers["entidade_classificacaoRisco_cor"]
                        .replace("", "Not Informed")
                        .fillna("Not Informed")
                    )

                    # 3. Plotagem do Scatter com os parâmetros matematicamente corretos usando a global
                    fig_out = px.scatter(
                        df_outliers,
                        x="DiasFila",
                        y="Pontos",
                        color="entidade_classificacaoRisco_cor",
                        color_discrete_map=MAPA_CORES_RISCO,
                        opacity=0.7,
                        size="Pontos",
                        hover_data=["numeroCMCE"],
                        title="Outlier Detection: Queue Time vs Severity",
                        labels={
                            "DiasFila": "Wait Time (Days)",
                            "Pontos": "Severity Points",
                        },
                        render_mode="svg",
                    )
                    fig_out.add_hline(
                        y=40, line_dash="dot", annotation_text="High Severity"
                    )
                    fig_out.add_vline(
                        x=180, line_dash="dot", annotation_text="180 d SLA"
                    )
                    st.plotly_chart(
                        fig_out, width="stretch", config={"displayModeBar": False}
                    )
            except Exception:
                st.warning("⚠️ Insufficient data for configuring outliers scatter.")

        with c2:
            # Top Ofensores (Barra Horizontal)
            st.markdown("### ⚖️ Top Offenders")
            df_medico = use_case.execute_custom_query(
                f"SELECT medicoSolicitante, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND medicoSolicitante != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
                spec=filters,
                current_user=st.session_state.user,
            )
            try:
                if not df_medico.empty and "medicoSolicitante" in df_medico.columns:
                    fig_ofensor = px.bar(
                        df_medico,
                        x="Vol",
                        y="medicoSolicitante",
                        orientation="h",
                        title="Top 10 Doctors (Volume)",
                    )
                    fig_ofensor.update_layout(
                        yaxis={"categoryorder": "total ascending"}, height=450
                    )
                    st.plotly_chart(
                        fig_ofensor, width="stretch", config={"displayModeBar": False}
                    )
            except Exception:
                st.warning("⚠️ Insufficient data for doctors ranking.")

        # Log Clinical Audit
        st.markdown("---")
        st.markdown("### 📝 Clinical Evolutions Log")

        c_slider, c_export = st.columns([0.8, 0.2])
        with c_slider:
            limit = st.slider("Clinical Audit Sample", 10, 1000, 100)

        df_audit = use_case.execute_custom_query(
            f"""
            SELECT numeroCMCE, CAST(dataSolicitacao AS DATE) as Solicitação, CAST(dataCadastro AS TIMESTAMP) as Data_Evolução, 
            situacao, entidade_classificacaoRisco_cor as "Risco Cor", historico_quadro_clinico 
            FROM gercon WHERE {FINAL_WHERE} ORDER BY dataSolicitacao DESC, dataCadastro DESC LIMIT {limit}
        """,
            filters,
            st.session_state.user,
        )

        with c_export:
            st.write(" ")  # Espaçamento vertical
            csv_data = df_audit.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name=f"auditoria_gercon_{date.today()}.csv",
                mime="text/csv",
                width="stretch",
            )

        st.dataframe(df_audit, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
