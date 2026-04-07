import os
import streamlit as st
from domain.models import FilterCriteria
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
    st.markdown(
        """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
        hr { margin-top: 1rem; margin-bottom: 1rem; }
        .deep-search-bar { border-left: 4px solid #3b82f6; padding-left: 0.75rem; margin-top: 0.5rem; margin-bottom: 0.5rem; color: #4B5563; font-size: 0.9rem;}
        .deep-search-bar { border-left: 4px solid #3b82f6; padding-left: 0.75rem; margin-top: 0.5rem; margin-bottom: 0.5rem; color: #4B5563; font-size: 0.9rem;}
        .aggregate-search-bar { border-left: 4px solid #8b5cf6; padding-left: 0.75rem; margin-top: 0.5rem; margin-bottom: 0.5rem; color: #4B5563; font-size: 0.9rem;}

        /* ========================================================
           SRE FIX: ISOLAMENTO TOTAL DO FLEXBOX (PREVINE BUBBLING)
           ======================================================== */
           
        /* 1. Título da Categoria: Comporta-se como um bloco normal (força nova linha) */
        .cat-title {
            font-weight: 700;
            font-size: 0.75rem;
            color: #94a3b8;
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-top: 1.2rem;
            margin-bottom: 0.5rem;
            display: block;
        }

        /* 2. Container dos filtros: Seleciona APENAS o bloco mais profundo (innermost) */
        div[data-testid="stVerticalBlock"]:has(.filter-row-marker):not(:has(div[data-testid="stVerticalBlock"] .filter-row-marker)) {
            display: flex !important;
            flex-direction: row !important;
            flex-wrap: wrap !important;
            gap: 1.5rem !important;     /* Distância elegante entre filtros */
            align-items: center !important;
            margin-bottom: 0.5rem !important;
        }

        div[data-testid="stVerticalBlock"]:has(.filter-row-marker):not(:has(div[data-testid="stVerticalBlock"] .filter-row-marker)) > div {
            width: fit-content !important;
            flex: 0 1 auto !important;
        }

        /* 3. Botões Extremamente Discretos e Inquebráveis */
        [data-testid="stExpanderDetails"] button {
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 0 !important;
            margin: 0 !important;
            color: #64748b !important;
            font-size: 0.85rem !important;
            font-weight: 400 !important;
            min-height: unset !important;
            height: auto !important;
            white-space: nowrap !important; /* Impede o X de cair para a linha de baixo */
            transition: color 0.2s ease;
        }

        /* Hover: Vermelho e Riscado */
        [data-testid="stExpanderDetails"] button:hover {
            color: #ef4444 !important;
            text-decoration: line-through !important;
            background: transparent !important;
        }
    </style>
    """,
        unsafe_allow_html=True,
    )


# --- 2. INFRASTRUCTURE: USE CASE & DI ---
@st.cache_resource
def get_use_case():
    from infrastructure.repositories.duckdb_repository import (
        DuckDBAnalyticsRepository,
    )
    from application.use_cases.analytics_use_case import AnalyticsUseCase
    try:
        from infrastructure.telemetry.metrics import init_telemetry
        init_telemetry(port=8001)
    except (ImportError, AttributeError):
        pass

    try:
        repo = DuckDBAnalyticsRepository(settings.OUTPUT_FILE)
    except ValueError as e:
        st.error(f"🔌 **Circuit Breaker Acionado:** {e}")
        st.stop()
        
    return AnalyticsUseCase(repo)


def get_dynamic_options(column: str, current_where: str, current_user) -> list:
    return get_use_case().get_dynamic_options(column, current_where, current_user)


@st.cache_data(ttl=3600)
def get_global_bounds(column: str, is_date=False):
    return get_use_case().get_global_bounds(column, is_date)


# --- 3. STATE MANAGEMENT ---
def clear_filter_state(keys_to_clear: list):
    """
    Limpa o estado dos filtros no session_state do Streamlit.
    WHY: Os text_inputs de Busca Profunda usam dois pares de chaves:
      - `{key}_or_val` / `{key}_and_val` / `{key}_not_val` → backing store (valor lógico)
      - `{key}_or`     / `{key}_and`     / `{key}_not`     → chave do widget Streamlit

    Ambos devem ser zerados simultaneamente para que o sidebar reflita a limpeza.
    Se apenas o _val for zerado, o widget Streamlit mantém o texto antigo no próximo render.
    Se apenas o widget key for deletado, a próxima renderização cria um novo widget vazio
    mas o _val ainda está preenchido, causando re-filtro fantasma.
    """
    for key in keys_to_clear:
        if key in st.session_state:
            if key.endswith("_in") or key.endswith("_ex"):
                st.session_state[key] = []
            elif key.endswith("_val"):
                # SRE FIX: Limpa o backing store E o widget key correspondente (sem sufixo _val)
                st.session_state[key] = ""
                widget_key = key[:-4]  # Remove "_val" → obtém a chave do widget
                if widget_key in st.session_state:
                    st.session_state[widget_key] = ""
            elif key.endswith("_toggle"):
                st.session_state[key] = False
            elif key.endswith("_or") or key.endswith("_and") or key.endswith("_not"):
                # Widget key direto do text_input — zera o texto visível no sidebar
                st.session_state[key] = ""
                # Zera também o backing store _val correspondente (mirror)
                val_key = f"{key}_val"
                if val_key in st.session_state:
                    st.session_state[val_key] = ""
            elif key == "num_min":
                st.session_state[key] = 0
            elif key == "num_max":
                st.session_state[key] = 99999
            elif key == "oj_radio":
                st.session_state[key] = "Ambos"
            else:
                try:
                    del st.session_state[key]
                except Exception:
                    pass


# --- 4. UI COMPONENTS (DOMAIN FILTERS & TRACKING) ---
def render_include_exclude(
    label: str,
    column: str,
    clauses: list,
    current_where: str,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    current_user,
):
    cat_keys.extend([f"{key}_in", f"{key}_ex"])
    options = get_dynamic_options(column, current_where, current_user)
    if not options:
        return current_where

    st.write(
        f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>",
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    incl = c1.multiselect(
        "✅ Incluir",
        options,
        key=f"{key}_in",
        label_visibility="collapsed",
        placeholder="✅ Incluir...",
    )
    excl = c2.multiselect(
        "❌ Excluir",
        options,
        key=f"{key}_ex",
        label_visibility="collapsed",
        placeholder="❌ Excluir...",
    )

    def sanitize(v):
        return str(v).replace("'", "''")

    if incl:
        # ARQUITETURA DE ESTADO: Agora guardamos o Texto Visual e as Chaves Associadas
        ui_tracker.append(
            {
                "text": f"✅ {label}: {', '.join([str(v) for v in incl])}",
                "keys": [f"{key}_in"],
            }
        )
        sanitized_incl = [f"'{sanitize(v)}'" for v in incl]
        clauses.append(f'"{column}" IN ({", ".join(sanitized_incl)})')

    if excl:
        # ARQUITETURA DE ESTADO: Agora guardamos o Texto Visual e as Chaves Associadas
        ui_tracker.append(
            {
                "text": f"❌ {label}: {', '.join([str(v) for v in excl])}",
                "keys": [f"{key}_ex"],
            }
        )
        sanitized_excl = [f"'{sanitize(v)}'" for v in excl]
        clauses.append(f'"{column}" NOT IN ({", ".join(sanitized_excl)})')

    return " AND ".join(clauses)


def render_boolean_radio(
    label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list
):
    """Componente SRE para campos booleanos (True/False/Null)"""
    cat_keys.append(f"{key}_radio")

    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Ambos"

    st.write(
        f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>",
        unsafe_allow_html=True,
    )
    val = st.radio(
        label,
        ["Ambos", "Sim", "Não"],
        horizontal=True,
        key=f"{key}_radio",
        label_visibility="collapsed",
    )

    if val == "Sim":
        ui_tracker.append({"text": f"{label}: Sim", "keys": [f"{key}_radio"]})
        clauses.append(f'"{column}" = true')
    elif val == "Não":
        ui_tracker.append({"text": f"{label}: Não", "keys": [f"{key}_radio"]})
        # Tratamento seguro para Falsos ou Nulos
        clauses.append(f'("{column}" = false OR "{column}" IS NULL)')

    return " AND ".join(clauses)


def render_presence_radio(
    label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list
):
    """Componente SRE para campos de texto/ID onde a presença de valor valida a flag verdadeira (Ex: Liminar)."""
    cat_keys.append(f"{key}_radio")

    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Ambos"

    st.write(
        f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>",
        unsafe_allow_html=True,
    )
    val = st.radio(
        label,
        ["Ambos", "Sim", "Não"],
        horizontal=True,
        key=f"{key}_radio",
        label_visibility="collapsed",
    )

    if val == "Sim":
        ui_tracker.append({"text": f"{label}: Sim", "keys": [f"{key}_radio"]})
        clauses.append(f'("{column}" IS NOT NULL AND "{column}" != \'\')')
    elif val == "Não":
        ui_tracker.append({"text": f"{label}: Não", "keys": [f"{key}_radio"]})
        clauses.append(f'("{column}" IS NULL OR "{column}" = \'\')')

    return " AND ".join(clauses)


def render_dual_slider(
    label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list
):
    """SRE UX FIX: Slider bidirecional sincronizado com inputs numéricos para precisão cirúrgica."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    vmin, vmax = get_global_bounds(column, is_date=False)

    # SRE FIX: Usando pd.notna() para proteger contra valores ausentes (<NA>) do banco
    if pd.notna(vmin) and pd.notna(vmax) and vmin != vmax:
        vmin_val, vmax_val = int(vmin), int(vmax)

        # Inicializa o estado com os limites do banco se não existir
        if f"{key}_min" not in st.session_state:
            st.session_state[f"{key}_min"] = vmin_val
        if f"{key}_max" not in st.session_state:
            st.session_state[f"{key}_max"] = vmax_val
        if f"{key}_sld" not in st.session_state:
            st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)

        st.write(
            f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>",
            unsafe_allow_html=True,
        )

        # Callbacks de Sincronização de Estado (Evita loops infinitos)
        def sync_slider():
            st.session_state[f"{key}_min"] = st.session_state[f"{key}_sld"][0]
            st.session_state[f"{key}_max"] = st.session_state[f"{key}_sld"][1]

        def sync_num():
            # Proteção contra inversão de valores (min > max)
            safe_min = min(
                st.session_state[f"{key}_min"], st.session_state[f"{key}_max"]
            )
            safe_max = max(
                st.session_state[f"{key}_min"], st.session_state[f"{key}_max"]
            )
            st.session_state[f"{key}_sld"] = (safe_min, safe_max)

        c1, c2 = st.columns(2)
        c1.number_input(
            "Mínimo",
            min_value=vmin_val,
            max_value=vmax_val,
            key=f"{key}_min",
            on_change=sync_num,
            label_visibility="collapsed",
        )
        c2.number_input(
            "Máximo",
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
            clauses.append(
                f'TRY_CAST("{column}" AS INTEGER) BETWEEN {val[0]} AND {val[1]}'
            )

    return " AND ".join(clauses)


def render_age_slider(
    label: str, clauses: list, key: str, ui_tracker: list, cat_keys: list
):
    """Componente de Domínio para Idade: Converte Faixa Etária visível para DATEDIFF no SQL OLAP."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    vmin_val, vmax_val = settings.AGE_MIN, settings.AGE_MAX

    if f"{key}_min" not in st.session_state:
        st.session_state[f"{key}_min"] = vmin_val
    if f"{key}_max" not in st.session_state:
        st.session_state[f"{key}_max"] = vmax_val
    if f"{key}_sld" not in st.session_state:
        st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)

    st.write(
        f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>",
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
        "Idade Min",
        min_value=vmin_val,
        max_value=vmax_val,
        key=f"{key}_min",
        on_change=sync_num_age,
        label_visibility="collapsed",
    )
    c2.number_input(
        "Idade Max",
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
                "text": f"{label}: {val[0]} a {val[1]} anos",
                "keys": [f"{key}_sld", f"{key}_min", f"{key}_max"],
            }
        )
        # SRE FIX: Usa a coluna pré-calculada entidade_idade_idadeInteiro do Parquet
        # WHY: DATEDIFF calculado em runtime é mais lento e não aproveita o valor já consolidado.
        clauses.append(
            f'TRY_CAST("entidade_idade_idadeInteiro" AS INTEGER) BETWEEN {val[0]} AND {val[1]}'
        )
    return " AND ".join(clauses)


def render_smart_date_range(
    label: str,
    column: str,
    clauses: list,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    default_to_30_days: bool = False,
):
    """SRE UX FIX: Usa exclusivamente o seletor nativo do Streamlit, que já traz Range e Presets embutidos."""
    cat_keys.append(key)

    # Inicializa estado dinâmico (Otimização Cold Start vs UX Cross-Sectional)
    if key not in st.session_state:
        if default_to_30_days:
            hoje = date.today()
            st.session_state[key] = (hoje - timedelta(days=30), hoje)
        else:
            st.session_state[key] = ()

    st.write(
        f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>",
        unsafe_allow_html=True,
    )

    # Renderiza o input diretamente na sidebar. Sem popovers, sem botões extras.
    val = st.date_input(label, key=key, label_visibility="collapsed")

    # Construtor do OLAP
    if isinstance(val, tuple) and len(val) == 2:
        ui_tracker.append(
            {
                "text": f"{label}: {val[0].strftime('%d/%m/%Y')} a {val[1].strftime('%d/%m/%Y')}",
                "keys": [key],
            }
        )
        clauses.append(f"CAST(\"{column}\" AS DATE) BETWEEN '{val[0]}' AND '{val[1]}'")

    return " AND ".join(clauses)


def render_advanced_text_search(
    label: str,
    column: str,
    clauses: list,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    aggregate_by: str = None,
    default_toggle: bool = False,
):
    """
    Renderiza um Toggle com lógica Booleana, tolerância a Acentos e suporte a Wildcards (*).
    Se aggregate_by for passado, utiliza 'bool_or' (Single-pass OLAP).
    Adicionado 'default_toggle' para permitir Busca Profunda já aberta (Ex: Evoluções).
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
    is_active = st.toggle(f"{icon} Busca Profunda: {label}", key=f"{key}_toggle")

    if is_active:
        col_indent, col_content = st.columns([0.05, 0.95])

        with col_content:
            if aggregate_by:
                st.markdown(
                    "<div class='aggregate-search-bar'>Busca Global: Procura em <b>todo o histórico clínico</b>.</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div class='deep-search-bar'>Busca no Evento.</div>",
                    unsafe_allow_html=True,
                )

            st.caption(
                r"Separe por vírgula ( , ). Use **\*** como curinga (ex: *cardio\**). Acentos são ignorados."
            )

            or_terms = st.text_input(
                "✅ Contém QUALQUER UMA (OR)",
                value=st.session_state[f"{key}_or_val"],
                key=f"{key}_or",
            )
            and_terms = st.text_input(
                "⚠️ Contém TODAS (AND)",
                value=st.session_state[f"{key}_and_val"],
                key=f"{key}_and",
            )
            not_terms = st.text_input(
                "❌ NÃO contém (NOT)",
                value=st.session_state[f"{key}_not_val"],
                key=f"{key}_not",
            )

            # SRE FIX: Apenas sincroniza _val ← widget se o backing store não foi zerado
            # pelo clear_filter_state. Isso previne o re-preenchimento fantasma após limpeza.
            if st.session_state.get(f"{key}_or_val", "") or or_terms:
                st.session_state[f"{key}_or_val"] = or_terms
            if st.session_state.get(f"{key}_and_val", "") or and_terms:
                st.session_state[f"{key}_and_val"] = and_terms
            if st.session_state.get(f"{key}_not_val", "") or not_terms:
                st.session_state[f"{key}_not_val"] = not_terms

            # --- LEXICAL PARSER EXTRACTED TO ADAPTER ---
            from presentation.adapters.parsers import parse_term

            # --- CONSTRUTOR DE SQL SOTA (Com strip_accents) ---
            if and_terms or or_terms or not_terms:
                # ESTRATÉGIA OLAP: AGRUPAMENTO POR ENTIDADE (PACIENTE)
                if aggregate_by:
                    having_conds = []
                    if or_terms:
                        ui_tracker.append(
                            {
                                "text": f"✅ {label}: {or_terms}",
                                "keys": [f"{key}_or_val", f"{key}_or", f"{key}_toggle"],
                            }
                        )
                        words = [w for w in or_terms.split(",") if w.strip()]
                        if words:
                            or_expr = [
                                f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}'))"
                                for w in words
                            ]
                            having_conds.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append(
                            {
                                "text": f"⚠️ AND {label}: {and_terms}",
                                "keys": [f"{key}_and_val", f"{key}_and", f"{key}_toggle"],
                            }
                        )
                        for w in [w for w in and_terms.split(",") if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(
                                f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}'))"
                            )

                    if not_terms:
                        ui_tracker.append(
                            {
                                "text": f"❌ {label}: {not_terms}",
                                "keys": [f"{key}_not_val", f"{key}_not", f"{key}_toggle"],
                            }
                        )
                        for w in [w for w in not_terms.split(",") if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(
                                f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')) = FALSE"
                            )

                    if having_conds:
                        subquery = f'SELECT "{aggregate_by}" FROM gercon GROUP BY "{aggregate_by}" HAVING {" AND ".join(having_conds)}'
                        clauses.append(f'"{aggregate_by}" IN ({subquery})')

                # ESTRATÉGIA NORMAL: FILTRO POR EVENTO/LINHA
                else:
                    if or_terms:
                        ui_tracker.append(
                            {
                                "text": f"✅ {label}: {or_terms}",
                                "keys": [f"{key}_or_val", f"{key}_or", f"{key}_toggle"],
                            }
                        )
                        words = [w for w in or_terms.split(",") if w.strip()]
                        if words:
                            or_expr = [
                                f"strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}')"
                                for w in words
                            ]
                            clauses.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append(
                            {
                                "text": f"⚠️ AND {label}: {and_terms}",
                                "keys": [f"{key}_and_val", f"{key}_and", f"{key}_toggle"],
                            }
                        )
                        for w in [w for w in and_terms.split(",") if w.strip()]:
                            p_term = parse_term(w)
                            clauses.append(
                                f"strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')"
                            )

                    if not_terms:
                        ui_tracker.append(
                            {
                                "text": f"❌ {label}: {not_terms}",
                                "keys": [f"{key}_not_val", f"{key}_not", f"{key}_toggle"],
                            }
                        )
                        for w in [w for w in not_terms.split(",") if w.strip()]:
                            p_term = parse_term(w)
                            clauses.append(
                                f"strip_accents(\"{column}\") NOT ILIKE strip_accents('{p_term}')"
                            )

    return " AND ".join(clauses)


# --- 4.5 BFF: IDENTITY AWARE PROXY (IAP) & BFF MOCK ---


def _is_cloud_run() -> bool:
    """Detecta se o runtime é Google Cloud Run via variável injetada automaticamente."""
    return bool(os.getenv("K_SERVICE"))


def _is_dev_mock_allowed() -> bool:
    """
    Guarda de Segurança Dupla para bypass de autenticação em desenvolvimento.
    WHY: Usar ENVIRONMENT=='dev' sozinho é fraco demais - qualquer deploy acidental com
    ENVIRONMENT=dev (como ocorreu no Cloud Run) habilita acesso sem login.
    Requer explicitamente ALLOW_UNAUTHENTICATED_DEV=true como segundo fator de opt-in.
    Cloud Run e prod NUNCA devem ter esta variável setada.
    """
    environment = os.getenv("ENVIRONMENT", "production").lower()
    allow_dev = os.getenv("ALLOW_UNAUTHENTICATED_DEV", "false").lower() == "true"
    return environment in ("local", "dev") and allow_dev


def _cloud_run_login_gate():
    """
    ADR-004 Phase 1: Login gate para Cloud Run via senha compartilhada.
    WHY: Cloud Run serverless não executa Keycloak/oauth2-proxy como sidecar.
    Usa CLOUD_RUN_AUTH_PASSWORD (injetado via Cloud Run secrets) como gate temporário
    enquanto Firebase Auth (Phase 2) não é implementado.
    Retorna True se o usuário já está autenticado na session, False caso contrário.
    """
    import hashlib

    # Já autenticou nesta sessão Streamlit
    if st.session_state.get("cloud_run_authenticated"):
        return True

    expected_hash = os.getenv("CLOUD_RUN_AUTH_PASSWORD_HASH", "")
    expected_plain = os.getenv("CLOUD_RUN_AUTH_PASSWORD", "")

    # Fail-fast: Nenhuma senha configurada no Cloud Run
    if not expected_hash and not expected_plain:
        st.error(
            "🚨 **Configuração Ausente.** "
            "`CLOUD_RUN_AUTH_PASSWORD` não está definido no Cloud Run. "
            "Contate o administrador."
        )
        st.stop()
        return False  # pragma: no cover

    st.markdown(
        """
        <div style="text-align: center; margin-top: 60px;">
            <h1 style="font-family: 'Inter', sans-serif; color: #1e293b;">🎯 Gercon Analytics</h1>
            <p style="color: #64748b; font-size: 1.1rem;">Sistema de Regulação Clínica</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_left, col_center, col_right = st.columns([1, 2, 1])
    with col_center:
        with st.form("cloud_run_login", clear_on_submit=True):
            st.subheader("🔐 Login")
            password = st.text_input(
                "Senha de Acesso", type="password", key="cr_pwd"
            )
            submitted = st.form_submit_button(
                "Entrar", use_container_width=True, type="primary"
            )

        if submitted and password:
            # Validação: compara hash SHA-256 ou fallback para texto plano
            pwd_sha256 = hashlib.sha256(password.encode()).hexdigest()

            is_valid = False
            if expected_hash:
                is_valid = pwd_sha256 == expected_hash.lower()
            elif expected_plain:
                is_valid = password == expected_plain

            if is_valid:
                st.session_state["cloud_run_authenticated"] = True
                st.rerun()
            else:
                st.error("❌ Senha incorreta.")

    st.stop()
    return False  # pragma: no cover


def get_authenticated_user():
    """
    SRE BFF Pattern: Estratégia de autenticação adaptável por runtime.
    - Cloud Run: Password gate (ADR-004) → Mock user com perfil clínico.
    - Docker Compose (RDE): IAP Proxy headers → JWT Keycloak.
    - Local Dev: Mock dupla-guarda (ENVIRONMENT + ALLOW_UNAUTHENTICATED_DEV).
    """
    import time
    from infrastructure.auth.token_acl import ValidatedUserToken

    # === PATH 1: Desenvolvimento Local sem IAP Proxy ===
    # WHY: Guarda dupla — não basta ENVIRONMENT=dev; exige ALLOW_UNAUTHENTICATED_DEV=true.
    if _is_dev_mock_allowed():
        mock_user = ValidatedUserToken(
            sub="dev-id-123",
            email="dev@gercon.com",
            preferred_username="dev_user",
            roles=["diretor_medico"],
            crm_numero="99999",
            crm_uf="RS",
            exp=int(time.time() + 86400),
        )
        return mock_user, "mock-jwt-token"

    # === PATH 2: Cloud Run Serverless (sem Keycloak/oauth2-proxy) ===
    # ADR-004: Password gate → cria sessão com perfil clínico default.
    # TODO(ADR-004/Phase2): Substituir por Firebase Auth com Firestore user profiles.
    if _is_cloud_run():
        _cloud_run_login_gate()  # Bloqueia com st.stop() se não autenticado
        cloud_user = ValidatedUserToken(
            sub="cloud-run-user",
            email=os.getenv("CLOUD_RUN_DEFAULT_EMAIL", "clinico@gercon.com"),
            preferred_username=os.getenv("CLOUD_RUN_DEFAULT_USER", "clinico"),
            roles=[os.getenv("CLOUD_RUN_DEFAULT_ROLE", "diretor_medico")],
            crm_numero=os.getenv("CLOUD_RUN_CRM_NUMERO"),
            crm_uf=os.getenv("CLOUD_RUN_CRM_UF"),
            exp=int(time.time() + 86400),
        )
        return cloud_user, "cloud-run-session"

    # === PATH 3: Docker Compose / K8s com OAuth2-Proxy (Prod original) ===
    # Extração do Header injetado pelo OAuth2-Proxy (Streamlit 1.37+)
    auth_header = (
        st.context.headers.get("x-forwarded-access-token")
        or st.context.headers.get("x-auth-request-access-token")
        or st.context.headers.get("authorization", "").replace("Bearer ", "")
    )

    if not auth_header:
        raise ValueError("Missing Authentication Headers (IAP Proxy)")

    from infrastructure.auth.jwt_validator import verify_token

    user = verify_token(auth_header)
    return user, auth_header



# --- 4.6 SIDEBAR: USER IDENTITY WIDGET (Keycloak / IAP / Cloud Run) ---
def _render_user_widget(user) -> None:
    """
    Renderiza o card de identidade do usuário no topo da sidebar.
    WHY: Adapta o logout conforme o runtime:
    - Cloud Run: Limpa session_state e recarrega (sem proxy/Keycloak).
    - Docker Compose: Destroi sessão Redis do OAuth2-Proxy + SSO Keycloak.
    """
    username = getattr(user, "preferred_username", None) or getattr(user, "email", "?")
    display_name = username.split("@")[0].replace(".", " ").replace("_", " ").title()

    if _is_cloud_run():
        # Cloud Run: Logout simples — limpa session_state do Streamlit
        st.sidebar.markdown(
            f"👤 **{display_name}**",
        )
        if st.sidebar.button(
            "🚪 Logout",
            use_container_width=True,
            key="cloud_run_logout",
        ):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
    else:
        # Docker Compose / K8s: Logout com cadeia de redirects OAuth2-Proxy → Keycloak
        # WHY: Logout exige dois passos — limpar cookie do OAuth2-Proxy E destruir SSO Keycloak.
        from urllib.parse import quote

        keycloak_base = os.getenv(
            "KEYCLOAK_SERVER_URL", "http://iam.127.0.0.1.nip.io:8080"
        )
        realm = os.getenv("KEYCLOAK_REALM", "gercon-realm")
        client_id = os.getenv("KEYCLOAK_CLIENT_ID", "gercon-analytics")
        post_logout_uri = f"http://{os.getenv('EXTERNAL_DOMAIN', '127.0.0.1.nip.io')}/dashboard/"

        keycloak_logout = (
            f"{keycloak_base}/realms/{realm}/protocol/openid-connect/logout"
            f"?client_id={client_id}"
            f"&post_logout_redirect_uri={quote(post_logout_uri, safe='')}"
        )

        st.sidebar.markdown(
            f"""
            <form action="/oauth2/sign_out" method="GET" style="margin: 10px 0;">
                <input type="hidden" name="rd" value="{keycloak_logout}" />
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




# --- 5. MAIN APP ---
def main():
    setup_ui()
    # WHY: cache_resource.clear() + cache_data.clear() removidos do loop de rerun.
    # Esses calls destruíiam a conexão DuckDB e o use_case a cada interação do usuário,
    # cascãideando falhas transitórias que disparavam o alerta de sessão expirada.
    # O cache é válido e gerenciado pelos decoradores @st.cache_resource/@st.cache_data.

    import time

    # === CAMADA 1: Sessão já ativa e válida — zero fricção ===
    _user_already_in_state = "user" in st.session_state
    _token_exp = st.session_state.get("token_exp", 0)
    _token_still_valid = _token_exp > time.time()

    if _user_already_in_state and _token_still_valid:
        # Caminho feliz: usuário ativo, sem nenhuma verificação adicional neste rerun
        pass

    elif _user_already_in_state and not _token_still_valid:
        # === CAMADA 2: Token venceu — apresenta CTA de renovar ===
        st.warning(
            "⏱️ Sua sessão de 24h expirou. Clique em **Renovar Login** para continuar.",
            icon="🔒",
        )
        if _is_cloud_run():
            # Cloud Run: Limpa session e recarrega (sem oauth2-proxy)
            if st.button("🔄 Renovar Login", type="primary"):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.rerun()
        else:
            # Docker Compose: Redireciona para cadeia OAuth2-Proxy → Keycloak
            st.link_button(
                "🔄 Renovar Login",
                "/oauth2/sign_out?rd=/dashboard/",
                type="primary",
            )
        st.stop()

    else:
        # === CAMADA 3: Primeira carga — autentica e popula session_state ===
        try:
            user_domain, jwt_str = get_authenticated_user()
            st.session_state.user = user_domain
            st.session_state.raw_jwt = jwt_str
            # SRE: Sessão de 24h alinhada com a política de sessão clínica
            st.session_state.token_exp = (
                user_domain.exp if user_domain.exp else (time.time() + 86400)
            )
            # Força o rerun já com a sessão populada para injetar o CSS e carregar o app
            st.rerun()
        except Exception as _auth_err:
            # Falha real de autenticação (ex: header IAP ausente, token inválido)
            if _is_cloud_run():
                # Cloud Run: O password gate já tratou tudo via st.stop() dentro
                # de _cloud_run_login_gate(). Se chegou aqui, é um bug.
                st.error(
                    "🚨 **Erro inesperado de autenticação no Cloud Run.** "
                    "Recarregue a página."
                )
            else:
                # Docker Compose: Mostra botão de login via OAuth2-Proxy
                st.error(
                    "🚨 **Acesso não autorizado.** "
                    "Não foi possível verificar a sua identidade."
                )
                st.markdown(
                    """
                    <div style="display: flex; justify-content: center; margin-top: 20px;">
                        <form action="/oauth2/start" method="GET">
                            <input type="hidden" name="rd" value="/dashboard/" />
                            <button type="submit" style="
                                background-color: #ef4444;
                                color: white;
                                padding: 12px 32px;
                                border-radius: 12px;
                                font-weight: 600;
                                font-size: 1.1rem;
                                box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                                border: 2px solid #ef4444;
                                cursor: pointer;
                                font-family: 'Source Sans Pro', sans-serif;
                                transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
                            ">
                                🔑 Realizar Login (Keycloak)
                            </button>
                        </form>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            
            # Debug (Opcional): Remova em prod se não quiser expor headers
            if os.getenv("APP__DEBUG", "false").lower() == "true":
                with st.expander("🛠️ Debug Identity (Headers detectados)"):
                    st.write("Headers detectados via st.context.headers:")
                    st.json({k: v for k, v in st.context.headers.items() if k.lower().startswith("x-")})
                    
            st.stop()

    inject_custom_css()
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f"⚠️ Base Parquet não encontrada ({settings.OUTPUT_FILE}).")
        return

    # ==========================================
    # SIDEBAR: USER IDENTITY WIDGET (IAP / Keycloak)
    # ==========================================
    _render_user_widget(st.session_state.user)

    st.title("🎯 Gercon SRE | Advanced Root Cause Analysis")

    # ==========================================
    # SRE FIX: DICIONÁRIO DE NOMENCLATURAS (UBIQUITOUS LANGUAGE)
    # ==========================================
    MAPA_NOMENCLATURAS = {
        "entidade_especialidade_especialidadeMae_descricao": "Especialidade Mãe",
        "entidade_especialidade_descricao": "Especialidade Fina",
        "entidade_especialidade_cbo_descricao": "CBO Especialidade",
        "entidade_cidPrincipal_codigo": "CID Principal (Código)",
        "entidade_cidPrincipal_descricao": "CID Principal (Descrição)",
        "origem_lista": "Origem (Lista)",
        "situacao": "Situação Atual",
        "entidade_especialidade_tipoRegulacao": "Tipo de Regulação",
        "entidade_especialidade_ativa": "Especialidade Ativa",
        "entidade_especialidade_teleconsulta": "Aceita Teleconsulta",
        "entidade_centralRegulacao_nome": "Central de Regulação",
        "entidade_unidadeOperador_centralRegulacao_nome": "Unidade Op. Central Regulação",
        "liminarOrdemJudicial": "Liminar / Ordem Judicial",
        "entidade_unidadeOperador_nome": "Unidade Operadora",
        "entidade_unidadeOperador_razaoSocial": "Unidade Operadora (Razão Social)",
        "entidade_unidadeOperador_tipoUnidade_descricao": "Tipo de Unidade Operadora",
        "medicoSolicitante": "Médico Solicitante",
        "operador_nome": "Operador",
        "usuarioSolicitante_nome": "Usuário Solicitante",
        "evolucoes_json": "Origem da Informação",
        "historico_evolucoes_completo": "Tipo de Informação",
        "entidade_complexidade": "Complexidade",
        "entidade_classificacaoRisco_cor": "Cor da Classificação de Risco",
        "corRegulador": "Cor do Regulador",
        "usuarioSUS_municipioResidencia_nome": "Município de Residência",
        "usuarioSUS_bairro": "Bairro",
        "usuarioSUS_sexo": "Sexo",
        "usuarioSUS_racaCor": "Raça/Cor",
        "usuarioSUS_nacionalidade": "Nacionalidade",
    }

    # ==========================================
    # SRE FIX: DICIONÁRIO MESTRE DE CORES (GLOBAL)
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

    # WHY: O filtro padrão é Origem da Lista = 'Fila de Espera', mas deve ser
    # selecionável como qualquer outro filtro — o usuário pode modificá-lo ou
    # removê-lo. A pré-seleção acontece APENAS na primeira carga da sessão
    # via session_state, e o render_include_exclude gera a cláusula SQL
    # dinamicamente a partir do estado do widget (como todos os outros filtros).
    _DEFAULT_LISTA = "Fila de Espera"

    # Pré-seleciona o widget SOMENTE na primeira carga da sessão.
    # Após isso, o usuário controla o valor — não sobrescrevemos mais.
    if "lst_in" not in st.session_state:
        st.session_state["lst_in"] = [_DEFAULT_LISTA]

    clauses = ["1=1"]
    curr_where = "1=1"

    ui_filters = {
        "🩺 Clínico & Regulação": [],
        "🏛️ Governança & Atores": [],
        "📅 Ciclo de Vida (Datas)": [],
        "🌍 Demografia & Rede": [],
        "⚠️ Triagem & Classificação de Risco": [],
        "🎯 Desfechos, Gargalos & SLA": [],
    }
    state_keys = {k: [] for k in ui_filters.keys()}



    # ==========================================
    # CASCADING SIDEBAR (TOP-DOWN FLOW OTIMIZADO)
    # ==========================================
    st.sidebar.header("🎛️ Filtros em Cascata")

    cat = "🩺 Clínico & Regulação"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude(
            "Especialidade Mãe",
            "entidade_especialidade_especialidadeMae_descricao",
            clauses,
            curr_where,
            "espm",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Especialidade Fina",
            "entidade_especialidade_descricao",
            clauses,
            curr_where,
            "espf",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "CBO Especialidade",
            "entidade_especialidade_cbo_descricao",
            clauses,
            curr_where,
            "esp_cbo",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Descrição Auxiliar",
            "entidade_especialidade_descricaoAuxiliar",
            clauses,
            curr_where,
            "esp_aux",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(
            "Médico Solicitante",
            "medicoSolicitante",
            clauses,
            curr_where,
            "med_sol",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Unidade Operadora",
            "entidade_unidadeOperador_nome",
            clauses,
            curr_where,
            "usol",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(
            "CID Principal (Código)",
            "entidade_cidPrincipal_codigo",
            clauses,
            curr_where,
            "cid_cod",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_advanced_text_search(
            "CID Principal (Descrição)",
            "entidade_cidPrincipal_descricao",
            clauses,
            "txt_cid_desc",
            ui_filters[cat],
            state_keys[cat],
        )
        # MÁGICA CLÍNICA MOVIDA: Agregação pelo numeroCMCE inteiro
        st.markdown("---")
        curr_where = render_advanced_text_search(
            "Evoluções do Paciente",
            "historico_quadro_clinico",
            clauses,
            "txt_evo",
            ui_filters[cat],
            state_keys[cat],
            aggregate_by="numeroCMCE",
        )
        curr_where = " AND ".join(clauses)

    cat = "🏛️ Governança & Atores"
    # WHY: Abre expandido na primeira carga para o usuário ver o filtro
    # padrão "Fila de Espera" pré-selecionado e poder modificá-lo.
    _gov_expanded = "lst_in" not in st.session_state or bool(st.session_state.get("lst_in"))
    with st.sidebar.expander(cat, expanded=_gov_expanded):
        # Atores movidos da antiga aba de Evoluções
        curr_where = render_advanced_text_search(
            "Tipo de Informação",
            "historico_evolucoes_completo",
            clauses,
            "txt_tinf",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_advanced_text_search(
            "Origem da Informação",
            "evolucoes_json",
            clauses,
            "txt_orig_inf",
            ui_filters[cat],
            state_keys[cat],
        )
        st.markdown("---")

        curr_where = render_include_exclude(
            "Origem (Lista)",
            "origem_lista",
            clauses,
            curr_where,
            "lst",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Situação Atual",
            "situacao",
            clauses,
            curr_where,
            "sit",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Tipo de Regulação",
            "entidade_especialidade_tipoRegulacao",
            clauses,
            curr_where,
            "treg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Especialidade Ativa",
            "entidade_especialidade_ativa",
            clauses,
            curr_where,
            "stesp",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_presence_radio(
            "Liminar / Ordem Judicial",
            "liminarOrdemJudicial",
            clauses,
            "oj",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        curr_where = render_include_exclude(
            "Operador",
            "operador_nome",
            clauses,
            curr_where,
            "op_nome",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Usuário Solicitante",
            "usuarioSolicitante_nome",
            clauses,
            curr_where,
            "usu_sol_nome",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_include_exclude(
            "Central de Regulação",
            "entidade_centralRegulacao_nome",
            clauses,
            curr_where,
            "cent_reg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Unidade Op. Central Regulação",
            "entidade_unidadeOperador_centralRegulacao_nome",
            clauses,
            curr_where,
            "uni_op_cent",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Unidade de Referência",
            "entidade_unidadeReferencia_nome",
            clauses,
            curr_where,
            "uni_ref",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_boolean_radio(
            "Possui DITA",
            "entidade_possuiDita",
            clauses,
            "dita",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Fora da Regionalização",
            "entidade_foraDaRegionalizacao",
            clauses,
            "freg",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Regularização de Acesso",
            "regularizacaoAcesso",
            clauses,
            "reg_acc",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Aceita Teleconsulta",
            "entidade_especialidade_teleconsulta",
            clauses,
            "tele",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Matriciamento",
            "entidade_especialidade_matriciamento",
            clauses,
            "matri",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Sem Classificação",
            "entidade_semClassificacao",
            clauses,
            "sem_class",
            ui_filters[cat],
            state_keys[cat],
        )

        curr_where = " AND ".join(clauses)

    cat = "📅 Ciclo de Vida (Datas)"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_smart_date_range(
            "Data de Solicitação",
            "dataSolicitacao",
            clauses,
            "dt_solic",
            ui_filters[cat],
            state_keys[cat],
            default_to_30_days=True,
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Data de Cadastro",
            "dataCadastro",
            clauses,
            "dt_cad",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Data da Evolução",
            "dataCadastro",
            clauses,
            "dt_evo",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Primeiro Agendamento",
            "dataPrimeiroAgendamento",
            clauses,
            "dt_pagend",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Primeira Autorização",
            "dataPrimeiraAutorizacao",
            clauses,
            "dt_paut",
            ui_filters[cat],
            state_keys[cat],
        )

    cat = "🌍 Demografia & Rede"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_advanced_text_search(
            "Pesquisa: Nome do Paciente",
            "usuarioSUS_nomeCompleto",
            clauses,
            "txt_pac_nome",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = " AND ".join(clauses)
        st.markdown("---")

        curr_where = render_include_exclude(
            "Município de Residência",
            "usuarioSUS_municipioResidencia_nome",
            clauses,
            curr_where,
            "mun",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Bairro",
            "usuarioSUS_bairro",
            clauses,
            curr_where,
            "bai",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        # Logradouro com a condicional injetando a numeração dento da Deep Search
        curr_where = render_advanced_text_search(
            "Logradouro",
            "usuarioSUS_logradouro",
            clauses,
            "txt_logr",
            ui_filters[cat],
            state_keys[cat],
        )
        if st.session_state.get("txt_logr_toggle", False):
            st.markdown(
                "<div style='margin-left: 1rem; border-left: 2px solid #cbd5e1; padding-left: 0.5rem;'>",
                unsafe_allow_html=True,
            )
            state_keys[cat].extend(["num_min", "num_max"])
            # SRE FIX: Inicializa estado antes do widget para evitar mismatch de valor
            if "num_min" not in st.session_state:
                st.session_state["num_min"] = 0
            if "num_max" not in st.session_state:
                st.session_state["num_max"] = 99999
            col_nmin, col_nmax = st.columns(2)
            v_nmin = col_nmin.number_input(
                "Nº Min", min_value=0, max_value=99999,
                step=10, key="num_min", label_visibility="collapsed"
            )
            v_nmax = col_nmax.number_input(
                "Nº Max", min_value=0, max_value=99999,
                step=100, key="num_max", label_visibility="collapsed"
            )
            if v_nmin > 0 or v_nmax < 99999:
                ui_filters[cat].append(
                    {
                        "text": f"Nº Logradouro: {v_nmin} a {v_nmax}",
                        "keys": ["num_min", "num_max"],
                    }
                )
                clauses.append(
                    f'TRY_CAST("usuarioSUS_numero" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}'
                )
            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()  # --- Separador Visual de Identificação Pessoal ---

        curr_where = " AND ".join(clauses)
        curr_where = render_include_exclude(
            "Sexo",
            "usuarioSUS_sexo",
            clauses,
            curr_where,
            "sex",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        # Componente que injeta entidade_idade_idadeInteiro (com Slider Duplo)
        curr_where = render_age_slider(
            "Faixa Etária (Idade)", clauses, "f_idade", ui_filters[cat], state_keys[cat]
        )

        curr_where = render_include_exclude(
            "Cor/Raça",
            "usuarioSUS_racaCor",
            clauses,
            curr_where,
            "cor",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Nacionalidade",
            "usuarioSUS_nacionalidade",
            clauses,
            curr_where,
            "nac",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

    cat = "⚠️ Triagem & Classificação de Risco"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude(
            "Complexidade",
            "entidade_complexidade",
            clauses,
            curr_where,
            "cpx",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Risco Cor (Atual)",
            "entidade_classificacaoRisco_cor",
            clauses,
            curr_where,
            "r_cor",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Cor do Regulador",
            "corRegulador",
            clauses,
            curr_where,
            "c_reg",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        curr_where = render_boolean_radio(
            "Reclassificada pelo Solicitante",
            "entidade_classificacaoRisco_reclassificadaSolicitante",
            clauses,
            "r_recl",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        curr_where = render_dual_slider(
            "Pontos Gravidade",
            "entidade_classificacaoRisco_pontosGravidade",
            clauses,
            "pt_grav",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Pontos Tempo",
            "entidade_classificacaoRisco_pontosTempo",
            clauses,
            "pt_tmp",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Pontos Totais",
            "entidade_classificacaoRisco_totalPontos",
            clauses,
            "pt_tot",
            ui_filters[cat],
            state_keys[cat],
        )

    cat = "🎯 Desfechos, Gargalos & SLA"
    with st.sidebar.expander(cat, expanded=False):
        # 1. Tipo de Desfecho — inclui "EM ANDAMENTO" para casos sem desfecho ainda
        _desfecho_options_raw = get_dynamic_options("SLA_Tipo_Desfecho", curr_where, st.session_state.user)
        _desfecho_options = sorted(set([o for o in _desfecho_options_raw if o])) + ["EM ANDAMENTO"]
        cat_keys_desfecho = ["sla_tipo_in", "sla_tipo_ex"]
        state_keys[cat].extend(cat_keys_desfecho)
        _c1, _c2 = st.columns(2)
        _sla_incl = _c1.multiselect(
            "Tipo de Desfecho ✅", _desfecho_options,
            key="sla_tipo_in", label_visibility="collapsed", placeholder="✅ Incluir..."
        )
        _sla_excl = _c2.multiselect(
            "Tipo de Desfecho ❌", _desfecho_options,
            key="sla_tipo_ex", label_visibility="collapsed", placeholder="❌ Excluir..."
        )
        st.write(
            "<span style='font-size:0.9em;font-weight:600;color:#4B5563;'>Tipo de Desfecho</span>",
            unsafe_allow_html=True,
        )
        if _sla_incl:
            ui_filters[cat].append({"text": f"✅ Tipo Desfecho: {', '.join(_sla_incl)}", "keys": ["sla_tipo_in"]})
            _parts = []
            if "EM ANDAMENTO" in _sla_incl:
                _rest = [v for v in _sla_incl if v != "EM ANDAMENTO"]
                _parts.append('("SLA_Tipo_Desfecho" IS NULL OR "SLA_Tipo_Desfecho" = \'\')')
                if _rest:
                    _safe = "', '".join(v.replace("'", "''") for v in _rest)
                    _parts.append(f'"SLA_Tipo_Desfecho" IN (\'{_safe}\')')
            else:
                _safe = "', '".join(v.replace("'", "''") for v in _sla_incl)
                _parts.append(f'"SLA_Tipo_Desfecho" IN (\'{_safe}\')')
            clauses.append(f"({' OR '.join(_parts)})")
            curr_where = " AND ".join(clauses)
        if _sla_excl:
            ui_filters[cat].append({"text": f"❌ Tipo Desfecho: {', '.join(_sla_excl)}", "keys": ["sla_tipo_ex"]})
            _parts_ex = []
            if "EM ANDAMENTO" in _sla_excl:
                _rest_ex = [v for v in _sla_excl if v != "EM ANDAMENTO"]
                _parts_ex.append('("SLA_Tipo_Desfecho" IS NOT NULL AND "SLA_Tipo_Desfecho" != \'\')')
                if _rest_ex:
                    _safe_ex = "', '".join(v.replace("'", "''") for v in _rest_ex)
                    _parts_ex.append(f'"SLA_Tipo_Desfecho" NOT IN (\'{_safe_ex}\')')
            else:
                _safe_ex = "', '".join(v.replace("'", "''") for v in _sla_excl)
                _parts_ex.append(f'"SLA_Tipo_Desfecho" NOT IN (\'{_safe_ex}\')')
            clauses.append(f"({' AND '.join(_parts_ex)})")
            curr_where = " AND ".join(clauses)

        curr_where = render_include_exclude(
            "Status Provisório",
            "statusProvisorio",
            clauses,
            curr_where,
            "st_prov",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        # WHY: motivoPendencia é armazenado como string Python-dict (aspas simples), não JSON válido.
        # json_extract_string falha. A solução é regexp_extract diretamente no DuckDB para
        # extrair tipo, descricao e status de forma segura e sem chamar o contexto do app.
        st.markdown("**📦 Motivo Pendência**")
        _pend_fields = [
            (
                "Tipo",
                "mot_pend_tipo",
                r"regexp_extract(motivoPendencia, '''tipo'': ''([^'']+)''', 1)",
                r"regexp_extract(motivoPendencia, '''tipo'': ''([^'']+)''', 1)",
            ),
            (
                "Descrição",
                "mot_pend_desc",
                r"regexp_extract(motivoPendencia, '''descricao'': ''([^'']+)''', 1)",
                r"regexp_extract(motivoPendencia, '''descricao'': ''([^'']+)''', 1)",
            ),
            (
                "Status",
                "mot_pend_sta",
                r"regexp_extract(motivoPendencia, '''status'': ''([^'']+)''', 1)",
                r"regexp_extract(motivoPendencia, '''status'': ''([^'']+)''', 1)",
            ),
        ]
        _uc = get_use_case()
        for _pf_label, _pf_key, _pf_expr, _pf_filter_expr in _pend_fields:
            try:
                _pf_sql = (
                    f"SELECT DISTINCT {_pf_expr} AS val "
                    f"FROM gercon "
                    f"WHERE {curr_where} "
                    f"AND motivoPendencia IS NOT NULL "
                    f"AND motivoPendencia != '' "
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
            st.caption(f"Pendência — {_pf_label}")
            _pf_c1, _pf_c2 = st.columns(2)
            _pf_incl = _pf_c1.multiselect(
                f"Pendência {_pf_label} ✅",
                sorted(set(str(o) for o in _pf_opts)),
                key=f"{_pf_key}_in",
                label_visibility="collapsed",
                placeholder="✅ Incluir...",
            )
            _pf_excl = _pf_c2.multiselect(
                f"Pendência {_pf_label} ❌",
                sorted(set(str(o) for o in _pf_opts)),
                key=f"{_pf_key}_ex",
                label_visibility="collapsed",
                placeholder="❌ Excluir...",
            )
            if _pf_incl:
                # WHY: regexp_matches é verdadeiro se o padrão aparece na string — não usamos IN()
                # pois o campo inteiro é o dict Python, não o valor isolado.
                _pf_pattern = "|".join(
                    v.replace("'", "''").replace("(", r"\(").replace(")", r"\)")
                    for v in _pf_incl
                )
                clauses.append(
                    f"regexp_matches(motivoPendencia, '(?i){_pf_pattern}')"
                )
                ui_filters[cat].append({
                    "text": f"✅ Pendência {_pf_label}: {', '.join(_pf_incl)}",
                    "keys": [f"{_pf_key}_in"],
                })
                curr_where = " AND ".join(clauses)
            if _pf_excl:
                _pf_pattern_ex = "|".join(
                    v.replace("'", "''").replace("(", r"\(").replace(")", r"\)")
                    for v in _pf_excl
                )
                clauses.append(
                    f"NOT regexp_matches(motivoPendencia, '(?i){_pf_pattern_ex}')"
                )
                ui_filters[cat].append({
                    "text": f"❌ Pendência {_pf_label}: {', '.join(_pf_excl)}",
                    "keys": [f"{_pf_key}_ex"],
                })
                curr_where = " AND ".join(clauses)

        st.markdown("---")
        curr_where = render_include_exclude(
            "Motivo Cancelamento",
            "motivoCancelamento",
            clauses,
            curr_where,
            "mot_canc",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )
        curr_where = render_include_exclude(
            "Motivo Encerramento",
            "motivoEncerramento",
            clauses,
            curr_where,
            "mot_enc",
            ui_filters[cat],
            state_keys[cat],
            st.session_state.user,
        )

        st.markdown("---")
        # 2. Textos de Justificativa (Deep Search)
        curr_where = render_advanced_text_search(
            "Justificativa de Retorno",
            "justificativaRetorno",
            clauses,
            "txt_retorno",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        # 3. Marcos de Sucesso (Booleans)
        curr_where = render_boolean_radio(
            "1. Passou por Autorização?",
            "SLA_Marco_Autorizada",
            clauses,
            "m_aut",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "2. Chegou a Agendar?",
            "SLA_Marco_Agendada",
            clauses,
            "m_agd",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "3. Foi Realizada?",
            "SLA_Marco_Realizada",
            clauses,
            "m_rea",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Fila Finalizada? (Timer Parado)",
            "SLA_Desfecho_Atingido",
            clauses,
            "m_fim",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        # 4. Sliders de SLA (Métricas calculadas em dias e interações)
        curr_where = render_dual_slider(
            "Lead Time Total (Dias)",
            "SLA_Lead_Time_Total_Dias",
            clauses,
            "sla_tot",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Tempo com Regulador (Dias)",
            "SLA_Tempo_Regulador_Dias",
            clauses,
            "sla_reg",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Tempo com Solicitante (Dias)",
            "SLA_Tempo_Solicitante_Dias",
            clauses,
            "sla_sol",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(
            "Volume de Interações (Ping-Pong)",
            "SLA_Interacoes_Regulacao",
            clauses,
            "sla_int",
            ui_filters[cat],
            state_keys[cat],
        )

    # ==========================================
    # VISUALIZAR E LIMPAR FILTROS ATIVOS (TOP BAR)
    # ==========================================
    has_active_filters = any(len(v) > 0 for v in ui_filters.values())

    if has_active_filters:
        total_count = sum(len(v) for v in ui_filters.values())

        with st.expander(f"🔍 Filtros Ativos ({total_count})", expanded=True):
            for category, filters in ui_filters.items():
                if filters:
                    # 1. TÍTULO NA SUA PRÓPRIA LINHA
                    st.markdown(
                        f"<div class='cat-title'>{category}</div>",
                        unsafe_allow_html=True,
                    )

                    # 2. FILTROS AGRUPADOS NA LINHA SEGUINTE
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

            # 3. LIMPAR TODOS ISOLADO NO FINAL (Sem a linha tracejada)
            st.write("")  # Micro-espaçamento natural
            all_keys = [key for sublist in state_keys.values() for key in sublist]
            st.button(
                "🗑️ Limpar Todos os Filtros",
                key="btn_clear_all",
                on_click=clear_filter_state,
                args=(all_keys,),
            )

        st.write(" ")  # Um micro-espaçamento logo antes dos KPIs para respirar

    # ==========================================
    # DASHBOARD TABS: ESTRUTURADO POR NÍVEL DE DECISÃO
    # ==========================================
    # SRE FIX: Adicionada a aba de KPIs como a primeira (t_kpi) para Executive Summary
    t_kpi, t_macro, t_clin, t_micro = st.tabs(
        [
            "📊 Visão Geral (KPIs)",
            "📈 Estratégia (Macro)",
            "🩺 Inteligência Clínica",
            "🔎 Auditoria (Micro)",
        ]
    )

    # ==========================================
    # CLÁUSULA FINAL E PROCESSAMENTO (KPIs)
    # ==========================================
    FINAL_WHERE = " AND ".join(clauses)
    filters = FilterCriteria(clauses=clauses)
    use_case = get_use_case()

    with st.spinner(
        "Processando Modelo de Leitura (OLAP) e Latência de Cauda (P90)..."
    ):
        kpi_data = use_case.get_executive_summary(filters, st.session_state.user)

    # --- SRE FIX: DATA FRESHNESS SLA MONITOR ---
    if kpi_data.last_sync_at > 0:
        import time
        age_hours = (time.time() - kpi_data.last_sync_at) / 3600
        if age_hours > settings.DATA_SLA_THRESHOLD:
            st.warning(f"⚠️ **Amber Alert:** Os dados exibidos estão com defasagem de {age_hours:.1f} horas. O Worker Scraper pode estar inativo ou ter falhado no último ciclo.")

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
            "<h4 style='font-size: 1rem; color: #4B5563;'>Painel de Desempenho (SLA e Carga)</h4>",
            unsafe_allow_html=True,
        )

        # --- LINHA 1: Volume, Carga e Esforço ---
        r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
        r1_c1.metric(
            "🏢 Origens do Gercon",
            f"{origens:,}".replace(",", "."),
            help="Quantidade de portas de entrada/sistemas de origem distintos.",
        )
        r1_c2.metric(
            "👥 Pacientes",
            f"{pacientes:,}".replace(",", "."),
            help="Número total de pacientes únicos selecionados.",
        )
        r1_c3.metric(
            "📋 Evoluções",
            f"{eventos:,}".replace(",", "."),
            help="Número total de eventos no histórico clínico.",
        )
        r1_c4.metric(
            "📈 Evoluções/Paciente",
            f"{evo_por_paciente}".replace(".", ","),
            help="Média de vezes que o paciente foi movimentado ou avaliado.",
        )

        st.write(" ")

        # --- LINHA 2: Complexidade Clínica e SLA ---
        r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
        r2_c1.metric(
            "🏛️ Especialidades (Mãe)",
            f"{esp_mae:,}".replace(",", "."),
            help="Grandes áreas clínicas abrangidas (Ex: CIRURGIA).",
        )
        r2_c2.metric(
            "🎯 Subespecialidades",
            f"{sub_esp:,}".replace(",", "."),
            help="Especialidades finas abrangidas (Ex: CIRURGIA DA MÃO).",
        )
        r2_c3.metric(
            "🔀 Subs/Especialidade",
            f"{sub_por_esp}".replace(".", ","),
            help="Média de ramificações por grande área clínica.",
        )

        lead_str = (
            f"{lead_time} dias | {max_lead_time} dias"
            if pd.notna(lead_time)
            else "0 dias"
        )
        r2_c4.metric(
            "⏱️ Fila: Média | Pior",
            lead_str,
            help="Tempo Médio vs Tempo do paciente mais antigo.",
        )

        st.write(" ")

        # --- LINHA 3: Governança e Comportamento Médico ---
        r3_c1, r3_c2, r3_c3, r3_c4 = st.columns(4)
        r3_c1.metric(
            "👨⚕️ Médicos Solicitantes",
            f"{medicos:,}".replace(",", "."),
            help="Total de médicos distintos que inseriram pacientes nesta fila.",
        )
        r3_c2.metric(
            "📅 Cadastros/Mês",
            f"{cad_por_mes}".replace(".", ","),
            help="Média mensal histórica de novos pacientes inseridos na fila (baseado na janela filtrada).",
        )
        r3_c3.metric(
            "🧠 Dispersão Diagnóstica",
            f"{cid_por_medico}".replace(".", ","),
            help="Média de CIDs distintos usados por médico.",
        )
        r3_c4.metric(
            "⚙️ Carga/Médico",
            f"{evo_por_medico}".replace(".", ","),
            help="Volume médio de evoluções administrativas geradas por cada médico.",
        )

        st.divider()

        # --- BLOCO 2 CONSOLIDADO: ANATOMIA COMPARATIVA E RISCO ---
        st.markdown(
            "<h4 style='font-size: 1.1rem; font-weight: 600; color: #4B5563; margin-bottom: 0.2rem;'>Anatomia Comparativa: Dispersão e Escala de Espera</h4>",
            unsafe_allow_html=True,
        )

        st.markdown(
            """
            <div style="font-size: 0.85rem; color: #6b7280; margin-bottom: 1.5rem; line-height: 1.4;">
                <b>Como ler os gráficos:</b> A <b>linha central</b> na caixa é a mediana (paciente típico). 
                A <b>caixa</b> agrupa 50% da fila. As <b>linhas tracejadas</b> mostram as fronteiras SRE: 
                <b>P10</b> (os 10% mais rápidos/eficiência) e <b>P90</b> (o limite de 90% da rede/garantia). 
                Os <b>pontos individuais</b> à direita são os <i>outliers</i> (casos críticos fora do padrão).
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
                title="Abandono: Dias sem Evolução",
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
            st.plotly_chart(
                fig_esq, width="stretch", config={"displayModeBar": False}
            )

            # --- RENDERIZAÇÃO: BOXPLOT CADASTRO (AZUL) ---
            df_render_fila = (
                df_plot_fila.sample(n=min(10000, len(df_plot_fila)), random_state=42)
                if not df_plot_fila.empty
                else df_plot_fila
            )
            fig_fila = px.box(
                df_render_fila,
                x="dias_fila",
                title="Cadastro: Dias de Espera",
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
            st.plotly_chart(
                fig_fila, width="stretch", config={"displayModeBar": False}
            )

            if len(df_dist) > len(df_plot_fila) or len(df_dist) > len(df_plot_esq):
                st.caption(
                    "ℹ️ Escala otimizada (outliers extremos ocultos do visor de alcance para facilitar visualização). Estatísticas preservadas."
                )

            # --- 2. INDICADORES P90 (PADRÃO ST.METRIC PARA CONSISTÊNCIA VISUAL) ---
            st.write(" ")
            g_p90_1, g_p90_2 = st.columns(2)

            with g_p90_1:
                st.metric(
                    label="⏳ P90 Tempo Esquecido",
                    value=f"{p90_esquecido} dias",
                    help="90% da rede não recebe atualizações clínicas há até este limite de dias.",
                )

            with g_p90_2:
                st.metric(
                    label="⏱️ P90 Tempo de Fila",
                    value=f"{p90_lead_time} dias",
                    help="90% da rede espera até este limite de dias desde o cadastro para o agendamento.",
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
                        title={"text": "Índice de Gravidade", "font": {"size": 14}},
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
                        title={"text": "Quebra de SLA (>180d)", "font": {"size": 14}},
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
            "📊 Explorador de Fila Dinâmico: Bivariado (Carga vs Latência/Risco)"
        )

        st.info(
            "💡 **Como ler (Gráfico Bivariado SRE):** \n"
            "- **Tamanho da Fatia:** Representa a **Carga (Volume)**. Fatias largas indicam muitos pacientes em espera.\n"
            "- **Cor da Fatia:** Representa a métrica de **Risco/Latência** escolhida. Tons quentes (vermelho) revelam gargalos, pacientes críticos ou faixas etárias avançadas, enquanto tons frios (azul) indicam fluxo rápido ou baixo risco."
        )

        # Dividimos a tela para os dois controles do usuário
        c_hier, c_metric = st.columns([0.7, 0.3])

        with c_hier:
            niveis_sunburst = st.multiselect(
                "Selecione a Hierarquia de Dados (Máx: 5 níveis):",
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
                help="Arraste e solte as tags para reordenar o funil (path) do gráfico.",
                format_func=lambda col: MAPA_NOMENCLATURAS.get(col, col),
            )

        with c_metric:
            st.write(" ")  # Alinhamento visual com o label do multiselect
            # Dicionário SRE: Mapeia a UX para a query OLAP
            METRICAS_COR = {
                "⏳ Tempo de Espera (Fila)": {
                    "sql": "ROUND(AVG(SLA_Lead_Time_Total_Dias), 1)",
                    "unit": "dias",
                },
                "⚠️ Tempo Esquecido (Sem Evolução)": {
                    "sql": "ROUND(AVG(SLA_Tempo_Regulador_Dias), 1)",
                    "unit": "dias",
                },
                "🚨 Pontos de Gravidade": {
                    "sql": "ROUND(AVG(entidade_classificacaoRisco_pontosGravidade), 1)",
                    "unit": "pts",
                },
                "⏱️ Pontos de Tempo": {
                    "sql": "ROUND(AVG(entidade_classificacaoRisco_pontosTempo), 1)",
                    "unit": "pts",
                },
                "🔥 Pontuação Total": {
                    "sql": "ROUND(AVG(entidade_classificacaoRisco_totalPontos), 1)",
                    "unit": "pts",
                },
                "🎂 Idade Média (Demografia)": {
                    "sql": "ROUND(AVG(date_diff('year', TRY_CAST(usuarioSUS_dataNascimento AS DATE), CURRENT_DATE)), 1)",
                    "unit": "anos",
                },
            }

            cor_selecionada = st.selectbox(
                "Métrica da Cor (Temperatura):",
                options=list(METRICAS_COR.keys()),
                index=0,
                help="Define o que a cor de cada fatia representa. O tamanho será sempre o volume de pacientes.",
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
                        .replace("", "Não Informado")
                        .fillna("Não Informado")
                    )

                # Paleta divergente universal (Azul = Baixo Risco/Rápido, Vermelho = Alto Risco/Atraso)
                paleta = "RdYlBu_r"

                fig_sun = px.sunburst(
                    df_plot_sun,
                    path=niveis_sunburst,
                    values="Vol",
                    color="Metrica_Cor",
                    color_continuous_scale=paleta,
                    title=f"Análise Bivariada: Tamanho (Carga) vs Cor ({nome_metrica})",
                    labels={"Vol": "Pacientes", "Metrica_Cor": nome_metrica},
                )

                # SRE UX: Injeta dinamicamente a unidade correta (dias, pts ou anos) e remove bordas
                fig_sun.update_traces(
                    hovertemplate=f"<b>%{{label}}</b><br>Pacientes (Carga): %{{value}}<br>{nome_metrica}: %{{color}} {unidade_cor}<extra></extra>",
                    marker=dict(line=dict(width=0)),
                )

                fig_sun.update_layout(height=700, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(
                    fig_sun, width="stretch", config={"displayModeBar": False}
                )
            else:
                st.warning(
                    "⚠️ Nenhuma data disponível para o Sunburst com os filtros atuais."
                )
        else:
            st.warning("⚠️ Selecione pelo menos 1 nível para renderizar o gráfico.")

        st.markdown("---")
        st.subheader("⏱️ Golden Signals: Governança e Saúde do Fluxo")
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
                        title="Matriz de Risco (Prioridade)",
                    ),
                    width="stretch",
                    config={"displayModeBar": False},
                )

        with c2:
            # Funil de Jornada (Conversão)
            df_funil = use_case.execute_custom_query(
                f"""
                SELECT '1. Solicitado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE}
                UNION ALL
                SELECT '2. Triado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND entidade_classificacaoRisco_cor != ''
                UNION ALL
                SELECT '3. Agendado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND situacao ILIKE '%AGENDADA%'
                UNION ALL
                SELECT '4. Realizado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND (situacao ILIKE '%ATENDIDO%' OR situacao ILIKE '%REALIZADO%')
            """,
                filters,
                st.session_state.user,
            )
            st.plotly_chart(
                px.funnel(
                    df_funil,
                    x="Vol",
                    y="Etapa",
                    title="Funil da Jornada: Gargalos e Abandono",
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
                title="Situação Geral da Rede",
                color="situacao",
                template="plotly_white",
            ),
            width="stretch",
            config={"displayModeBar": False},
        )

    with t_clin:
        st.subheader("Inteligência Clínica & Perfil Demográfico")
        
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
                        .replace("", "Não Informado")
                        .fillna("Não Informado")
                    )
                    st.plotly_chart(
                        px.treemap(
                            df_mun,
                            path=[
                                "usuarioSUS_municipioResidencia_nome",
                                "usuarioSUS_bairro",
                            ],
                            values="Vol",
                            title="Geometria: Município ➔ usuarioSUS_bairro",
                            color="Vol",
                            color_continuous_scale="Viridis",
                        ),
                        width="stretch",
                        config={"displayModeBar": False},
                    )
                if RENDER_LATENCY:
                    RENDER_LATENCY.labels(component="t_clin_treemap").observe(time.time() - start_treemap)
            except Exception:
                if SILENT_ERRORS:
                    SILENT_ERRORS.labels(component="t_clin_treemap").inc()
                st.warning("⚠️ Dados insuficientes ou mal formatados para o Treemap.")

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
                        color_discrete_map={"Feminino": "#ec4899", "Masculino": "#3b82f6"},
                        title="Perfil Demográfico (Idade vs usuarioSUS_sexo)",
                        labels={
                            "Idade_Int": "Idade Aproximada",
                            "Vol": "Volume de Pacientes",
                        },
                    )
                    st.plotly_chart(
                        fig_demo, width="stretch", config={"displayModeBar": False}
                    )
                if RENDER_LATENCY:
                    RENDER_LATENCY.labels(component="t_clin_demographics").observe(time.time() - start_hist)
            except Exception:
                if SILENT_ERRORS:
                    SILENT_ERRORS.labels(component="t_clin_demographics").inc()
                st.warning("⚠️ Erro silencioso capturado na renderização demográfica.")

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
                title="Throughput Temporal: Volume de Pacientes por Origem",
            ),
            width="stretch",
            config={"displayModeBar": False},
        )

        st.markdown("---")
        st.subheader("🕵️ Auditoria de Padrões Clínicos (Local / Ator  ×  Diagnóstico)")

        # --- SELETORES DE DIMENSÃO ---
        # WHY: Hardcodar medicoSolicitante × CID limita a análise.
        # Com seletores livres o gestor pode parear qualquer eixo:
        # "UBS × Especialidade Mãe" revela gargalos regionais; 
        # "Médico × CID" mantém a auditoria individual original.
        _OPCOES_DIAGNOSTICO = {
            "CID — Descrição":             "entidade_cidPrincipal_descricao",
            "CID — Código":                "entidade_cidPrincipal_codigo",
            "Especialidade Mãe":           "entidade_especialidade_especialidadeMae_descricao",
            "Especialidade Fina":          "entidade_especialidade_descricao",
            "CBO (Especialidade)":         "entidade_especialidade_cbo_descricao",
        }
        _OPCOES_ATOR = {
            "Médico Solicitante":          "medicoSolicitante",
            "Unidade Operadora (UBS)":     "entidade_unidadeOperador_nome",
            "Unidade Operadora (Razão Social)": "entidade_unidadeOperador_razaoSocial",
            "Tipo de Unidade":             "entidade_unidadeOperador_tipoUnidade_descricao",
            "Central de Regulação":        "entidade_centralRegulacao_nome",
            "Operador (Regulador)":        "operador_nome",
            "Usuário Solicitante":         "usuarioSolicitante_nome",
        }

        c_dim1, c_dim2 = st.columns(2)
        with c_dim1:
            _label_ator = st.selectbox(
                "📍 Eixo X — Local / Ator:",
                options=list(_OPCOES_ATOR.keys()),
                index=0,
                help="Define quem ou qual local aparece no eixo X do heatmap.",
            )
        with c_dim2:
            _label_diag = st.selectbox(
                "🔬 Eixo Y — Diagnóstico / Dimensão Clínica:",
                options=list(_OPCOES_DIAGNOSTICO.keys()),
                index=0,
                help="Define qual dimensão clínica aparece no eixo Y do heatmap.",
            )

        _col_ator = _OPCOES_ATOR[_label_ator]
        _col_diag = _OPCOES_DIAGNOSTICO[_label_diag]

        # --- 1. DEFINIÇÃO ESTRITA DE VARIÁVEIS DE ESTADO ---
        OPT_CID = "Análise Horizontal (Comparação de Pares)"
        OPT_MED = "Análise Vertical (Perfil Individual)"

        # --- 2. UI UX FIX: Controles Analíticos (Sliders Independentes) ---
        c_top1, c_top2, c_metric = st.columns([0.15, 0.15, 0.7])
        with c_top1:
            top_x_med = st.slider(
                f"Top {_label_ator.split('(')[0].strip()}:",
                min_value=5,
                max_value=100,
                value=15,
                step=1,
                help=f"Define a quantidade de itens de '{_label_ator}' no eixo X.",
            )
        with c_top2:
            top_x_cid = st.slider(
                f"Top {_label_diag.split('(')[0].strip()}:",
                min_value=5,
                max_value=100,
                value=15,
                step=1,
                help=f"Define a quantidade de itens de '{_label_diag}' no eixo Y.",
            )
        with c_metric:
            st.write(" ")
            modo_heatmap = st.radio(
                "Métrica de Visualização Analítica (Desvio Padrão):",
                options=[OPT_CID, OPT_MED],
                horizontal=True,
            )

        # --- CAIXA DE EXPLICAÇÃO DINÂMICA DE LEITURA (UX) ---
        if modo_heatmap == OPT_CID:
            st.info(
                f"💡 **Dica: Análise Horizontal (Comparação de Pares):** Avalia um mesmo **{_label_diag} (linha)** entre todos os '{_label_ator}'. "
                f"Tons quentes (vermelho) indicam que o ator em questão tem frequência **estatisticamente muito acima da média de seus pares**."
            )
        else:
            st.info(
                f"💡 **Dica: Análise Vertical (Perfil Individual):** Avalia a rotina de um único **{_label_ator} (coluna)** comparando todas as dimensões clínicas que ele apresenta. "
                f"Tons quentes (vermelho) revelam quais '{_label_diag}' são anomalias que fogem do padrão normal daquele ator específico."
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
            if not df_heatmap.empty and "_diag" in df_heatmap.columns and "_ator" in df_heatmap.columns:
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
                    df_math = df_math.sub(medias_linhas, axis=0).div(desvios_linhas, axis=0)
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
                    title=f"Matriz de Desvios (Z-Score): Top {top_x_cid} {_label_diag} × Top {top_x_med} {_label_ator}",
                    labels=dict(
                        x=_label_ator, y=_label_diag, color="Z-Score"
                    ),
                )

                fig_heat.update_traces(
                    text=df_text.values,
                    texttemplate="%{text}",
                    customdata=df_pivot_vol.values,
                    hovertemplate=f"<b>{_label_ator}:</b> %{{x}}<br><b>{_label_diag}:</b> %{{y}}<br><b>Volume Real:</b> %{{customdata}} pacientes<br><b>Z-Score:</b> %{{text}} desvios<extra></extra>",
                )

                altura_dinamica = max(500, top_x_cid * 35)
                fig_heat.update_layout(
                    xaxis_tickangle=-45, height=altura_dinamica, margin=dict(l=250, b=120)
                )
                st.plotly_chart(
                    fig_heat, width="stretch", config={"displayModeBar": False}
                )
        except Exception:
            st.warning("⚠️ Dados insuficientes para gerar o heatmap de auditoria clínica.")

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
            if not df_perfil_med.empty and "_ator" in df_perfil_med.columns and "_diag" in df_perfil_med.columns:
                df_perfil_med["_ator"] = df_perfil_med["_ator"].replace("", f"{_label_ator} Não Informado")
                df_perfil_med["_diag"] = df_perfil_med["_diag"].replace("", f"{_label_diag} Não Informado")
                fig_tree_med = px.treemap(
                    df_perfil_med,
                    path=["_ator", "_diag"],
                    values="Vol",
                    color="Vol",
                    color_continuous_scale="Teal",
                    title=f"Perfil: {_label_ator} ➔ {_label_diag} (Clique para expandir)",
                )
                fig_tree_med.update_layout(height=500, margin=dict(t=40, l=10, r=10, b=10))
                st.plotly_chart(
                    fig_tree_med, width="stretch", config={"displayModeBar": False}
                )
        except Exception:
            st.warning("⚠️ Dados insuficientes para gerar o treemap de perfil clínico.")

    with t_micro:
        st.subheader("Auditoria de Outliers & Top Ofensores (SRE)")

        c1, c2 = st.columns([0.7, 0.3])
        with c1:
            # Matriz de Outliers (Scatter Plot)
            st.markdown("### 🔍 Detecção de Outliers SLA")
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
                if not df_outliers.empty and "DiasFila" in df_outliers.columns and "Pontos" in df_outliers.columns:
                    # 2. Prevenção de Nós Vazios
                    df_outliers["entidade_classificacaoRisco_cor"] = (
                        df_outliers["entidade_classificacaoRisco_cor"]
                        .replace("", "Não Informado")
                        .fillna("Não Informado")
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
                        title="Deteção de Outliers: Tempo de Fila vs Gravidade",
                        labels={
                            "DiasFila": "Tempo de Espera (Dias)",
                            "Pontos": "Pontos de Gravidade",
                        },
                        render_mode="svg",
                    )
                    fig_out.add_hline(
                        y=40, line_dash="dot", annotation_text="Alta Gravidade"
                    )
                    fig_out.add_vline(x=180, line_dash="dot", annotation_text="SLA 180 d")
                    st.plotly_chart(
                        fig_out, width="stretch", config={"displayModeBar": False}
                    )
            except Exception:
                st.warning("⚠️ Dados insuficientes para o scatter de outliers.")

        with c2:
            # Top Ofensores (Barra Horizontal)
            st.markdown("### ⚖️ Top Ofensores")
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
                        title="Top 10 Médicos (Volume)",
                    )
                    fig_ofensor.update_layout(
                        yaxis={"categoryorder": "total ascending"}, height=450
                    )
                    st.plotly_chart(
                        fig_ofensor, width="stretch", config={"displayModeBar": False}
                    )
            except Exception:
                st.warning("⚠️ Dados insuficientes para o ranking de médicos.")

        # Log Clinical Audit
        st.markdown("---")
        st.markdown("### 📝 Log de Evoluções Clínicas")

        c_slider, c_export = st.columns([0.8, 0.2])
        with c_slider:
            limit = st.slider("Amostra para Auditoria Clínica", 10, 1000, 100)

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
                label="📥 Baixar CSV",
                data=csv_data,
                file_name=f"auditoria_gercon_{date.today()}.csv",
                mime="text/csv",
                width="stretch",
            )

        st.dataframe(df_audit, width="stretch", hide_index=True)


if __name__ == "__main__":
    main()
