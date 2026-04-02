import os
import streamlit as st
from src.domain.models import FilterCriteria
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import date, timedelta

# --- 1. CONFIGURAÇÃO DA PÁGINA E DX ---
st.set_page_config(page_title="Gercon Analytics | RCA", page_icon="🎯", layout="wide", initial_sidebar_state="expanded")

from src.infrastructure.config import settings

def inject_custom_css():
    st.markdown("""
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
    """, unsafe_allow_html=True)

# --- 2. INFRASTRUCTURE: USE CASE & DI ---
@st.cache_resource
def get_use_case():
    from src.infrastructure.repositories.duckdb_repository import DuckDBAnalyticsRepository
    from src.application.use_cases.analytics_use_case import AnalyticsUseCase
    
    repo = DuckDBAnalyticsRepository(settings.OUTPUT_FILE)
    return AnalyticsUseCase(repo)

def get_dynamic_options(column: str, current_where: str, current_user) -> list:
    return get_use_case().get_dynamic_options(column, current_where, current_user)

@st.cache_data(ttl=3600)
def get_global_bounds(column: str, is_date=False):
    return get_use_case().get_global_bounds(column, is_date)

# --- 3. STATE MANAGEMENT ---
def clear_filter_state(keys_to_clear: list):
    for key in keys_to_clear:
        if key in st.session_state:
            if key.endswith("_in") or key.endswith("_ex"):
                st.session_state[key] = []
            elif key.endswith("_val"): 
                st.session_state[key] = ""
            elif key.endswith("_toggle"): 
                st.session_state[key] = False
            elif key == "num_min":
                st.session_state[key] = 0
            elif key == "num_max":
                st.session_state[key] = 99999
            elif key == "oj_radio":
                st.session_state[key] = "Ambos"
            else:
                try:
                    del st.session_state[key]
                except:
                    pass

# --- 4. UI COMPONENTS (DOMAIN FILTERS & TRACKING) ---
def render_include_exclude(label: str, column: str, clauses: list, current_where: str, key: str, ui_tracker: list, cat_keys: list, current_user):
    cat_keys.extend([f"{key}_in", f"{key}_ex"])
    options = get_dynamic_options(column, current_where, current_user)
    if not options: return current_where
    
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    incl = c1.multiselect("✅ Incluir", options, key=f"{key}_in", label_visibility="collapsed", placeholder="✅ Incluir...")
    excl = c2.multiselect("❌ Excluir", options, key=f"{key}_ex", label_visibility="collapsed", placeholder="❌ Excluir...")
    
    def sanitize(v): return str(v).replace("'", "''")
    if incl: 
        # ARQUITETURA DE ESTADO: Agora guardamos o Texto Visual e as Chaves Associadas
        ui_tracker.append({"text": f"✅ {label}: {', '.join([str(v) for v in incl])}", "keys": [f"{key}_in"]})
        sanitized_incl = [f"'{sanitize(v)}'" for v in incl]
        clauses.append(f'"{column}" IN ({", ".join(sanitized_incl)})')
        
    if excl: 
        # ARQUITETURA DE ESTADO: Agora guardamos o Texto Visual e as Chaves Associadas
        ui_tracker.append({"text": f"❌ {label}: {', '.join([str(v) for v in excl])}", "keys": [f"{key}_ex"]})
        sanitized_excl = [f"'{sanitize(v)}'" for v in excl]
        clauses.append(f'"{column}" NOT IN ({", ".join(sanitized_excl)})')
    
    return " AND ".join(clauses)

def render_boolean_radio(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list):
    """Componente SRE para campos booleanos (True/False/Null)"""
    cat_keys.append(f"{key}_radio")
    
    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Ambos"
        
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    val = st.radio(label, ["Ambos", "Sim", "Não"], horizontal=True, key=f"{key}_radio", label_visibility="collapsed")
    
    if val == "Sim":
        ui_tracker.append({"text": f"{label}: Sim", "keys": [f"{key}_radio"]})
        clauses.append(f"\"{column}\" = true")
    elif val == "Não":
        ui_tracker.append({"text": f"{label}: Não", "keys": [f"{key}_radio"]})
        # Tratamento seguro para Falsos ou Nulos
        clauses.append(f"(\"{column}\" = false OR \"{column}\" IS NULL)")
        
    return " AND ".join(clauses)

def render_presence_radio(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list):
    """Componente SRE para campos de texto/ID onde a presença de valor valida a flag verdadeira (Ex: Liminar)."""
    cat_keys.append(f"{key}_radio")
    
    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Ambos"
        
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    val = st.radio(label, ["Ambos", "Sim", "Não"], horizontal=True, key=f"{key}_radio", label_visibility="collapsed")
    
    if val == "Sim":
        ui_tracker.append({"text": f"{label}: Sim", "keys": [f"{key}_radio"]})
        clauses.append(f"(\"{column}\" IS NOT NULL AND \"{column}\" != '')")
    elif val == "Não":
        ui_tracker.append({"text": f"{label}: Não", "keys": [f"{key}_radio"]})
        clauses.append(f"(\"{column}\" IS NULL OR \"{column}\" = '')")
        
    return " AND ".join(clauses)

def render_dual_slider(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list):
    """SRE UX FIX: Slider bidirecional sincronizado com inputs numéricos para precisão cirúrgica."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    vmin, vmax = get_global_bounds(column, is_date=False)
    
    # SRE FIX: Usando pd.notna() para proteger contra valores ausentes (<NA>) do banco
    if pd.notna(vmin) and pd.notna(vmax) and vmin != vmax:
        vmin_val, vmax_val = int(vmin), int(vmax)
        
        # Inicializa o estado com os limites do banco se não existir
        if f"{key}_min" not in st.session_state: st.session_state[f"{key}_min"] = vmin_val
        if f"{key}_max" not in st.session_state: st.session_state[f"{key}_max"] = vmax_val
        if f"{key}_sld" not in st.session_state: st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)
        
        st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
        
        # Callbacks de Sincronização de Estado (Evita loops infinitos)
        def sync_slider():
            st.session_state[f"{key}_min"] = st.session_state[f"{key}_sld"][0]
            st.session_state[f"{key}_max"] = st.session_state[f"{key}_sld"][1]
            
        def sync_num():
            # Proteção contra inversão de valores (min > max)
            safe_min = min(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
            safe_max = max(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
            st.session_state[f"{key}_sld"] = (safe_min, safe_max)
        
        c1, c2 = st.columns(2)
        c1.number_input("Mínimo", min_value=vmin_val, max_value=vmax_val, key=f"{key}_min", on_change=sync_num, label_visibility="collapsed")
        c2.number_input("Máximo", min_value=vmin_val, max_value=vmax_val, key=f"{key}_max", on_change=sync_num, label_visibility="collapsed")
        
        val = st.slider(label, vmin_val, vmax_val, key=f"{key}_sld", on_change=sync_slider, label_visibility="collapsed")
        
        if val[0] > vmin_val or val[1] < vmax_val:
            ui_tracker.append({"text": f"{label}: {val[0]} a {val[1]}", "keys": [f"{key}_sld", f"{key}_min", f"{key}_max"]})
            clauses.append(f"TRY_CAST(\"{column}\" AS INTEGER) BETWEEN {val[0]} AND {val[1]}")
            
    return " AND ".join(clauses)

def render_age_slider(label: str, clauses: list, key: str, ui_tracker: list, cat_keys: list):
    """Componente de Domínio para Idade: Converte Faixa Etária visível para DATEDIFF no SQL OLAP."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    vmin_val, vmax_val = settings.AGE_MIN, settings.AGE_MAX
    
    if f"{key}_min" not in st.session_state: st.session_state[f"{key}_min"] = vmin_val
    if f"{key}_max" not in st.session_state: st.session_state[f"{key}_max"] = vmax_val
    if f"{key}_sld" not in st.session_state: st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)
    
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    
    def sync_slider_age():
        st.session_state[f"{key}_min"] = st.session_state[f"{key}_sld"][0]
        st.session_state[f"{key}_max"] = st.session_state[f"{key}_sld"][1]
    def sync_num_age():
        safe_min = min(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
        safe_max = max(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
        st.session_state[f"{key}_sld"] = (safe_min, safe_max)
    
    c1, c2 = st.columns(2)
    c1.number_input("Idade Min", min_value=vmin_val, max_value=vmax_val, key=f"{key}_min", on_change=sync_num_age, label_visibility="collapsed")
    c2.number_input("Idade Max", min_value=vmin_val, max_value=vmax_val, key=f"{key}_max", on_change=sync_num_age, label_visibility="collapsed")
    
    val = st.slider(label, vmin_val, vmax_val, key=f"{key}_sld", on_change=sync_slider_age, label_visibility="collapsed")
    
    if val[0] > vmin_val or val[1] < vmax_val:
        ui_tracker.append({"text": f"{label}: {val[0]} a {val[1]} anos", "keys": [f"{key}_sld", f"{key}_min", f"{key}_max"]})
        clauses.append(f"date_diff('year', TRY_CAST(usuarioSUS_dataNascimento AS DATE), CURRENT_DATE) BETWEEN {val[0]} AND {val[1]}")
    return " AND ".join(clauses)

def render_smart_date_range(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list, default_to_30_days: bool = False):
    """SRE UX FIX: Usa exclusivamente o seletor nativo do Streamlit, que já traz Range e Presets embutidos."""
    cat_keys.append(key)
    
    # Inicializa estado dinâmico (Otimização Cold Start vs UX Cross-Sectional)
    if key not in st.session_state:
        if default_to_30_days:
            hoje = date.today()
            st.session_state[key] = (hoje - timedelta(days=30), hoje)
        else:
            st.session_state[key] = ()
        
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    
    # Renderiza o input diretamente na sidebar. Sem popovers, sem botões extras.
    val = st.date_input(label, key=key, label_visibility="collapsed")
    
    # Construtor do OLAP
    if isinstance(val, tuple) and len(val) == 2:
        ui_tracker.append({"text": f"{label}: {val[0].strftime('%d/%m/%Y')} a {val[1].strftime('%d/%m/%Y')}", "keys": [key]})
        clauses.append(f"CAST(\"{column}\" AS DATE) BETWEEN '{val[0]}' AND '{val[1]}'")
        
    return " AND ".join(clauses)

def render_advanced_text_search(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list, aggregate_by: str = None, default_toggle: bool = False):
    """
    Renderiza um Toggle com lógica Booleana, tolerância a Acentos e suporte a Wildcards (*).
    Se aggregate_by for passado, utiliza 'bool_or' (Single-pass OLAP).
    Adicionado 'default_toggle' para permitir Busca Profunda já aberta (Ex: Evoluções).
    """
    cat_keys.extend([f"{key}_toggle", f"{key}_and_val", f"{key}_or_val", f"{key}_not_val"])
    
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
                st.markdown(f"<div class='aggregate-search-bar'>Busca Global: Procura em <b>todo o histórico clínico</b>.</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='deep-search-bar'>Busca no Evento.</div>", unsafe_allow_html=True)
            
            st.caption("Separe por vírgula ( , ). Use **\*** como curinga (ex: *cardio\**). Acentos são ignorados.")
            
            or_terms = st.text_input("✅ Contém QUALQUER UMA (OR)", value=st.session_state[f"{key}_or_val"], key=f"{key}_or")
            and_terms = st.text_input("⚠️ Contém TODAS (AND)", value=st.session_state[f"{key}_and_val"], key=f"{key}_and")
            not_terms = st.text_input("❌ NÃO contém (NOT)", value=st.session_state[f"{key}_not_val"], key=f"{key}_not")
            
            st.session_state[f"{key}_or_val"] = or_terms
            st.session_state[f"{key}_and_val"] = and_terms
            st.session_state[f"{key}_not_val"] = not_terms
            
            # --- LEXICAL PARSER (SRE Refined: Trata Injeção, Curingas e Acentos) ---
            import re
            def parse_term(term: str) -> str:
                # SRE FIX: Sanitização contra caracteres de controle SQL perigosos
                t = re.sub(r'[;]|--', '', term.strip())
                t = t.replace("'", "''") # Escapa aspas
                t = t.replace("*", "%")  # Traduz UX Curinga para SQL
                
                if not t.startswith("%"):
                    t = f"%{t}"
                if not t.endswith("%"):
                    t = f"{t}%"
                    
                return t

            # --- CONSTRUTOR DE SQL SOTA (Com strip_accents) ---
            if and_terms or or_terms or not_terms:
                
                # ESTRATÉGIA OLAP: AGRUPAMENTO POR ENTIDADE (PACIENTE)
                if aggregate_by:
                    having_conds = []
                    if or_terms:
                        ui_tracker.append({"text": f"✅ {label}: {or_terms}", "keys": [f"{key}_or_val", f"{key}_or"]})
                        words = [w for w in or_terms.split(',') if w.strip()]
                        if words:
                            or_expr = [f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}'))" for w in words]
                            having_conds.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append({"text": f"⚠️ AND {label}: {and_terms}", "keys": [f"{key}_and_val", f"{key}_and"]})
                        for w in [w for w in and_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}'))")
                            
                    if not_terms:
                        ui_tracker.append({"text": f"❌ {label}: {not_terms}", "keys": [f"{key}_not_val", f"{key}_not"]})
                        for w in [w for w in not_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')) = FALSE")
                            
                    if having_conds:
                        subquery = f"SELECT \"{aggregate_by}\" FROM gercon GROUP BY \"{aggregate_by}\" HAVING {' AND '.join(having_conds)}"
                        clauses.append(f"\"{aggregate_by}\" IN ({subquery})")
                        
                # ESTRATÉGIA NORMAL: FILTRO POR EVENTO/LINHA
                else:
                    if or_terms:
                        ui_tracker.append({"text": f"✅ {label}: {or_terms}", "keys": [f"{key}_or_val", f"{key}_or"]})
                        words = [w for w in or_terms.split(',') if w.strip()]
                        if words:
                            or_expr = [f"strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}')" for w in words]
                            clauses.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append({"text": f"⚠️ AND {label}: {and_terms}", "keys": [f"{key}_and_val", f"{key}_and"]})
                        for w in [w for w in and_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            clauses.append(f"strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')")
                            
                    if not_terms:
                        ui_tracker.append({"text": f"❌ {label}: {not_terms}", "keys": [f"{key}_not_val", f"{key}_not"]})
                        for w in [w for w in not_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            clauses.append(f"strip_accents(\"{column}\") NOT ILIKE strip_accents('{p_term}')")

# --- 4.5 BFF: IDENTITY AWARE PROXY (IAP) & BFF MOCK ---
def get_authenticated_user():
    """
    SRE BFF Pattern: Extrai o JWT injetado pelo IAP Proxy ou Mock local.
    """
    # DX: Desenvolvimento Local sem IAP Proxy
    if os.getenv("ENVIRONMENT") == "dev":
        from src.infrastructure.auth.token_acl import ValidatedUserToken
        import time
        # Mock Session para Desenvolvimento Local (Bypass RLS se role for diretor_medico)
        mock_user = ValidatedUserToken(
            sub="dev-id-123",
            email="dev@gercon.com",
            preferred_username="dev_user",
            roles=["diretor_medico"], 
            crm_numero="99999",
            crm_uf="RS",
            exp=int(time.time() + 3600) # 1h exp
        )
        return mock_user, "mock-jwt-token"

    # Prod Flow: Extração do Header injetado pelo OAuth2-Proxy (Streamlit 1.37+)
    auth_header = st.context.headers.get("X-Forwarded-Access-Token")
    if not auth_header:
        st.error("🚨 Acesso Bloqueado: IAP Proxy não detectado. Contate o administrador do Cluster (Zero Trust).")
        st.stop()

    from src.infrastructure.auth.jwt_validator import verify_token
    user = verify_token(auth_header)
    return user, auth_header

# --- 5. MAIN APP ---
def main():
    # Cache Bust Temporário para instanciar as novas bibliotecas e códigos importados:
    st.cache_resource.clear()
    st.cache_data.clear()
    
    # SRE Loop: Mitigação de WebSocket Staleness via Re-Handshake (com 60s de Leeway)
    import time
    import streamlit.components.v1 as components
    
    if "token_exp" in st.session_state and time.time() > (st.session_state.token_exp - 60):
        st.warning("🔄 Sua sessão segura foi renovada no background. Clique abaixo para sincronizar o túnel Proxy.")
        # TODO(UX/SRE): Sincronizar FilterCriteria com st.query_params antes do reload
        if st.button("Sincronizar Sessão", type="primary"):
            components.html("<script>window.parent.location.reload();</script>", height=0, width=0)
        st.stop() # Mata a execução do Python imediatamente. Previne vazamento de dados via token vencido.
        
    if "user" not in st.session_state:
        try:
            user_domain, jwt_str = get_authenticated_user()
            st.session_state.user = user_domain
            st.session_state.raw_jwt = jwt_str
            st.session_state.token_exp = user_domain.exp if user_domain.exp else (time.time() + 86400)
        except Exception as e:
            st.warning("⏱️ A sua sessão expirou devido a um longo período de inatividade.")
            st.info("Para proteger os seus dados, a ligação ao servidor foi encerrada.")
            
            # Rota de Fuga SRE: Força o OAuth2-Proxy a destruir a sessão e redireciona para o dashboard
            st.link_button(
                "Renovar Sessão / Fazer Login", 
                "/oauth2/sign_out?rd=/dashboard/",
                type="primary"
            )
            st.stop() # Interrompe a renderização do resto do painel

    inject_custom_css()
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f"⚠️ Base Parquet não encontrada ({settings.OUTPUT_FILE}).")
        return

    st.title("🎯 Gercon SRE | Advanced Root Cause Analysis")
    
    # ==========================================
    # SRE FIX: DICIONÁRIO MESTRE DE CORES (GLOBAL)
    # ==========================================
    MAPA_CORES_RISCO = {
        'VERMELHO': '#ef4444', 
        'LARANJA': '#f97316', 
        'AMARELO': '#eab308', 
        'VERDE': '#22c55e', 
        'AZUL': '#3b82f6',
        'BRANCO': '#e5e7eb',
        'Não Informado': '#9ca3af'
    }
    
    clauses = ["1=1"]
    curr_where = "1=1"

    # ==========================================
    # SRE FIX: DICIONÁRIO MESTRE (MANTÉM CONSISTÊNCIA DE ORDEM UI/UX)
    # ==========================================
    ui_filters = {
        "🩺 Clínico & Regulação": [], 
        "🏛️ Governança & Atores": [], 
        "📅 Ciclo de Vida (Datas)": [], 
        "🌍 Demografia & Rede": [],
        "⚠️ Triagem & entidade_classificacaoRisco_totalPontos": [],
        "🎯 Desfechos, Gargalos & SLA": []
    }
    state_keys = {k: [] for k in ui_filters.keys()}

    # ==========================================
    # CASCADING SIDEBAR (TOP-DOWN FLOW OTIMIZADO)
    # ==========================================
    st.sidebar.header("🎛️ Filtros em Cascata")

    cat = "🩺 Clínico & Regulação"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Especialidade Mãe", "entidade_especialidade_especialidadeMae_descricao", clauses, curr_where, "espm", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Especialidade Fina", "entidade_especialidade_descricao", clauses, curr_where, "espf", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("CBO Especialidade", "entidade_especialidade_cbo_descricao", clauses, curr_where, "esp_cbo", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Descrição Auxiliar", "entidade_especialidade_descricaoAuxiliar", clauses, curr_where, "esp_aux", ui_filters[cat], state_keys[cat], st.session_state.user)
        st.markdown("---")
        curr_where = render_include_exclude("Médico Solicitante", "medicoSolicitante", clauses, curr_where, "med_sol", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Unidade Operadora", "entidade_unidadeOperador_nome", clauses, curr_where, "usol", ui_filters[cat], state_keys[cat], st.session_state.user)
        st.markdown("---")
        curr_where = render_include_exclude("CID Principal (Código)", "entidade_cidPrincipal_codigo", clauses, curr_where, "cid_cod", ui_filters[cat], state_keys[cat], st.session_state.user)
        render_advanced_text_search("CID Principal (Descrição)", "entidade_cidPrincipal_descricao", clauses, "txt_cid_desc", ui_filters[cat], state_keys[cat])
        # MÁGICA CLÍNICA MOVIDA: Agregação pelo numeroCMCE inteiro
        st.markdown("---")
        render_advanced_text_search("Evoluções do Paciente", "historico_quadro_clinico", clauses, "txt_evo", ui_filters[cat], state_keys[cat], aggregate_by="numeroCMCE")
        curr_where = " AND ".join(clauses)

    cat = "🏛️ Governança & Atores"
    with st.sidebar.expander(cat, expanded=False):
        # Atores movidos da antiga aba de Evoluções
        render_advanced_text_search("Tipo de Informação", "historico_evolucoes_completo", clauses, "txt_tinf", ui_filters[cat], state_keys[cat])
        render_advanced_text_search("Origem da Informação", "evolucoes_json", clauses, "txt_orig_inf", ui_filters[cat], state_keys[cat])
        st.markdown("---")
        
        curr_where = render_include_exclude("Origem (Lista)", "origem_lista", clauses, curr_where, "lst", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Situação Atual", "situacao", clauses, curr_where, "sit", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Tipo de Regulação", "entidade_especialidade_tipoRegulacao", clauses, curr_where, "treg", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Especialidade Ativa", "entidade_especialidade_ativa", clauses, curr_where, "stesp", ui_filters[cat], state_keys[cat], st.session_state.user)
        
        st.markdown("---")
        curr_where = render_presence_radio("Liminar / Ordem Judicial", "liminarOrdemJudicial", clauses, "oj", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        curr_where = render_include_exclude("Operador", "operador_nome", clauses, curr_where, "op_nome", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Usuário Solicitante", "usuarioSolicitante_nome", clauses, curr_where, "usu_sol_nome", ui_filters[cat], state_keys[cat], st.session_state.user)
        
        st.markdown("---")
        curr_where = render_include_exclude("Central de Regulação", "entidade_centralRegulacao_nome", clauses, curr_where, "cent_reg", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Unidade Op. Central Regulação", "entidade_unidadeOperador_centralRegulacao_nome", clauses, curr_where, "uni_op_cent", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Unidade de Referência", "entidade_unidadeReferencia_nome", clauses, curr_where, "uni_ref", ui_filters[cat], state_keys[cat], st.session_state.user)
        
        st.markdown("---")
        curr_where = render_boolean_radio("Possui DITA", "entidade_possuiDita", clauses, "dita", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("Fora da Regionalização", "entidade_foraDaRegionalizacao", clauses, "freg", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("Regularização de Acesso", "regularizacaoAcesso", clauses, "reg_acc", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("Aceita Teleconsulta", "entidade_especialidade_teleconsulta", clauses, "tele", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("Matriciamento", "entidade_especialidade_matriciamento", clauses, "matri", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("Sem Classificação", "entidade_semClassificacao", clauses, "sem_class", ui_filters[cat], state_keys[cat])

        curr_where = " AND ".join(clauses)



    cat = "📅 Ciclo de Vida (Datas)"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_smart_date_range("Data de Solicitação", "dataSolicitacao", clauses, "dt_solic", ui_filters[cat], state_keys[cat], default_to_30_days=True)
        st.write(" ")
        curr_where = render_smart_date_range("Data de Cadastro", "dataCadastro", clauses, "dt_cad", ui_filters[cat], state_keys[cat])
        st.write(" ")
        curr_where = render_smart_date_range("Data da Evolução", "dataCadastro", clauses, "dt_evo", ui_filters[cat], state_keys[cat])
        st.write(" ")
        curr_where = render_smart_date_range("Primeiro Agendamento", "dataPrimeiroAgendamento", clauses, "dt_pagend", ui_filters[cat], state_keys[cat])
        st.write(" ")
        curr_where = render_smart_date_range("Primeira Autorização", "dataPrimeiraAutorizacao", clauses, "dt_paut", ui_filters[cat], state_keys[cat])

    cat = "🌍 Demografia & Rede"
    with st.sidebar.expander(cat, expanded=False):
        render_advanced_text_search("Pesquisa: Nome do Paciente", "usuarioSUS_nomeCompleto", clauses, "txt_pac_nome", ui_filters[cat], state_keys[cat])
        curr_where = " AND ".join(clauses)
        st.markdown("---")
        
        curr_where = render_include_exclude("Município de Residência", "usuarioSUS_municipioResidencia_nome", clauses, curr_where, "mun", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Bairro", "usuarioSUS_bairro", clauses, curr_where, "bai", ui_filters[cat], state_keys[cat], st.session_state.user)
        
        # Logradouro com a condicional injetando a numeração dento da Deep Search
        render_advanced_text_search("Logradouro", "usuarioSUS_logradouro", clauses, "txt_logr", ui_filters[cat], state_keys[cat])
        if st.session_state.get("txt_logr_toggle", False):
            st.markdown("<div style='margin-left: 1rem; border-left: 2px solid #cbd5e1; padding-left: 0.5rem;'>", unsafe_allow_html=True)
            state_keys[cat].extend(["num_min", "num_max"])
            num_min, num_max = st.columns(2)
            v_nmin = num_min.number_input("Nº Min", value=0, step=10, key="num_min")
            v_nmax = num_max.number_input("Nº Max", value=99999, step=100, key="num_max")
            if v_nmin > 0 or v_nmax < 99999: 
                ui_filters[cat].append({"text": f"Nº Logradouro: {v_nmin} a {v_nmax}", "keys": ["num_min", "num_max"]})
                clauses.append(f"TRY_CAST(\"Número\" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}")
            st.markdown("</div>", unsafe_allow_html=True)
        
        st.divider() # --- Separador Visual de Identificação Pessoal ---
        
        curr_where = " AND ".join(clauses)
        curr_where = render_include_exclude("Sexo", "usuarioSUS_sexo", clauses, curr_where, "sex", ui_filters[cat], state_keys[cat], st.session_state.user)
        
        # Componente que injeta idade (com Slider Duplo)
        curr_where = render_age_slider("Faixa Etária (Idade)", clauses, "f_idade", ui_filters[cat], state_keys[cat])
        
        curr_where = render_include_exclude("Cor/Raça", "usuarioSUS_racaCor", clauses, curr_where, "cor", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Nacionalidade", "usuarioSUS_nacionalidade", clauses, curr_where, "nac", ui_filters[cat], state_keys[cat], st.session_state.user)

    cat = "⚠️ Triagem & entidade_classificacaoRisco_totalPontos"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Complexidade", "entidade_complexidade", clauses, curr_where, "cpx", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Risco Cor (Atual)", "entidade_classificacaoRisco_cor", clauses, curr_where, "r_cor", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Cor do Regulador", "corRegulador", clauses, curr_where, "c_reg", ui_filters[cat], state_keys[cat], st.session_state.user)
        
        st.markdown("---")
        curr_where = render_boolean_radio("Reclassificada pelo Solicitante", "entidade_classificacaoRisco_reclassificadaSolicitante", clauses, "r_recl", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        curr_where = render_dual_slider("Pontos Gravidade", "entidade_classificacaoRisco_pontosGravidade", clauses, "pt_grav", ui_filters[cat], state_keys[cat])
        curr_where = render_dual_slider("Pontos Tempo", "entidade_classificacaoRisco_pontosTempo", clauses, "pt_tmp", ui_filters[cat], state_keys[cat])
        curr_where = render_dual_slider("entidade_classificacaoRisco_totalPontos Total", "entidade_classificacaoRisco_totalPontos", clauses, "pt_tot", ui_filters[cat], state_keys[cat])

    cat = "🎯 Desfechos, Gargalos & SLA"
    with st.sidebar.expander(cat, expanded=False):
        # 1. Funil e Motivos (Include/Exclude)
        curr_where = render_include_exclude("Tipo de Desfecho", "SLA_Tipo_Desfecho", clauses, curr_where, "sla_tipo", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Status Provisório", "statusProvisorio", clauses, curr_where, "st_prov", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Motivo Pendência", "motivoPendencia", clauses, curr_where, "mot_pend", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Motivo Cancelamento", "motivoCancelamento", clauses, curr_where, "mot_canc", ui_filters[cat], state_keys[cat], st.session_state.user)
        curr_where = render_include_exclude("Motivo Encerramento", "motivoEncerramento", clauses, curr_where, "mot_enc", ui_filters[cat], state_keys[cat], st.session_state.user)
        
        st.markdown("---")
        # 2. Textos de Justificativa (Deep Search)
        render_advanced_text_search("Justificativa de Retorno", "justificativaRetorno", clauses, "txt_retorno", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        # 3. Marcos de Sucesso (Booleans)
        curr_where = render_boolean_radio("1. Passou por Autorização?", "SLA_Marco_Autorizada", clauses, "m_aut", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("2. Chegou a Agendar?", "SLA_Marco_Agendada", clauses, "m_agd", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("3. Foi Realizada?", "SLA_Marco_Realizada", clauses, "m_rea", ui_filters[cat], state_keys[cat])
        curr_where = render_boolean_radio("Fila Finalizada? (Timer Parado)", "SLA_Desfecho_Atingido", clauses, "m_fim", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        # 4. Sliders de SLA (Métricas calculadas em dias e interações)
        curr_where = render_dual_slider("Lead Time Total (Dias)", "SLA_Lead_Time_Total_Dias", clauses, "sla_tot", ui_filters[cat], state_keys[cat])
        curr_where = render_dual_slider("Tempo com Regulador (Dias)", "SLA_Tempo_Regulador_Dias", clauses, "sla_reg", ui_filters[cat], state_keys[cat])
        curr_where = render_dual_slider("Tempo com Solicitante (Dias)", "SLA_Tempo_Solicitante_Dias", clauses, "sla_sol", ui_filters[cat], state_keys[cat])
        curr_where = render_dual_slider("Volume de Interações (Ping-Pong)", "SLA_Interacoes_Regulacao", clauses, "sla_int", ui_filters[cat], state_keys[cat])

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
                    st.markdown(f"<div class='cat-title'>{category}</div>", unsafe_allow_html=True)
                    
                    # 2. FILTROS AGRUPADOS NA LINHA SEGUINTE
                    with st.container():
                        st.markdown("<div class='filter-row-marker' style='display:none;'></div>", unsafe_allow_html=True)
                        for i, f in enumerate(filters):
                            st.button(f"{f['text']}", key=f"clr_item_{category}_{i}", on_click=clear_filter_state, args=(f['keys'],))
            
            # 3. LIMPAR TODOS ISOLADO NO FINAL (Sem a linha tracejada)
            st.write("") # Micro-espaçamento natural
            all_keys = [key for sublist in state_keys.values() for key in sublist]
            st.button("🗑️ Limpar Todos os Filtros", key="btn_clear_all", on_click=clear_filter_state, args=(all_keys,))
        
        st.write(" ") # Um micro-espaçamento logo antes dos KPIs para respirar

    # ==========================================
    # DASHBOARD TABS: ESTRUTURADO POR NÍVEL DE DECISÃO
    # ==========================================
    # SRE FIX: Adicionada a aba de KPIs como a primeira (t_kpi) para Executive Summary
    t_kpi, t_macro, t_clin, t_micro = st.tabs([
        "📊 Visão Geral (KPIs)", 
        "📈 Estratégia (Macro)", 
        "🩺 Inteligência Clínica", 
        "🔎 Auditoria (Micro)"
    ])

    # ==========================================
    # CLÁUSULA FINAL E PROCESSAMENTO (KPIs)
    # ==========================================
    FINAL_WHERE = " AND ".join(clauses)
    filters = FilterCriteria(clauses=clauses)
    use_case = get_use_case()

    with st.spinner("Processando Modelo de Leitura (OLAP) e Latência de Cauda (P90)..."):
        kpi_data = use_case.get_executive_summary(filters, st.session_state.user)

    # --- Extração Segura das Variáveis Absolutas ---
    pacientes = kpi_data.pacientes
    eventos = kpi_data.eventos
    esp_mae = kpi_data.esp_mae
    sub_esp = kpi_data.sub_esp
    medicos = kpi_data.medicos
    cids = kpi_data.cids
    origens = kpi_data.origens
    lead_time = kpi_data.lead_time
    max_lead_time = kpi_data.max_lead_time
    span_dias = kpi_data.span_dias
    pac_urgentes = kpi_data.pac_urgentes
    pac_vencidos = kpi_data.pac_vencidos
    
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
        st.markdown("<h4 style='font-size: 1rem; color: #4B5563;'>Painel de Desempenho (SLA e Carga)</h4>", unsafe_allow_html=True)
        
        # --- LINHA 1: Volume, Carga e Esforço ---
        r1_c1, r1_c2, r1_c3, r1_c4 = st.columns(4)
        r1_c1.metric("🏢 Origens do Gercon", f"{origens:,}".replace(',', '.'), help="Quantidade de portas de entrada/sistemas de origem distintos.")
        r1_c2.metric("👥 Pacientes", f"{pacientes:,}".replace(',', '.'), help="Número total de pacientes únicos selecionados.")
        r1_c3.metric("📋 Evoluções", f"{eventos:,}".replace(',', '.'), help="Número total de eventos no histórico clínico.")
        r1_c4.metric("📈 Evoluções/Paciente", f"{evo_por_paciente}".replace('.', ','), help="Média de vezes que o paciente foi movimentado ou avaliado.")
        
        st.write(" ") 
        
        # --- LINHA 2: entidade_complexidade Clínica e SLA ---
        r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
        r2_c1.metric("🏛️ entidade_especialidade_descricaos (Mãe)", f"{esp_mae:,}".replace(',', '.'), help="Grandes áreas clínicas abrangidas (Ex: CIRURGIA).")
        r2_c2.metric("🎯 Subespecialidades", f"{sub_esp:,}".replace(',', '.'), help="entidade_especialidade_descricaos finas abrangidas (Ex: CIRURGIA DA MÃO).")
        r2_c3.metric("🔀 Subs/entidade_especialidade_descricao", f"{sub_por_esp}".replace('.', ','), help="Média de ramificações por grande área clínica.")
        
        lead_str = f"{lead_time} dias | {max_lead_time} dias" if pd.notna(lead_time) else "0 dias"
        r2_c4.metric("⏱️ Fila: Média | Pior", lead_str, help="Tempo Médio vs Tempo do paciente mais antigo.")

        st.write(" ") 

        # --- LINHA 3: Governança e Comportamento Médico ---
        r3_c1, r3_c2, r3_c3, r3_c4 = st.columns(4)
        r3_c1.metric("👨⚕️ Médicos Solicitantes", f"{medicos:,}".replace(',', '.'), help="Total de médicos distintos que inseriram pacientes nesta fila.")
        r3_c2.metric("📅 Cadastros/Mês", f"{cad_por_mes}".replace('.', ','), help="Média mensal histórica de novos pacientes inseridos na fila (baseado na janela filtrada).")
        r3_c3.metric("🧠 Dispersão Diagnóstica", f"{cid_por_medico}".replace('.', ','), help="Média de CIDs distintos usados por médico.")
        r3_c4.metric("⚙️ Carga/Médico", f"{evo_por_medico}".replace('.', ','), help="Volume médio de evoluções administrativas geradas por cada médico.")
        
        st.divider()

        # --- BLOCO 2 CONSOLIDADO: ANATOMIA COMPARATIVA E RISCO ---
        st.markdown("<h4 style='font-size: 1.1rem; font-weight: 600; color: #4B5563; margin-bottom: 0.2rem;'>Anatomia Comparativa: Dispersão e Escala de Espera</h4>", unsafe_allow_html=True)
        
        st.markdown("""
            <div style="font-size: 0.85rem; color: #6b7280; margin-bottom: 1.5rem; line-height: 1.4;">
                <b>Como ler os gráficos:</b> A <b>linha central</b> na caixa é a mediana (paciente típico). 
                A <b>caixa</b> agrupa 50% da fila. As <b>linhas tracejadas</b> mostram as fronteiras SRE: 
                <b>P10</b> (os 10% mais rápidos/eficiência) e <b>P90</b> (o limite de 90% da rede/garantia). 
                Os <b>pontos individuais</b> à direita são os <i>outliers</i> (casos críticos fora do padrão).
            </div>
        """, unsafe_allow_html=True)

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
                
                if pd.isna(min_fence): min_fence = df_clean[col].min()
                if pd.isna(max_fence): max_fence = df_clean[col].max()

                # Separação Top/Bottom em zigue-zague para os textos NUNCA colidirem na UI
                stats_top = {"Min": min_fence, "Q1": q1, "Q3": q3, "Max": max_fence}
                stats_bot = {"P10": p10, "Med": med, "P90": p90}
                
                # Aplica cor BRANCA aos valores
                for label, val in stats_top.items():
                    if pd.notna(val):
                        fig.add_annotation(x=val, y=0.58, yref="paper", text=f"{label}<br><b>{int(val)}</b>", 
                                           showarrow=False, font=dict(size=11, color="white"), yanchor="bottom", align="center")
                
                for label, val in stats_bot.items():
                    if pd.notna(val):
                        fig.add_annotation(x=val, y=0.42, yref="paper", text=f"<b>{int(val)}</b><br>{label}", 
                                           showarrow=False, font=dict(size=11, color="white"), yanchor="top", align="center")
                
                # Desenha os fences pontilhados discretos para P10 e P90 usando a cor original da linha do plot
                for val in [p10, p90]:
                    if pd.notna(val):
                        fig.add_shape(type="line", x0=val, x1=val, y0=0.35, y1=0.65, yref="paper", line=dict(color=line_color, width=2, dash="dot"))

            df_plot_fila, p10_fila, p90_fila = get_sre_stats(df_dist, 'dias_fila')
            df_plot_esq, p10_esq, p90_esq = get_sre_stats(df_dist, 'dias_esquecido')

            # Escala Unificada para comparação direta (Adicionamos margem negativa para os textos não cortarem)
            max_val = max(df_plot_fila['dias_fila'].max(), df_plot_esq['dias_esquecido'].max()) if not df_plot_fila.empty and not df_plot_esq.empty else 100
            limite_x = [-max_val * 0.08, max_val * 1.08]

            # --- RENDERIZAÇÃO: BOXPLOT ABANDONO (VERMELHO) ---
            # SRE Performance Fix: Amostragem para evitar MessageSizeError (OOM do FrontEnd via Websocket de >200MB)
            df_render_esq = df_plot_esq.sample(n=min(10000, len(df_plot_esq)), random_state=42) if not df_plot_esq.empty else df_plot_esq
            fig_esq = px.box(df_render_esq, x="dias_esquecido", title="Abandono: Dias sem Evolução", 
                             points="outliers", color_discrete_sequence=['#ef4444'], range_x=limite_x)
            
            # Aplica a anotação SOTA
            annotate_boxplot(fig_esq, df_plot_esq, 'dias_esquecido', p10_esq, p90_esq, '#ef4444')
            
            # Remove Hover (SRE UX: Zero Distraction)
            fig_esq.update_traces(hoverinfo="skip", hovertemplate=None)
            fig_esq.update_layout(hovermode=False, height=200, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=40, b=40))
            fig_esq.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#f1f5f9')
            st.plotly_chart(fig_esq, use_container_width=True, config={'displayModeBar': False})

            # --- RENDERIZAÇÃO: BOXPLOT CADASTRO (AZUL) ---
            df_render_fila = df_plot_fila.sample(n=min(10000, len(df_plot_fila)), random_state=42) if not df_plot_fila.empty else df_plot_fila
            fig_fila = px.box(df_render_fila, x="dias_fila", title="Cadastro: Dias de Espera", 
                              points="outliers", color_discrete_sequence=['#3b82f6'], range_x=limite_x)
            
            # Aplica a anotação SOTA
            annotate_boxplot(fig_fila, df_plot_fila, 'dias_fila', p10_fila, p90_fila, '#3b82f6')
                
            # Remove Hover (SRE UX: Zero Distraction)
            fig_fila.update_traces(hoverinfo="skip", hovertemplate=None)
            fig_fila.update_layout(hovermode=False, height=200, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=40, b=40))
            fig_fila.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#f1f5f9')
            st.plotly_chart(fig_fila, use_container_width=True, config={'displayModeBar': False})
            
            if len(df_dist) > len(df_plot_fila) or len(df_dist) > len(df_plot_esq):
                st.caption(f"ℹ️ Escala otimizada (outliers extremos ocultos do visor de alcance para facilitar visualização). Estatísticas preservadas.")

            # --- 2. INDICADORES P90 (PADRÃO ST.METRIC PARA CONSISTÊNCIA VISUAL) ---
            st.write(" ")
            g_p90_1, g_p90_2 = st.columns(2)
            
            with g_p90_1:
                st.metric(
                    label="⏳ P90 Tempo Esquecido", 
                    value=f"{p90_esquecido} dias", 
                    help="90% da rede não recebe atualizações clínicas há até este limite de dias."
                )
            
            with g_p90_2:
                st.metric(
                    label="⏱️ P90 Tempo de Fila", 
                    value=f"{p90_lead_time} dias", 
                    help="90% da rede espera até este limite de dias desde o cadastro para o agendamento."
                )

            # 3. GAUGES (FINAL DA SEÇÃO)
            st.write(" ")
            g1, g2 = st.columns(2)
            with g1:
                fig_gauge1 = go.Figure(go.Indicator(mode="gauge+number+delta", value=taxa_urgencia, number={'suffix': "%", 'font': {'color': '#4B5563'}}, title={'text': "Índice de Gravidade", 'font': {'size': 14}},
                    gauge={'axis': {'range': [0, 100]}, 'bar': {'color': "#ef4444" if taxa_urgencia > 30 else "#f97316"}, 'bgcolor': "rgba(0,0,0,0)", 'steps': [{'range': [0, 15], 'color': '#dcfce7'}, {'range': [15, 30], 'color': '#fef08a'}, {'range': [30, 100], 'color': '#fee2e2'}]}))
                fig_gauge1.update_layout(height=220, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_gauge1, use_container_width=True, config={'displayModeBar': False})
            with g2:
                fig_gauge2 = go.Figure(go.Indicator(mode="gauge+number", value=taxa_vencidos, number={'suffix': "%", 'font': {'color': '#4B5563'}}, title={'text': "Quebra de SLA (>180d)", 'font': {'size': 14}},
                    gauge={'axis': {'range': [0, 100]}, 'bar': {'color': "#1e293b"}, 'bgcolor': "rgba(0,0,0,0)", 'steps': [{'range': [0, 10], 'color': '#dcfce7'}, {'range': [10, 25], 'color': '#fef08a'}, {'range': [25, 100], 'color': '#fee2e2'}]}))
                fig_gauge2.update_layout(height=220, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor="rgba(0,0,0,0)")
                st.plotly_chart(fig_gauge2, use_container_width=True, config={'displayModeBar': False})

        st.divider()


    with t_macro:
        # --- BLOCO 1: EXPLORADOR DINÂMICO SOTA (EXPLOSÃO SOLAR BIVARIADA) ---
        st.subheader("📊 Explorador de Fila Dinâmico: Bivariado (Carga vs Latência/Risco)")
        
        st.info("💡 **Como ler (Gráfico Bivariado SRE):** \n"
                "- **Tamanho da Fatia:** Representa a **Carga (Volume)**. Fatias largas indicam muitos pacientes em espera.\n"
                "- **Cor da Fatia:** Representa a métrica de **Risco/Latência** escolhida. Tons quentes (vermelho) revelam gargalos, pacientes críticos ou faixas etárias avançadas, enquanto tons frios (azul) indicam fluxo rápido ou baixo risco.")
        
        # Dividimos a tela para os dois controles do usuário
        c_hier, c_metric = st.columns([0.7, 0.3])
        
        with c_hier:
            niveis_sunburst = st.multiselect(
                "Selecione a Hierarquia de Dados (Máx: 5 níveis):", 
                options=[
                    # --- Clínico & Regulação ---
                    "entidade_especialidade_especialidadeMae_descricao", "entidade_especialidade_descricao", "entidade_especialidade_cbo_descricao",
                    "entidade_cidPrincipal_codigo", "entidade_cidPrincipal_descricao",
                    "origem_lista", "situacao", 
                    "entidade_especialidade_tipoRegulacao", "entidade_especialidade_ativa", "entidade_especialidade_teleconsulta",
                    "entidade_centralRegulacao_nome", "entidade_unidadeOperador_centralRegulacao_nome",
                    
                    # --- Governança & Atores ---
                    "Ordem Judicial", "entidade_unidadeOperador_nome", "entidade_unidadeOperador_razaoSocial", "entidade_unidadeOperador_tipoUnidade_descricao", 
                    "medicoSolicitante", "operador_nome", "usuarioSolicitante_nome",
                    "evolucoes_json", "historico_evolucoes_completo",
                    
                    # --- Triagem & entidade_classificacaoRisco_totalPontos ---
                    "entidade_complexidade", "entidade_classificacaoRisco_cor", "corRegulador",
                    
                    # --- Demografia & Rede ---
                    "usuarioSUS_municipioResidencia_nome", "usuarioSUS_bairro", 
                    "usuarioSUS_sexo", "usuarioSUS_racaCor", "usuarioSUS_nacionalidade"
                ],
                default=["entidade_especialidade_especialidadeMae_descricao", "entidade_especialidade_descricao", "entidade_cidPrincipal_descricao"],
                max_selections=5,
                help="Arraste e solte as tags para reordenar o funil (path) do gráfico."
            )
            
        with c_metric:
            st.write(" ") # Alinhamento visual com o label do multiselect
            # Dicionário SRE: Mapeia a UX para a query OLAP
            METRICAS_COR = {
                "⏳ Tempo de Espera (Fila)": {"sql": "ROUND(AVG(SLA_Lead_Time_Total_Dias), 1)", "unit": "dias"},
                "⚠️ Tempo Esquecido (Sem Evolução)": {"sql": "ROUND(AVG(SLA_Tempo_Regulador_Dias), 1)", "unit": "dias"},
                "🚨 Pontos de Gravidade": {"sql": "ROUND(AVG(entidade_classificacaoRisco_pontosGravidade), 1)", "unit": "pts"},
                "⏱️ Pontos de Tempo": {"sql": "ROUND(AVG(entidade_classificacaoRisco_pontosTempo), 1)", "unit": "pts"},
                "🔥 Pontuação Total": {"sql": "ROUND(AVG(entidade_classificacaoRisco_totalPontos), 1)", "unit": "pts"},
                "🎂 Idade Média (Demografia)": {"sql": "ROUND(AVG(date_diff('year', TRY_CAST(usuarioSUS_dataNascimento AS DATE), CURRENT_DATE)), 1)", "unit": "anos"}
            }
            
            cor_selecionada = st.selectbox(
                "Métrica da Cor (Temperatura):",
                options=list(METRICAS_COR.keys()),
                index=0,
                help="Define o que a cor de cada fatia representa. O tamanho será sempre o volume de pacientes."
            )
        
        if niveis_sunburst:
            # Variáveis dinâmicas para a Query e para a UI
            levels_sql = ', '.join([f'"{n}"' for n in niveis_sunburst])
            sql_cor = METRICAS_COR[cor_selecionada]["sql"]
            unidade_cor = METRICAS_COR[cor_selecionada]["unit"]
            nome_metrica = cor_selecionada.split(" ", 1)[1] # Extrai apenas o texto sem o emoji para o gráfico
            
            # SQL OLAP Dinâmico: DuckDB calcula o cruzamento em tempo real
            df_plot_sun = use_case.execute_custom_query(f"""
                SELECT 
                    {levels_sql}, 
                    COUNT(DISTINCT numeroCMCE) as Vol,
                    {sql_cor} as Metrica_Cor
                FROM gercon
                WHERE {FINAL_WHERE}
                GROUP BY {levels_sql}
            """, filters, st.session_state.user)
            
            if not df_plot_sun.empty:
                # SRE FIX: Prevenção contra Nós Folha Vazios no Plotly
                for col in niveis_sunburst:
                    df_plot_sun[col] = df_plot_sun[col].replace('', 'Não Informado').fillna('Não Informado')

                # Paleta divergente universal (Azul = Baixo Risco/Rápido, Vermelho = Alto Risco/Atraso)
                paleta = 'RdYlBu_r' 
                
                fig_sun = px.sunburst(
                    df_plot_sun, 
                    path=niveis_sunburst, 
                    values='Vol',                 
                    color='Metrica_Cor',          
                    color_continuous_scale=paleta,
                    title=f"Análise Bivariada: Tamanho (Carga) vs Cor ({nome_metrica})",
                    labels={'Vol': 'Pacientes', 'Metrica_Cor': nome_metrica}
                )
                
                # SRE UX: Injeta dinamicamente a unidade correta (dias, pts ou anos) e remove bordas
                fig_sun.update_traces(
                    hovertemplate=f"<b>%{{label}}</b><br>Pacientes (Carga): %{{value}}<br>{nome_metrica}: %{{color}} {unidade_cor}<extra></extra>",
                    marker=dict(line=dict(width=0)) 
                )
                
                fig_sun.update_layout(height=700, margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig_sun, use_container_width=True, config={'displayModeBar': False})
            else:
                st.warning("⚠️ Nenhuma data disponível para o Sunburst com os filtros atuais.")
        else:
            st.warning("⚠️ Selecione pelo menos 1 nível para renderizar o gráfico.")
            
        st.markdown("---")
        st.subheader("⏱️ Golden Signals: Governança e Saúde do Fluxo")
        c1, c2 = st.columns([0.4, 0.6])

        
        with c1:
            # Matriz de Risco (Donut)
            df_risco = use_case.execute_custom_query(f"SELECT entidade_classificacaoRisco_cor, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND entidade_classificacaoRisco_cor != '' GROUP BY 1", filters=filters, current_user=st.session_state.user)
            if not df_risco.empty:
                # SRE FIX: Usando a nova variável global MAPA_CORES_RISCO
                st.plotly_chart(px.pie(df_risco, values='Vol', names='entidade_classificacaoRisco_cor', hole=0.5, color='entidade_classificacaoRisco_cor', color_discrete_map=MAPA_CORES_RISCO, title="Matriz de Risco (Prioridade)"), use_container_width=True, config={'displayModeBar': False})
            
        with c2:
            # Funil de Jornada (Conversão)
            df_funil = use_case.execute_custom_query(f"""
                SELECT '1. Solicitado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE}
                UNION ALL
                SELECT '2. Triado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND entidade_classificacaoRisco_cor != ''
                UNION ALL
                SELECT '3. Agendado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND situacao ILIKE '%AGENDADA%'
                UNION ALL
                SELECT '4. Realizado' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND (situacao ILIKE '%ATENDIDO%' OR situacao ILIKE '%REALIZADO%')
            """, filters, st.session_state.user)
            st.plotly_chart(px.funnel(df_funil, x='Vol', y='Etapa', title="Funil da Jornada: Gargalos e Abandono"), use_container_width=True, config={'displayModeBar': False})

        df_sit = use_case.execute_custom_query(f"SELECT situacao, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1 ORDER BY 2 DESC", filters=filters, current_user=st.session_state.user)
        st.plotly_chart(px.bar(df_sit, x='situacao', y='Vol', title="situacao Geral da Rede", color='situacao', template="plotly_white"), use_container_width=True, config={'displayModeBar': False})

    with t_clin:
        st.subheader("Inteligência Clínica & Perfil Demográfico")

        c1, c2 = st.columns(2)
        with c1:
            # Geometria da Demanda (Treemap)
            df_mun = use_case.execute_custom_query(f"SELECT usuarioSUS_municipioResidencia_nome, usuarioSUS_bairro, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND usuarioSUS_municipioResidencia_nome != '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30", filters=filters, current_user=st.session_state.user)
            
            # --- SRE FIX: Prevenção contra Nós Folha Vazios no Plotly ---
            if not df_mun.empty:
                df_mun['usuarioSUS_bairro'] = df_mun['usuarioSUS_bairro'].replace('', 'Não Informado').fillna('Não Informado')
                st.plotly_chart(px.treemap(df_mun, path=['usuarioSUS_municipioResidencia_nome', 'usuarioSUS_bairro'], values='Vol', title="Geometria: Município ➔ usuarioSUS_bairro", color='Vol', color_continuous_scale='Viridis'), use_container_width=True, config={'displayModeBar': False})
        with c2:
            # SRE FIX: Cálculo de Idade blindado (TRY_CAST para evitar Conversion Error)
            df_demo = use_case.execute_custom_query(f"""
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
            """, filters, st.session_state.user)
            
            if not df_demo.empty:
                fig_demo = px.histogram(
                    df_demo, 
                    x='Idade_Int', 
                    y='Vol', 
                    color='usuarioSUS_sexo', 
                    barmode='group', 
                    color_discrete_map={'Feminino':'#ec4899', 'Masculino':'#3b82f6'},
                    title="Perfil Demográfico (Idade vs usuarioSUS_sexo)",
                    labels={'Idade_Int': 'Idade Aproximada', 'Vol': 'Volume de Pacientes'}
                )
                st.plotly_chart(fig_demo, use_container_width=True, config={'displayModeBar': False})

        # Throughput vs Capacidade (Temporal)
        df_fluxo = use_case.execute_custom_query(f"SELECT CAST(dataSolicitacao AS DATE) as Dia, origem_lista, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND dataSolicitacao IS NOT NULL GROUP BY 1, 2 ORDER BY 1", filters=filters, current_user=st.session_state.user)
        st.plotly_chart(px.area(df_fluxo, x='Dia', y='Vol', color='origem_lista', title="Throughput Temporal: Volume de Pacientes por Origem"), use_container_width=True, config={'displayModeBar': False})

        st.markdown("---")
        st.subheader("🕵️ Auditoria de Padrões Clínicos (Médico vs Diagnóstico)")
        
        # --- 1. DEFINIÇÃO ESTRITA DE VARIÁVEIS DE ESTADO ---
        OPT_CID = "Análise Horizontal (Comparação de Pares)"
        OPT_MED = "Análise Vertical (Perfil Individual)"
        
        # --- 2. UI UX FIX: Controles Analíticos (Sliders Independentes) ---
        c_top1, c_top2, c_metric = st.columns([0.15, 0.15, 0.7])
        with c_top1:
            top_x_med = st.slider(
                "Top Médicos:", 
                min_value=5, max_value=100, value=15, step=1,
                help="Define a quantidade de médicos no eixo X."
            )
        with c_top2:
            top_x_cid = st.slider(
                "Top Diagnósticos:", 
                min_value=5, max_value=100, value=15, step=1,
                help="Define a quantidade de CIDs no eixo Y."
            )
        with c_metric:
            st.write(" ")
            modo_heatmap = st.radio(
                "Métrica de Visualização Analítica (Desvio Padrão):",
                options=[OPT_CID, OPT_MED], 
                horizontal=True
            )

        # --- CAIXA DE EXPLICAÇÃO DINÂMICA DE LEITURA (UX) ---
        if modo_heatmap == OPT_CID:
            st.info("💡 **Dica: Análise Horizontal (Comparação de Pares):** Avalia um mesmo **Diagnóstico (linha)** entre todos os médicos. Tons quentes (vermelho) indicam que o médico em questão solicita este CID com uma frequência **estatisticamente muito acima da média de seus colegas**. É útil para identificar profissionais que apresentam desvio sistêmico de conduta para uma doença específica.")
        else:
            st.info("💡 **Dica: Análise Vertical (Perfil Individual):** Avalia a rotina de um único **Médico (coluna)** comparando todos os diagnósticos que ele próprio emite. Tons quentes (vermelho) revelam quais CIDs são anomalias (excessos) que **fogem do padrão normal de trabalho daquele profissional específico**. É útil para detectar concentração atípica ou vieses de encaminhamento de um indivíduo.")

        # --- 3. EXTRACÇÃO OLAP (DuckDB com Limites Independentes) ---
        df_heatmap = use_case.execute_custom_query(f"""
            WITH TopMedicos AS (
                SELECT medicoSolicitante FROM gercon 
                WHERE {FINAL_WHERE} AND medicoSolicitante != '' AND medicoSolicitante IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT numeroCMCE) DESC LIMIT {top_x_med}
            ),
            TopCIDs AS (
                SELECT entidade_cidPrincipal_descricao FROM gercon 
                WHERE {FINAL_WHERE} AND entidade_cidPrincipal_descricao != '' AND entidade_cidPrincipal_descricao IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT numeroCMCE) DESC LIMIT {top_x_cid}
            )
            SELECT 
                medicoSolicitante, 
                entidade_cidPrincipal_descricao, 
                COUNT(DISTINCT numeroCMCE) as Vol
            FROM gercon
            WHERE {FINAL_WHERE}
              AND medicoSolicitante IN (SELECT medicoSolicitante FROM TopMedicos)
              AND entidade_cidPrincipal_descricao IN (SELECT entidade_cidPrincipal_descricao FROM TopCIDs)
            GROUP BY 1, 2
        """, filters, st.session_state.user)

        if not df_heatmap.empty:
            df_heatmap['CID_Curto'] = df_heatmap['entidade_cidPrincipal_descricao'].apply(lambda x: x[:45] + '...' if len(x) > 45 else x)
            
            # Cria a Matriz Base (Volumes Absolutos para hover)
            df_pivot_vol = df_heatmap.pivot_table(index='CID_Curto', columns='medicoSolicitante', values='Vol', fill_value=0)
            df_math = df_pivot_vol.copy().astype(float)
            
            # --- 4. MOTOR ESTATÍSTICO (Vetorização Pandas) ---
            paleta_heatmap = 'RdBu_r' 
            
            if modo_heatmap == OPT_CID:
                medias_linhas = df_math.mean(axis=1)
                desvios_linhas = df_math.std(axis=1).replace(0, 1)
                df_math = df_math.sub(medias_linhas, axis=0).div(desvios_linhas, axis=0)
            elif modo_heatmap == OPT_MED:
                medias_colunas = df_math.mean(axis=0)
                desvios_colunas = df_math.std(axis=0).replace(0, 1)
                df_math = df_math.sub(medias_colunas, axis=1).div(desvios_colunas, axis=1)

            # --- 5. FORMATADOR DE TEXTO VISUAL (Apenas Z-Score agora) ---
            df_text = df_math.apply(lambda col: col.map(lambda x: f"{x:+.1f}"))

            # --- 6. RENDERIZAÇÃO MATRICIAL SOTA (px.imshow) ---
            fig_heat = px.imshow(
                df_math, 
                aspect="auto", 
                color_continuous_scale=paleta_heatmap,
                color_continuous_midpoint=0, 
                title=f"Matriz de Desvios (Z-Score): Top {top_x_cid} CIDs vs Top {top_x_med} Médicos",
                labels=dict(x="Médico Solicitante", y="Diagnóstico (CID)", color="Z-Score")
            )
            
            fig_heat.update_traces(
                text=df_text.values,
                texttemplate="%{text}",
                customdata=df_pivot_vol.values,
                hovertemplate="<b>Médico:</b> %{x}<br><b>CID:</b> %{y}<br><b>Volume Real:</b> %{customdata} pacientes<br><b>Z-Score:</b> %{text} desvios<extra></extra>"
            )
            
            # A altura agora é ditada dinamicamente pelo slider de CIDs (Eixo Y)
            altura_dinamica = max(500, top_x_cid * 35) 
            fig_heat.update_layout(xaxis_tickangle=-45, height=altura_dinamica, margin=dict(l=250, b=120))
            st.plotly_chart(fig_heat, use_container_width=True, config={'displayModeBar': False})

        # --- GRÁFICO 2: TREEMAP HIERÁRQUICO DE PERFIL (Médico ➔ CID) ---
        df_perfil_med = use_case.execute_custom_query(f"""
            SELECT medicoSolicitante, entidade_cidPrincipal_descricao, COUNT(DISTINCT numeroCMCE) as Vol
            FROM gercon
            WHERE {FINAL_WHERE} AND medicoSolicitante != '' AND entidade_cidPrincipal_descricao != ''
            GROUP BY 1, 2 HAVING COUNT(DISTINCT numeroCMCE) >= 3 ORDER BY 3 DESC LIMIT 100
        """, filters, st.session_state.user)

        if not df_perfil_med.empty:
            df_perfil_med['medicoSolicitante'] = df_perfil_med['medicoSolicitante'].replace('', 'Médico Não Informado')
            df_perfil_med['entidade_cidPrincipal_descricao'] = df_perfil_med['entidade_cidPrincipal_descricao'].replace('', 'CID Não Informado')
            fig_tree_med = px.treemap(
                df_perfil_med, path=['medicoSolicitante', 'entidade_cidPrincipal_descricao'], values='Vol',
                color='Vol', color_continuous_scale='Teal', title="Perfil de Diagnóstico por Médico (Clique no Médico para expandir)"
            )
            fig_tree_med.update_layout(height=500, margin=dict(t=40, l=10, r=10, b=10))
            st.plotly_chart(fig_tree_med, use_container_width=True, config={'displayModeBar': False})

    with t_micro:
        st.subheader("Auditoria de Outliers & Top Ofensores (SRE)")
        
        c1, c2 = st.columns([0.7, 0.3])
        with c1:
            # Matriz de Outliers (Scatter Plot)
            st.markdown("### 🔍 Detecção de Outliers SLA")
            df_outliers = use_case.execute_custom_query(f"""
                SELECT numeroCMCE, entidade_classificacaoRisco_cor, TRY_CAST(entidade_classificacaoRisco_totalPontos AS INTEGER) as Pontos, 
                    DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) as DiasFila,
                    situacao, entidade_especialidade_descricao
                FROM gercon 
                WHERE {FINAL_WHERE} AND dataSolicitacao IS NOT NULL AND situacao NOT ILIKE '%ENCERRADA%'
                ORDER BY DiasFila DESC, Pontos DESC
                LIMIT 3000
            """, filters, st.session_state.user)
            if not df_outliers.empty:
                # 2. Prevenção de Nós Vazios
                df_outliers['entidade_classificacaoRisco_cor'] = df_outliers['entidade_classificacaoRisco_cor'].replace('', 'Não Informado').fillna('Não Informado')

                # 3. Plotagem do Scatter com os parâmetros matematicamente corretos usando a global
                fig_out = px.scatter(
                    df_outliers, 
                    x='DiasFila', 
                    y='Pontos', 
                    color='entidade_classificacaoRisco_cor',
                    color_discrete_map=MAPA_CORES_RISCO,
                    opacity=0.7,
                    size='Pontos',
                    hover_data=['numeroCMCE'],
                    title="Deteção de Outliers: Tempo de Fila vs Gravidade",
                    labels={'DiasFila': 'Tempo de Espera (Dias)', 'Pontos': 'entidade_classificacaoRisco_totalPontos de Gravidade'},
                    render_mode="svg" 
                )
                fig_out.add_hline(y=40, line_dash="dot", annotation_text="Alta Gravidade")
                fig_out.add_vline(x=180, line_dash="dot", annotation_text="SLA 180 d")
                st.plotly_chart(fig_out, use_container_width=True, config={'displayModeBar': False})
        
        with c2:
            # Top Ofensores (Barra Horizontal)
            st.markdown("### ⚖️ Top Ofensores")
            df_medico = use_case.execute_custom_query(f"SELECT medicoSolicitante, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND medicoSolicitante != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10", filters=filters, current_user=st.session_state.user)
            fig_ofensor = px.bar(df_medico, x='Vol', y='medicoSolicitante', orientation='h', title="Top 10 Médicos (Volume)")
            fig_ofensor.update_layout(yaxis={'categoryorder':'total ascending'}, height=450)
            st.plotly_chart(fig_ofensor, use_container_width=True, config={'displayModeBar': False})

        # Log Clinical Audit
        st.markdown("---")
        st.markdown("### 📝 Log de Evoluções Clínicas")
        
        c_slider, c_export = st.columns([0.8, 0.2])
        with c_slider:
            limit = st.slider("Amostra para Auditoria Clínica", 10, 1000, 100)
            
        df_audit = use_case.execute_custom_query(f"""
            SELECT numeroCMCE, CAST(dataSolicitacao AS DATE) as Solicitação, CAST(dataCadastro AS TIMESTAMP) as Data_Evolução, 
            situacao, entidade_classificacaoRisco_cor as "Risco Cor", historico_quadro_clinico 
            FROM gercon WHERE {FINAL_WHERE} ORDER BY dataSolicitacao DESC, dataCadastro DESC LIMIT {limit}
        """, filters, st.session_state.user)
        
        with c_export:
            st.write(" ") # Espaçamento vertical
            csv_data = df_audit.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Baixar CSV",
                data=csv_data,
                file_name=f"auditoria_gercon_{date.today()}.csv",
                mime='text/csv',
                use_container_width=True
            )
            
        st.dataframe(df_audit, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
