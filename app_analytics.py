import os
import duckdb
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import date, timedelta

# --- 1. CONFIGURAÇÃO DA PÁGINA E DX ---
st.set_page_config(page_title="Gercon Analytics | RCA", page_icon="🎯", layout="wide", initial_sidebar_state="expanded")

class AnalyticsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=("env/creds.env", "env/config.env"), env_file_encoding="utf-8", extra="ignore")
    OUTPUT_FILE: str = Field(default="gercon_consolidado.parquet")

settings = AnalyticsSettings()

def inject_custom_css():
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
        hr { margin-top: 1rem; margin-bottom: 1rem; }
        .filter-badge { display: inline-block; background-color: #e0f2fe; color: #1e40af; padding: 0.2rem 0.6rem; border-radius: 9999px; font-size: 0.85rem; font-weight: 500; margin-right: 0.5rem; margin-bottom: 0.5rem; border: 1px solid #bfdbfe;}
        .filter-category-title { font-weight: 600; font-size: 0.9rem; color: #374151; margin-bottom: 0.3rem;}
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

# --- 2. INFRASTRUCTURE: DUCKDB ENGINE ---
@st.cache_resource
def get_connection():
    # SRE FIX: Cria a conexão e a View UMA ÚNICA VEZ para evitar Race Conditions
    con = duckdb.connect(database=':memory:')
    con.execute(f"CREATE OR REPLACE VIEW gercon AS SELECT * FROM read_parquet('{settings.OUTPUT_FILE}')")
    return con

def query_db(sql_query: str) -> pd.DataFrame:
    # Read-Only Thread-Safe Access
    return get_connection().execute(sql_query).df()

def get_dynamic_options(column: str, current_where: str) -> list:
    try:
        q = f"SELECT DISTINCT \"{column}\" FROM gercon WHERE {current_where} AND \"{column}\" IS NOT NULL AND \"{column}\" != '' ORDER BY 1"
        return query_db(q)[column].tolist()
    except Exception:
        return []

@st.cache_data(ttl=3600)
def get_global_bounds(column: str, is_date=False):
    cast = "DATE" if is_date else "INTEGER"
    try:
        df = query_db(f"SELECT MIN(TRY_CAST(\"{column}\" AS {cast})) as vmin, MAX(TRY_CAST(\"{column}\" AS {cast})) as vmax FROM gercon")
        return df['vmin'].iloc[0], df['vmax'].iloc[0]
    except:
        return None, None

# --- 3. STATE MANAGEMENT ---
def clear_filter_state(keys_to_clear: list):
    for key in keys_to_clear:
        if key in st.session_state:
            if key.endswith("_in") or key.endswith("_ex") or key in ["dt_solic", "dt_cad", "dt_evo", "dt_nasc"]:
                st.session_state[key] = []
            elif key.endswith("_preset"): 
                # SRE FIX: Volta para Personalizado ao limpar filtros
                st.session_state[key] = "Personalizado"
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

# Callbacks para o Seletor de Datas Inteligente
def handle_preset_change(preset_key, date_key):
    preset = st.session_state[preset_key]
    today = date.today()
    
    if preset == "Hoje":
        st.session_state[date_key] = (today, today)
    elif preset == "Últimos 7 Dias (Past 7)":
        st.session_state[date_key] = (today - timedelta(days=7), today)
    elif preset == "Últimos 30 Dias (Past 30)":
        st.session_state[date_key] = (today - timedelta(days=30), today)
    elif preset == "Início da Semana até Hoje":
        st.session_state[date_key] = (today - timedelta(days=today.weekday()), today)
    elif preset == "Início do Mês até Hoje":
        st.session_state[date_key] = (today.replace(day=1), today)
    elif preset == "Início do Semestre até Hoje":
        start_sem = today.replace(month=1 if today.month <= 6 else 7, day=1)
        st.session_state[date_key] = (start_sem, today)
    elif preset == "Início do Ano até Hoje":
        st.session_state[date_key] = (today.replace(month=1, day=1), today)
    elif preset == "Últimos 2 Anos até Hoje":
        try: start_2y = today.replace(year=today.year - 2)
        except ValueError: start_2y = today.replace(year=today.year - 2, day=28) # Previne erro em anos bissextos
        st.session_state[date_key] = (start_2y, today)

def handle_date_change(preset_key):
    # Se o utilizador alterar o calendário manualmente, desmarca o preset
    st.session_state[preset_key] = "Personalizado"

# --- 4. UI COMPONENTS (DOMAIN FILTERS & TRACKING) ---
def render_include_exclude(label: str, column: str, clauses: list, current_where: str, key: str, ui_tracker: list, cat_keys: list):
    cat_keys.extend([f"{key}_in", f"{key}_ex"])
    options = get_dynamic_options(column, current_where)
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

def render_range_slider(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list):
    cat_keys.append(key)
    vmin, vmax = get_global_bounds(column, is_date=False)
    if vmin is not None and vmax is not None and vmin != vmax:
        vmin_val, vmax_val = int(vmin), int(vmax)
        val = st.slider(label, vmin_val, vmax_val, (vmin_val, vmax_val), key=key)
        if val[0] > vmin_val or val[1] < vmax_val:
            ui_tracker.append({"text": f"{label}: {val[0]} a {val[1]}", "keys": [key]})
            clauses.append(f"TRY_CAST(\"{column}\" AS INTEGER) BETWEEN {val[0]} AND {val[1]}")
    return " AND ".join(clauses)

def render_smart_date_range(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list):
    preset_key = f"{key}_preset"
    cat_keys.extend([preset_key, key])
    
    presets = [
        "Personalizado", 
        "Hoje", 
        "Últimos 7 Dias (Past 7)",
        "Últimos 30 Dias (Past 30)",
        "Início da Semana até Hoje", 
        "Início do Mês até Hoje", 
        "Início do Semestre até Hoje", 
        "Início do Ano até Hoje", 
        "Últimos 2 Anos até Hoje"
    ]
    
    if preset_key not in st.session_state:
        st.session_state[preset_key] = "Personalizado"
        
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    
    # 1. Seletor de Intervalo Rápido (Vem ANTES)
    st.selectbox("Período Rápido", presets, key=preset_key, on_change=handle_preset_change, args=(preset_key, key), label_visibility="collapsed")
    
    # 2. Calendário (Vem DEPOIS, reflete o que foi escolhido acima)
    val = st.date_input(label, key=key, on_change=handle_date_change, args=(preset_key,), label_visibility="collapsed")
    
    # Validação segura (evita crash se o utilizador selecionar apenas a data de início e não a do fim)
    if isinstance(val, tuple) and len(val) == 2:
        ui_tracker.append({"text": f"{label}: {val[0].strftime('%d/%m/%Y')} a {val[1].strftime('%d/%m/%Y')}", "keys": [key, preset_key]})
        clauses.append(f"CAST(\"{column}\" AS DATE) BETWEEN '{val[0]}' AND '{val[1]}'")
        
    return " AND ".join(clauses)

def render_advanced_text_search(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list, aggregate_by: str = None):
    """
    Renderiza um Toggle com lógica Booleana, tolerância a Acentos e suporte a Wildcards (*).
    Se aggregate_by for passado, utiliza 'bool_or' (Single-pass OLAP).
    """
    cat_keys.extend([f"{key}_toggle", f"{key}_and_val", f"{key}_or_val", f"{key}_not_val"])
    
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
                        ui_tracker.append({"text": f"{label} (OR Global): {or_terms}", "keys": [f"{key}_or_val", f"{key}_or"]})
                        words = [w for w in or_terms.split(',') if w.strip()]
                        if words:
                            or_expr = [f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}'))" for w in words]
                            having_conds.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append({"text": f"{label} (AND Global): {and_terms}", "keys": [f"{key}_and_val", f"{key}_and"]})
                        for w in [w for w in and_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}'))")
                            
                    if not_terms:
                        ui_tracker.append({"text": f"{label} (NOT Global): {not_terms}", "keys": [f"{key}_not_val", f"{key}_not"]})
                        for w in [w for w in not_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')) = FALSE")
                            
                    if having_conds:
                        subquery = f"SELECT \"{aggregate_by}\" FROM gercon GROUP BY \"{aggregate_by}\" HAVING {' AND '.join(having_conds)}"
                        clauses.append(f"\"{aggregate_by}\" IN ({subquery})")
                        
                # ESTRATÉGIA NORMAL: FILTRO POR EVENTO/LINHA
                else:
                    if or_terms:
                        ui_tracker.append({"text": f"{label} (OR Linha): {or_terms}", "keys": [f"{key}_or_val", f"{key}_or"]})
                        words = [w for w in or_terms.split(',') if w.strip()]
                        if words:
                            or_expr = [f"strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}')" for w in words]
                            clauses.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append({"text": f"{label} (AND Linha): {and_terms}", "keys": [f"{key}_and_val", f"{key}_and"]})
                        for w in [w for w in and_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            clauses.append(f"strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')")
                            
                    if not_terms:
                        ui_tracker.append({"text": f"{label} (NOT Linha): {not_terms}", "keys": [f"{key}_not_val", f"{key}_not"]})
                        for w in [w for w in not_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            clauses.append(f"strip_accents(\"{column}\") NOT ILIKE strip_accents('{p_term}')")

# --- 5. MAIN APP ---
def main():
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

    ui_filters = {
        "📅 Ciclo de Vida (Datas)": [], "🌍 Demografia & Rede": [], 
        "🩺 Clínico & Regulação": [], "⚠️ Triagem & Pontuação": [], 
        "🏛️ Governança & Atores": [], "📝 Logs Clínicos (Eventos)": []
    }
    state_keys = {k: [] for k in ui_filters.keys()}

    # ==========================================
    # CASCADING SIDEBAR (TOP-DOWN FLOW)
    # ==========================================
    st.sidebar.header("🎛️ Filtros em Cascata")

    cat = "📅 Ciclo de Vida (Datas)"
    with st.sidebar.expander(cat, expanded=False):
        # Substitui todo o código hardcoded pela nossa função SOTA
        curr_where = render_smart_date_range("Data de Solicitação", "Data Solicitação", clauses, "dt_solic", ui_filters[cat], state_keys[cat])
        st.write(" ") # Pequeno espaçamento
        curr_where = render_smart_date_range("Data do Cadastro", "Data do Cadastro", clauses, "dt_cad", ui_filters[cat], state_keys[cat])
        st.write(" ")
        curr_where = render_smart_date_range("Data da Evolução", "Data_Evolucao", clauses, "dt_evo", ui_filters[cat], state_keys[cat])

    cat = "🌍 Demografia & Rede"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Município de Residência", "Município de Residência", clauses, curr_where, "mun", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Bairro", "Bairro", clauses, curr_where, "bai", ui_filters[cat], state_keys[cat])
        
        render_advanced_text_search("Logradouro", "Logradouro", clauses, "txt_logr", ui_filters[cat], state_keys[cat])
        
        st.write(" ")
        state_keys[cat].extend(["num_min", "num_max"])
        num_min, num_max = st.columns(2)
        v_nmin = num_min.number_input("Número Min", value=0, step=10, key="num_min")
        v_nmax = num_max.number_input("Número Max", value=99999, step=100, key="num_max")
        if v_nmin > 0 or v_nmax < 99999: 
            ui_filters[cat].append({"text": f"Nº: {v_nmin} a {v_nmax}", "keys": ["num_min", "num_max"]})
            clauses.append(f"TRY_CAST(\"Número\" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}")
        
        state_keys[cat].append("dt_nasc")
        dt_nasc = st.date_input("Data Nascimento (Range)", value=[], key="dt_nasc")
        if len(dt_nasc) == 2: 
            ui_filters[cat].append({"text": f"Nascimento: {dt_nasc[0].strftime('%d/%m/%Y')} a {dt_nasc[1].strftime('%d/%m/%Y')}", "keys": ["dt_nasc"]})
            clauses.append(f"CAST(\"Data de Nascimento\" AS DATE) BETWEEN '{dt_nasc[0]}' AND '{dt_nasc[1]}'")
        
        curr_where = " AND ".join(clauses)
        curr_where = render_include_exclude("Nacionalidade", "Nacionalidade", clauses, curr_where, "nac", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Cor/Raça", "Cor", clauses, curr_where, "cor", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Sexo", "Sexo", clauses, curr_where, "sex", ui_filters[cat], state_keys[cat])

    cat = "🩺 Clínico & Regulação"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Origem da Lista", "Origem da Lista", clauses, curr_where, "lst", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Situação Atual", "Situação", clauses, curr_where, "sit", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Situação Final", "Situação Final", clauses, curr_where, "sitf", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Tipo de Regulação", "Tipo de Regulação", clauses, curr_where, "treg", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Status da Especialidade", "Status da Especialidade", clauses, curr_where, "stesp", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Teleconsulta", "Teleconsulta", clauses, curr_where, "tele", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        curr_where = render_include_exclude("Especialidade Mãe", "Especialidade Mãe", clauses, curr_where, "espm", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Especialidade Fina", "Especialidade", clauses, curr_where, "espf", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        curr_where = render_include_exclude("CID Código", "CID Código", clauses, curr_where, "cid_cod", ui_filters[cat], state_keys[cat])
        render_advanced_text_search("CID Descrição", "CID Descrição", clauses, "txt_cid_desc", ui_filters[cat], state_keys[cat])
        curr_where = " AND ".join(clauses)

    cat = "⚠️ Triagem & Pontuação"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Risco Cor (Atual)", "Risco Cor", clauses, curr_where, "r_cor", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Cor do Regulador", "Cor Regulador", clauses, curr_where, "c_reg", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Complexidade", "Complexidade", clauses, curr_where, "cpx", ui_filters[cat], state_keys[cat])
        
        curr_where = render_range_slider("Pontos Gravidade", "Pontos Gravidade", clauses, "pt_grav", ui_filters[cat], state_keys[cat])
        curr_where = render_range_slider("Pontos Tempo", "Pontos Tempo", clauses, "pt_tmp", ui_filters[cat], state_keys[cat])
        curr_where = render_range_slider("Pontuação Total", "Pontuação", clauses, "pt_tot", ui_filters[cat], state_keys[cat])

    cat = "🏛️ Governança & Atores"
    with st.sidebar.expander(cat, expanded=False):
        state_keys[cat].append("oj_radio")
        oj = st.radio("Ordem Judicial", ["Ambos", "Sim", "Não"], horizontal=True, key="oj_radio")
        if oj == "Sim": 
            ui_filters[cat].append({"text": "Ordem Judicial: Sim", "keys": ["oj_radio"]})
            clauses.append("(\"Ordem Judicial\" IS NOT NULL AND \"Ordem Judicial\" != '')")
        elif oj == "Não": 
            ui_filters[cat].append({"text": "Ordem Judicial: Não", "keys": ["oj_radio"]})
            clauses.append("(\"Ordem Judicial\" IS NULL OR \"Ordem Judicial\" = '')")
        
        curr_where = " AND ".join(clauses)
        curr_where = render_include_exclude("Unidade Solicitante", "Unidade Solicitante", clauses, curr_where, "usol", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        curr_where = render_include_exclude("Médico Solicitante", "Médico Solicitante", clauses, curr_where, "med_sol", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Operador", "Operador", clauses, curr_where, "oper", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Usuário Solicitante", "Usuário Solicitante", clauses, curr_where, "usr_sol", ui_filters[cat], state_keys[cat])
        curr_where = " AND ".join(clauses)

    cat = "📝 Logs Clínicos (Eventos)"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Tipo de Informação", "Tipo_Informacao", clauses, curr_where, "tinf", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        render_advanced_text_search("Origem da Informação", "Origem_Informacao", clauses, "txt_orig_inf", ui_filters[cat], state_keys[cat])
        
        # AQUI ACONTECE A MÁGICA CLÍNICA: Agregação pelo Protocolo inteiro
        render_advanced_text_search("Evoluções do Paciente", "Texto_Evolucao", clauses, "txt_evo", ui_filters[cat], state_keys[cat], aggregate_by="Protocolo")

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

    with st.spinner("Processando Modelo de Leitura (OLAP) e Latência de Cauda (P90)..."):
        kpis = query_db(f"""
            SELECT COUNT(DISTINCT Protocolo) as pacientes, 
                   COUNT(*) as eventos, 
                   COUNT(DISTINCT "Especialidade Mãe") as esp_mae,
                   COUNT(DISTINCT Especialidade) as sub_esp,
                   COUNT(DISTINCT "Médico Solicitante") as medicos,
                   COUNT(DISTINCT "CID Descrição") as cids,
                   COUNT(DISTINCT "Origem da Lista") as origens,
                   ROUND(AVG(DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE)), 1) as lead_time,
                   MAX(DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE)) as max_lead_time,
                   DATEDIFF('day', MIN(CAST("Data Solicitação" AS DATE)), MAX(CAST("Data Solicitação" AS DATE))) as span_dias,
                   COUNT(DISTINCT CASE WHEN "Risco Cor" IN ('VERMELHO', 'LARANJA', 'AMARELO') THEN Protocolo END) as pac_urgentes,
                   COUNT(DISTINCT CASE WHEN DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE) > 180 THEN Protocolo END) as pac_vencidos
            FROM gercon WHERE {FINAL_WHERE}
        """)

        # --- SRE FIX: Query de Latência de Cauda (P90) ---
        # Agrupamos por Protocolo PRIMEIRO para garantir peso igual por paciente.
        p90_metrics = query_db(f"""
            SELECT 
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY dias_fila) as p90_lead_time,
                PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY dias_esquecido) as p90_esquecido
            FROM (
                SELECT 
                    Protocolo,
                    DATEDIFF('day', MIN(CAST("Data Solicitação" AS DATE)), CURRENT_DATE) as dias_fila,
                    DATEDIFF('day', MAX(CAST(Data_Evolucao AS TIMESTAMP)), CURRENT_DATE) as dias_esquecido
                FROM gercon
                WHERE {FINAL_WHERE}
                GROUP BY Protocolo
            )
        """)

    # --- Extração Segura das Variáveis Absolutas ---
    pacientes = int(kpis['pacientes'].iloc[0])
    eventos = int(kpis['eventos'].iloc[0])
    esp_mae = int(kpis['esp_mae'].iloc[0])
    sub_esp = int(kpis['sub_esp'].iloc[0])
    medicos = int(kpis['medicos'].iloc[0])
    cids = int(kpis['cids'].iloc[0])
    origens = int(kpis['origens'].iloc[0])
    lead_time = kpis['lead_time'].iloc[0]
    max_lead_time = int(kpis['max_lead_time'].iloc[0]) if pd.notna(kpis['max_lead_time'].iloc[0]) else 0
    span_dias = kpis['span_dias'].iloc[0]
    pac_urgentes = int(kpis['pac_urgentes'].iloc[0])
    pac_vencidos = int(kpis['pac_vencidos'].iloc[0])
    
    # Extração das Métricas P90 (Tolerância a falhas caso não haja dados)
    p90_lead_time = int(p90_metrics['p90_lead_time'].iloc[0]) if pd.notna(p90_metrics['p90_lead_time'].iloc[0]) else 0
    p90_esquecido = int(p90_metrics['p90_esquecido'].iloc[0]) if pd.notna(p90_metrics['p90_esquecido'].iloc[0]) else 0
    
    # --- Cálculos Derivados SOTA (Prevenção contra divisão por zero) ---
    evo_por_paciente = round(eventos / pacientes, 1) if pacientes > 0 else 0.0
    sub_por_esp = round(sub_esp / esp_mae, 1) if esp_mae > 0 else 0.0
    cid_por_medico = round(cids / medicos, 1) if medicos > 0 else 0.0
    evo_por_medico = round(eventos / medicos, 1) if medicos > 0 else 0.0

    # SRE FIX: Motor de Taxa de Ingestão (Cadastros por Mês)
    dias_janela = float(span_dias) if pd.notna(span_dias) else 0.0
    meses_janela = max(dias_janela / 30.416, 1.0) 
    cad_por_mes = round(pacientes / meses_janela, 1) if pacientes > 0 else 0.0
    
    taxa_urgencia = round((pac_urgentes / pacientes) * 100, 1) if pacientes > 0 else 0.0
    taxa_vencidos = round((pac_vencidos / pacientes) * 100, 1) if pacientes > 0 else 0.0

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
        
        # --- LINHA 2: Complexidade Clínica e SLA ---
        r2_c1, r2_c2, r2_c3, r2_c4 = st.columns(4)
        r2_c1.metric("🏛️ Especialidades (Mãe)", f"{esp_mae:,}".replace(',', '.'), help="Grandes áreas clínicas abrangidas (Ex: CIRURGIA).")
        r2_c2.metric("🎯 Subespecialidades", f"{sub_esp:,}".replace(',', '.'), help="Especialidades finas abrangidas (Ex: CIRURGIA DA MÃO).")
        r2_c3.metric("🔀 Subs/Especialidade", f"{sub_por_esp}".replace('.', ','), help="Média de ramificações por grande área clínica.")
        
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

        df_dist = query_db(f"""
            SELECT 
                DATEDIFF('day', MIN(CAST("Data Solicitação" AS DATE)), CURRENT_DATE) as dias_fila,
                DATEDIFF('day', MAX(CAST(Data_Evolucao AS TIMESTAMP)), CURRENT_DATE) as dias_esquecido
            FROM gercon
            WHERE {FINAL_WHERE}
            GROUP BY Protocolo
        """)

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

            df_plot_fila, p10_fila, p90_fila = get_sre_stats(df_dist, 'dias_fila')
            df_plot_esq, p10_esq, p90_esq = get_sre_stats(df_dist, 'dias_esquecido')

            # Escala Unificada para comparação direta
            max_val = max(df_plot_fila['dias_fila'].max(), df_plot_esq['dias_esquecido'].max()) if not df_plot_fila.empty and not df_plot_esq.empty else 100
            limite_x = [0, max_val * 1.05]

            # --- RENDERIZAÇÃO: BOXPLOT ABANDONO (VERMELHO) ---
            fig_esq = px.box(df_plot_esq, x="dias_esquecido", title="Distribuição: Dias sem Evolução (Abandono)", 
                             points="outliers", color_discrete_sequence=['#ef4444'], range_x=limite_x)
            
            # Injetar Linhas P10 e P90 (Fronteiras de Eficiência e Garantia)
            for val, label in zip([p10_esq, p90_esq], ["P10", "P90"]):
                fig_esq.add_vline(x=val, line_dash="dash", line_color="#4b5563", opacity=0.7)
                fig_esq.add_annotation(x=val, y=0.8, text=f"{label}: {int(val)}d", showarrow=False, font=dict(size=10, color="#4b5563"), bgcolor="rgba(255,255,255,0.8)")
            
            fig_esq.update_layout(height=200, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=40, b=40))
            fig_esq.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#f1f5f9')
            st.plotly_chart(fig_esq, use_container_width=True, config={'displayModeBar': False})

            # --- RENDERIZAÇÃO: BOXPLOT CADASTRO (AZUL) ---
            fig_fila = px.box(df_plot_fila, x="dias_fila", title="Distribuição: Dias de Espera (Cadastro)", 
                              points="outliers", color_discrete_sequence=['#3b82f6'], range_x=limite_x)
            
            # Injetar Linhas P10 e P90
            for val, label in zip([p10_fila, p90_fila], ["P10", "P90"]):
                fig_fila.add_vline(x=val, line_dash="dash", line_color="#4b5563", opacity=0.7)
                fig_fila.add_annotation(x=val, y=0.8, text=f"{label}: {int(val)}d", showarrow=False, font=dict(size=10, color="#4b5563"), bgcolor="rgba(255,255,255,0.8)")
                
            fig_fila.update_layout(height=200, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=40, b=40))
            fig_fila.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#f1f5f9')
            st.plotly_chart(fig_fila, use_container_width=True, config={'displayModeBar': False})
            
            if len(df_dist) > len(df_plot_fila) or len(df_dist) > len(df_plot_esq):
                st.caption(f"ℹ️ Escala otimizada (outliers extremos ocultos). Mediana e Quartis preservados.")

            # --- 2. INDICADORES P90 (PADRÃO ST.METRIC PARA CONSISTÊNCIA VISUAL) ---
            st.write(" ")
            g_p90_1, g_p90_2 = st.columns(2)
            
            with g_p90_1:
                st.metric(
                    label="⏱️ P90 Tempo de Fila", 
                    value=f"{p90_lead_time} dias", 
                    help="90% da rede espera até este limite de dias desde o cadastro para o agendamento."
                )
            
            with g_p90_2:
                st.metric(
                    label="⏳ P90 Tempo Esquecido", 
                    value=f"{p90_esquecido} dias", 
                    help="90% da rede não recebe atualizações clínicas há até este limite de dias."
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
        st.subheader("Governança e Saúde do Fluxo")
        c1, c2 = st.columns([0.4, 0.6])
        
        with c1:
            # Matriz de Risco (Donut)
            df_risco = query_db(f"SELECT \"Risco Cor\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Risco Cor\" != '' GROUP BY 1")
            if not df_risco.empty:
                # SRE FIX: Usando a nova variável global MAPA_CORES_RISCO
                st.plotly_chart(px.pie(df_risco, values='Vol', names='Risco Cor', hole=0.5, color='Risco Cor', color_discrete_map=MAPA_CORES_RISCO, title="Matriz de Risco (Prioridade)"), use_container_width=True, config={'displayModeBar': False})
            
        with c2:
            # Funil de Jornada (Conversão)
            df_funil = query_db(f"""
                SELECT '1. Solicitado' as Etapa, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE}
                UNION ALL
                SELECT '2. Triado' as Etapa, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND "Risco Cor" != ''
                UNION ALL
                SELECT '3. Agendado' as Etapa, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND Situação ILIKE '%AGENDADA%'
                UNION ALL
                SELECT '4. Realizado' as Etapa, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND ("Situação Final" ILIKE '%ATENDIDO%' OR "Situação Final" ILIKE '%REALIZADO%')
            """)
            st.plotly_chart(px.funnel(df_funil, x='Vol', y='Etapa', title="Funil da Jornada: Gargalos e Abandono"), use_container_width=True, config={'displayModeBar': False})

        df_sit = query_db(f"SELECT Situação, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1 ORDER BY 2 DESC")
        st.plotly_chart(px.bar(df_sit, x='Situação', y='Vol', title="Situação Geral da Rede", color='Situação', template="plotly_white"), use_container_width=True, config={'displayModeBar': False})

    with t_clin:
        st.subheader("Inteligência Clínica & Perfil Demográfico")
        
        # Sunburst Hierárquico de 3 Níveis: Especialidade Mãe -> Especialidade Fina -> CID
        df_esp = query_db(f"SELECT \"Especialidade Mãe\", Especialidade, \"CID Descrição\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"CID Descrição\" != '' GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 50")
        
        # --- SRE FIX: Prevenção contra Nós Folha Vazios no Plotly ---
        if not df_esp.empty:
            df_esp['Especialidade Mãe'] = df_esp['Especialidade Mãe'].replace('', 'Sem Categoria Mãe').fillna('Sem Categoria Mãe')
            df_esp['Especialidade'] = df_esp['Especialidade'].replace('', 'Sem Subespecialidade').fillna('Sem Subespecialidade')
            df_esp['CID Descrição'] = df_esp['CID Descrição'].replace('', 'CID Não Informado').fillna('CID Não Informado')
            
            fig_sun = px.sunburst(df_esp, path=['Especialidade Mãe', 'Especialidade', 'CID Descrição'], values='Vol', color='Vol', color_continuous_scale='Blues', title="Explosão Solar: Especialidade Mãe ➔ Fina ➔ CID")
            
            # UX FIX: Aumentar drasticamente a altura e otimizar as margens
            fig_sun.update_layout(
                height=850, # Altura forçada em pixels (quase o dobro do padrão)
                margin=dict(t=40, l=10, r=10, b=10) # Reduz o espaço em branco à volta
            )
            st.plotly_chart(fig_sun, use_container_width=True, config={'displayModeBar': False})

        c1, c2 = st.columns(2)
        with c1:
            # Geometria da Demanda (Treemap)
            df_mun = query_db(f"SELECT \"Município de Residência\", Bairro, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Município de Residência\" != '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30")
            
            # --- SRE FIX: Prevenção contra Nós Folha Vazios no Plotly ---
            if not df_mun.empty:
                df_mun['Bairro'] = df_mun['Bairro'].replace('', 'Não Informado').fillna('Não Informado')
                st.plotly_chart(px.treemap(df_mun, path=['Município de Residência', 'Bairro'], values='Vol', title="Geometria: Município ➔ Bairro", color='Vol', color_continuous_scale='Viridis'), use_container_width=True, config={'displayModeBar': False})
        with c2:
            # SRE FIX: Cálculo de Idade blindado (TRY_CAST para evitar Conversion Error)
            df_demo = query_db(f"""
                SELECT Idade_Int, Sexo, COUNT(DISTINCT Protocolo) as Vol
                FROM (
                    SELECT 
                        date_diff('year', TRY_CAST("Data de Nascimento" AS DATE), CURRENT_DATE) as Idade_Int, 
                        Sexo, 
                        Protocolo
                    FROM gercon 
                    WHERE {FINAL_WHERE}
                ) 
                WHERE Idade_Int IS NOT NULL AND Idade_Int >= 0
                GROUP BY 1, 2
            """)
            
            if not df_demo.empty:
                fig_demo = px.histogram(
                    df_demo, 
                    x='Idade_Int', 
                    y='Vol', 
                    color='Sexo', 
                    barmode='group', 
                    nbins=20,
                    color_discrete_map={'Feminino':'#ec4899', 'Masculino':'#3b82f6'},
                    title="Perfil Demográfico (Idade vs Sexo)",
                    labels={'Idade_Int': 'Idade Aproximada', 'Vol': 'Volume de Pacientes'}
                )
                st.plotly_chart(fig_demo, use_container_width=True, config={'displayModeBar': False})

        # Throughput vs Capacidade (Temporal)
        df_fluxo = query_db(f"SELECT CAST(\"Data Solicitação\" AS DATE) as Dia, \"Origem da Lista\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Data Solicitação\" IS NOT NULL GROUP BY 1, 2 ORDER BY 1")
        st.plotly_chart(px.area(df_fluxo, x='Dia', y='Vol', color='Origem da Lista', title="Throughput Temporal: Volume de Protocolos por Origem"), use_container_width=True, config={'displayModeBar': False})

        st.markdown("---")
        st.subheader("🕵️ Auditoria de Padrões Clínicos (Médico vs Diagnóstico)")
        
        # --- 1. DEFINIÇÃO ESTRITA DE VARIÁVEIS DE ESTADO (Elimina bugs de strings) ---
        OPT_CID = "Z-Score (Desvio Padrão por CID)"
        OPT_MED = "Z-Score (Desvio Padrão por Médico)"
        OPT_ABS = "Volume Absoluto de Pacientes"
        
        # --- 2. UI UX FIX: Controles Analíticos ---
        c_top, c_metric = st.columns([0.3, 0.7])
        with c_top:
            top_x = st.slider(
                "Selecione o Top X (Volume de Análise):", 
                min_value=5, max_value=50, value=15, step=5,
                help="Define o tamanho da matriz cruzando os maiores ofensores."
            )
        with c_metric:
            st.write(" ")
            modo_heatmap = st.radio(
                "Métrica de Visualização no Mapa de Calor:",
                options=[OPT_CID, OPT_MED, OPT_ABS], 
                horizontal=True
            )

        # --- 3. EXTRACÇÃO OLAP (DuckDB) ---
        df_heatmap = query_db(f"""
            WITH TopMedicos AS (
                SELECT "Médico Solicitante" FROM gercon 
                WHERE {FINAL_WHERE} AND "Médico Solicitante" != '' AND "Médico Solicitante" IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT Protocolo) DESC LIMIT {top_x}
            ),
            TopCIDs AS (
                SELECT "CID Descrição" FROM gercon 
                WHERE {FINAL_WHERE} AND "CID Descrição" != '' AND "CID Descrição" IS NOT NULL
                GROUP BY 1 ORDER BY COUNT(DISTINCT Protocolo) DESC LIMIT {top_x}
            )
            SELECT 
                "Médico Solicitante", 
                "CID Descrição", 
                COUNT(DISTINCT Protocolo) as Vol
            FROM gercon
            WHERE {FINAL_WHERE}
              AND "Médico Solicitante" IN (SELECT "Médico Solicitante" FROM TopMedicos)
              AND "CID Descrição" IN (SELECT "CID Descrição" FROM TopCIDs)
            GROUP BY 1, 2
        """)

        if not df_heatmap.empty:
            df_heatmap['CID_Curto'] = df_heatmap['CID Descrição'].apply(lambda x: x[:45] + '...' if len(x) > 45 else x)
            
            # Cria a Matriz Base (Volumes Absolutos)
            df_pivot_vol = df_heatmap.pivot_table(index='CID_Curto', columns='Médico Solicitante', values='Vol', fill_value=0)
            df_math = df_pivot_vol.copy().astype(float)
            
            # --- 4. MOTOR ESTATÍSTICO (Vetorização Pandas) ---
            paleta_heatmap = 'Magma' 
            
            if modo_heatmap == OPT_CID:
                medias_linhas = df_math.mean(axis=1)
                desvios_linhas = df_math.std(axis=1).replace(0, 1)
                df_math = df_math.sub(medias_linhas, axis=0).div(desvios_linhas, axis=0)
                paleta_heatmap = 'RdBu_r' 
            elif modo_heatmap == OPT_MED:
                medias_colunas = df_math.mean(axis=0)
                desvios_colunas = df_math.std(axis=0).replace(0, 1)
                df_math = df_math.sub(medias_colunas, axis=1).div(desvios_colunas, axis=1)
                paleta_heatmap = 'RdBu_r'

            # --- 5. FORMATADOR DE TEXTO VISUAL ---
            if modo_heatmap == OPT_ABS:
                df_text = df_math.round(0).astype(int).astype(str)
            else:
                df_text = df_math.apply(lambda col: col.map(lambda x: f"{x:+.1f}"))

            # --- 6. RENDERIZAÇÃO MATRICIAL SOTA (px.imshow) ---
            fig_heat = px.imshow(
                df_math, 
                aspect="auto", 
                color_continuous_scale=paleta_heatmap,
                color_continuous_midpoint=0 if modo_heatmap != OPT_ABS else None, 
                title=f"Matriz de Concentração: Top {top_x} CIDs vs Top {top_x} Médicos",
                labels=dict(x="Médico Solicitante", y="Diagnóstico (CID)", color="Métrica")
            )
            
            fig_heat.update_traces(
                text=df_text.values,
                texttemplate="%{text}",
                customdata=df_pivot_vol.values,
                hovertemplate="<b>Médico:</b> %{x}<br><b>CID:</b> %{y}<br><b>Volume Real:</b> %{customdata} pacientes<br><b>Métrica Visível:</b> %{text}<extra></extra>"
            )
            
            altura_dinamica = max(500, top_x * 35) 
            fig_heat.update_layout(xaxis_tickangle=-45, height=altura_dinamica, margin=dict(l=250, b=120))
            st.plotly_chart(fig_heat, use_container_width=True, config={'displayModeBar': False})

        # --- GRÁFICO 2: TREEMAP HIERÁRQUICO DE PERFIL (Médico ➔ CID) ---
        df_perfil_med = query_db(f"""
            SELECT "Médico Solicitante", "CID Descrição", COUNT(DISTINCT Protocolo) as Vol
            FROM gercon
            WHERE {FINAL_WHERE} AND "Médico Solicitante" != '' AND "CID Descrição" != ''
            GROUP BY 1, 2 HAVING COUNT(DISTINCT Protocolo) >= 3 ORDER BY 3 DESC LIMIT 100
        """)

        if not df_perfil_med.empty:
            df_perfil_med['Médico Solicitante'] = df_perfil_med['Médico Solicitante'].replace('', 'Médico Não Informado')
            df_perfil_med['CID Descrição'] = df_perfil_med['CID Descrição'].replace('', 'CID Não Informado')
            fig_tree_med = px.treemap(
                df_perfil_med, path=['Médico Solicitante', 'CID Descrição'], values='Vol',
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
            df_outliers = query_db(f"""
                SELECT Protocolo, "Risco Cor", TRY_CAST(Pontuação AS INTEGER) as Pontos, 
                    DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE) as DiasFila,
                    Situação, Especialidade
                FROM gercon 
                WHERE {FINAL_WHERE} AND "Data Solicitação" IS NOT NULL AND Situação NOT ILIKE '%ENCERRADA%'
                ORDER BY DiasFila DESC, Pontos DESC
                LIMIT 3000
            """)
            if not df_outliers.empty:
                # 2. Prevenção de Nós Vazios
                df_outliers['Risco Cor'] = df_outliers['Risco Cor'].replace('', 'Não Informado').fillna('Não Informado')

                # 3. Plotagem do Scatter com os parâmetros matematicamente corretos usando a global
                fig_out = px.scatter(
                    df_outliers, 
                    x='DiasFila', 
                    y='Pontos', 
                    color='Risco Cor',
                    color_discrete_map=MAPA_CORES_RISCO,
                    opacity=0.7,
                    size='Pontos',
                    hover_data=['Protocolo'],
                    title="Deteção de Outliers: Tempo de Fila vs Gravidade",
                    labels={'DiasFila': 'Tempo de Espera (Dias)', 'Pontos': 'Pontuação de Gravidade'},
                    render_mode="svg" 
                )
                fig_out.add_hline(y=40, line_dash="dot", annotation_text="Alta Gravidade")
                fig_out.add_vline(x=180, line_dash="dot", annotation_text="SLA 180 d")
                st.plotly_chart(fig_out, use_container_width=True, config={'displayModeBar': False})
        
        with c2:
            # Top Ofensores (Barra Horizontal)
            st.markdown("### ⚖️ Top Ofensores")
            df_medico = query_db(f"SELECT \"Médico Solicitante\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Médico Solicitante\" != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
            fig_ofensor = px.bar(df_medico, x='Vol', y='Médico Solicitante', orientation='h', title="Top 10 Médicos (Volume)")
            fig_ofensor.update_layout(yaxis={'categoryorder':'total ascending'}, height=450)
            st.plotly_chart(fig_ofensor, use_container_width=True, config={'displayModeBar': False})

        # Log Clinical Audit
        st.markdown("---")
        st.markdown("### 📝 Log de Evoluções Clínicas")
        
        c_slider, c_export = st.columns([0.8, 0.2])
        with c_slider:
            limit = st.slider("Amostra para Auditoria Clínica", 10, 1000, 100)
            
        df_audit = query_db(f"""
            SELECT Protocolo, CAST(\"Data Solicitação\" AS DATE) as Solicitação, CAST(Data_Evolucao AS TIMESTAMP) as Data_Evolução, 
            Situação, \"Risco Cor\", Texto_Evolucao 
            FROM gercon WHERE {FINAL_WHERE} ORDER BY \"Data Solicitação\" DESC, Data_Evolucao DESC LIMIT {limit}
        """)
        
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
