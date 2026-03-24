import os
import duckdb
import streamlit as st
import pandas as pd
import plotly.express as px
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
        .stPlotlyChart { background-color: #ffffff; border-radius: 8px; border: 1px solid #e5e7eb; padding: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
        hr { margin-top: 1rem; margin-bottom: 1rem; }
        .filter-badge { display: inline-block; background-color: #e0f2fe; color: #1e40af; padding: 0.2rem 0.6rem; border-radius: 9999px; font-size: 0.85rem; font-weight: 500; margin-right: 0.5rem; margin-bottom: 0.5rem; border: 1px solid #bfdbfe;}
        .filter-category-title { font-weight: 600; font-size: 0.9rem; color: #374151; margin-bottom: 0.3rem;}
        .deep-search-bar { border-left: 4px solid #3b82f6; padding-left: 0.75rem; margin-top: 0.5rem; margin-bottom: 0.5rem; color: #4B5563; font-size: 0.9rem;}
        .aggregate-search-bar { border-left: 4px solid #8b5cf6; padding-left: 0.75rem; margin-top: 0.5rem; margin-bottom: 0.5rem; color: #4B5563; font-size: 0.9rem;}
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
        ui_tracker.append(f"{label} (✅): {', '.join([str(v) for v in incl])}")
        clauses.append(f"\"{column}\" IN ({', '.join([f"'{sanitize(v)}'" for v in incl])})")
        
    if excl: 
        # CORREÇÃO: Agora a exclusão é registada no tracker da interface visual
        ui_tracker.append(f"{label} (❌): {', '.join([str(v) for v in excl])}")
        clauses.append(f"\"{column}\" NOT IN ({', '.join([f"'{sanitize(v)}'" for v in excl])})")
    
    return " AND ".join(clauses)

def render_range_slider(label: str, column: str, clauses: list, key: str, ui_tracker: list, cat_keys: list):
    cat_keys.append(key)
    vmin, vmax = get_global_bounds(column, is_date=False)
    if vmin is not None and vmax is not None and vmin != vmax:
        vmin_val, vmax_val = int(vmin), int(vmax)
        val = st.slider(label, vmin_val, vmax_val, (vmin_val, vmax_val), key=key)
        if val[0] > vmin_val or val[1] < vmax_val:
            ui_tracker.append(f"{label}: {val[0]} a {val[1]}")
            clauses.append(f"TRY_CAST(\"{column}\" AS INTEGER) BETWEEN {val[0]} AND {val[1]}")
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
                        ui_tracker.append(f"{label} (OR Global): {or_terms}")
                        words = [w for w in or_terms.split(',') if w.strip()]
                        if words:
                            or_expr = [f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}'))" for w in words]
                            having_conds.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append(f"{label} (AND Global): {and_terms}")
                        for w in [w for w in and_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}'))")
                            
                    if not_terms:
                        ui_tracker.append(f"{label} (NOT Global): {not_terms}")
                        for w in [w for w in not_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            having_conds.append(f"bool_or(strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')) = FALSE")
                            
                    if having_conds:
                        subquery = f"SELECT \"{aggregate_by}\" FROM gercon GROUP BY \"{aggregate_by}\" HAVING {' AND '.join(having_conds)}"
                        clauses.append(f"\"{aggregate_by}\" IN ({subquery})")
                        
                # ESTRATÉGIA NORMAL: FILTRO POR EVENTO/LINHA
                else:
                    if or_terms:
                        ui_tracker.append(f"{label} (OR Linha): {or_terms}")
                        words = [w for w in or_terms.split(',') if w.strip()]
                        if words:
                            or_expr = [f"strip_accents(\"{column}\") ILIKE strip_accents('{parse_term(w)}')" for w in words]
                            clauses.append(f"({' OR '.join(or_expr)})")

                    if and_terms:
                        ui_tracker.append(f"{label} (AND Linha): {and_terms}")
                        for w in [w for w in and_terms.split(',') if w.strip()]:
                            p_term = parse_term(w)
                            clauses.append(f"strip_accents(\"{column}\") ILIKE strip_accents('{p_term}')")
                            
                    if not_terms:
                        ui_tracker.append(f"{label} (NOT Linha): {not_terms}")
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
        state_keys[cat].extend(["dt_solic", "dt_cad", "dt_evo"])
        
        dt_solic = st.date_input("Data de Solicitação", value=[], key="dt_solic")
        if len(dt_solic) == 2: 
            ui_filters[cat].append(f"Solicitação: {dt_solic[0].strftime('%d/%m/%Y')} a {dt_solic[1].strftime('%d/%m/%Y')}")
            clauses.append(f"CAST(\"Data Solicitação\" AS DATE) BETWEEN '{dt_solic[0]}' AND '{dt_solic[1]}'")
            
        dt_cad = st.date_input("Data do Cadastro", value=[], key="dt_cad")
        if len(dt_cad) == 2: 
            ui_filters[cat].append(f"Cadastro: {dt_cad[0].strftime('%d/%m/%Y')} a {dt_cad[1].strftime('%d/%m/%Y')}")
            clauses.append(f"CAST(\"Data do Cadastro\" AS DATE) BETWEEN '{dt_cad[0]}' AND '{dt_cad[1]}'")
            
        dt_evo = st.date_input("Data da Evolução", value=[], key="dt_evo")
        if len(dt_evo) == 2: 
            ui_filters[cat].append(f"Evolução: {dt_evo[0].strftime('%d/%m/%Y')} a {dt_evo[1].strftime('%d/%m/%Y')}")
            clauses.append(f"CAST(\"Data_Evolucao\" AS DATE) BETWEEN '{dt_evo[0]}' AND '{dt_evo[1]}'")
        curr_where = " AND ".join(clauses)

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
            ui_filters[cat].append(f"Nº: {v_nmin} a {v_nmax}")
            clauses.append(f"TRY_CAST(\"Número\" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}")
        
        state_keys[cat].append("dt_nasc")
        dt_nasc = st.date_input("Data Nascimento (Range)", value=[], key="dt_nasc")
        if len(dt_nasc) == 2: 
            ui_filters[cat].append(f"Nascimento: {dt_nasc[0].strftime('%d/%m/%Y')} a {dt_nasc[1].strftime('%d/%m/%Y')}")
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
            ui_filters[cat].append("Ordem Judicial: Sim")
            clauses.append("(\"Ordem Judicial\" IS NOT NULL AND \"Ordem Judicial\" != '')")
        elif oj == "Não": 
            ui_filters[cat].append("Ordem Judicial: Não")
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
    # VISUALIZAÇÃO DE FILTROS ATIVOS (TOP BAR)
    # ==========================================
    has_active_filters = any(len(v) > 0 for v in ui_filters.values())
    
    if has_active_filters:
        with st.expander("🔍 **VISUALIZAR E LIMPAR FILTROS APLICADOS**", expanded=True):
            for category, filters in ui_filters.items():
                if filters:
                    c1, c2 = st.columns([0.85, 0.15])
                    with c1:
                        st.markdown(f"<div class='filter-category-title'>{category}</div>", unsafe_allow_html=True)
                        badges_html = "".join([f"<span class='filter-badge'>{f}</span>" for f in filters])
                        st.markdown(badges_html, unsafe_allow_html=True)
                    with c2:
                        st.button("🗑️ Limpar", key=f"btn_clr_{category}", on_click=clear_filter_state, args=(state_keys[category],))
            
            st.markdown("---")
            all_keys = [key for sublist in state_keys.values() for key in sublist]
            st.button("🗑️ Limpar Todos os Filtros Globais", type="primary", on_click=clear_filter_state, args=(all_keys,))
    else:
        st.info("ℹ️ Nenhum filtro aplicado. A exibir a totalidade da base de dados.")

    # ==========================================
    # CLÁUSULA FINAL E PROCESSAMENTO (KPIs)
    # ==========================================
    FINAL_WHERE = " AND ".join(clauses)

    with st.spinner("Processando Modelo de Leitura (OLAP)..."):
        kpis = query_db(f"""
            SELECT COUNT(DISTINCT Protocolo) as pacientes, 
                   COUNT(*) as eventos, 
                   COUNT(DISTINCT Especialidade) as especialidades,
                   ROUND(AVG(DATEDIFF('day', CAST("Data Solicitação" AS DATE), CURRENT_DATE)), 1) as lead_time
            FROM gercon WHERE {FINAL_WHERE}
        """)

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("👥 Pacientes na Fila", f"{int(kpis['pacientes'].iloc[0]):,}".replace(',', '.'))
    m2.metric("📋 Evoluções Auditadas", f"{int(kpis['eventos'].iloc[0]):,}".replace(',', '.'))
    m3.metric("🎯 Especialidades", f"{int(kpis['especialidades'].iloc[0]):,}".replace(',', '.'))
    m4.metric("⏱️ Lead Time Médio", f"{kpis['lead_time'].iloc[0]} dias", help="Tempo médio de espera dos pacientes ativos.")
    st.divider()

    # Cores Semânticas
    cmap = {'VERMELHO': '#ef4444', 'AMARELO': '#eab308', 'VERDE': '#22c55e', 'AZUL': '#3b82f6', 'LARANJA': '#f97316'}

    # ==========================================
    # DASHBOARD TABS: ESTRATÉGICO -> TÁTICO -> OPERACIONAL
    # ==========================================
    t_macro, t_meso, t_micro = st.tabs(["📊 Estratégico (Macro)", "🎯 Tático & Clínica (Meso)", "🔎 Operacional & SRE (Micro)"])

    with t_macro:
        st.subheader("Governança e Saúde do Fluxo")
        c1, c2 = st.columns([0.4, 0.6])
        
        with c1:
            # Matriz de Risco (Donut)
            df_risco = query_db(f"SELECT \"Risco Cor\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Risco Cor\" != '' GROUP BY 1")
            if not df_risco.empty:
                st.plotly_chart(px.pie(df_risco, values='Vol', names='Risco Cor', hole=0.5, color='Risco Cor', color_discrete_map=cmap, title="Matriz de Risco (Prioridade)"), use_container_width=True, config={'displayModeBar': False})
            
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

    with t_meso:
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
        
        # --- UI UX FIX: Controles Analíticos (CID como Default) ---
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
                options=[
                    "Desvio Padrão (Comparado à média do CID)", 
                    "Desvio Padrão (Comparado à média do Médico)",
                    "Volume Absoluto"
                ],
                horizontal=True
            )

        # --- QUERY SOTA ---
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
            # 1. Trata Nomes Longos
            df_heatmap['CID_Curto'] = df_heatmap['CID Descrição'].apply(lambda x: x[:45] + '...' if len(x) > 45 else x)
            
            # 2. Preenchimento de Matriz 2D Direta (astype float para garantir cálculos limpos)
            df_pivot_vol = df_heatmap.pivot_table(index='CID_Curto', columns='Médico Solicitante', values='Vol', fill_value=0)
            df_pivot = df_pivot_vol.copy().astype(float)
            
            # 3. Motor Z-Score Dinâmico (VETORIZAÇÃO MATEMÁTICA PURA)
            cmap = 'Magma' 
            
            if "Média do CID" in modo_heatmap:
                mean_cid = df_pivot.mean(axis=1)
                std_cid = df_pivot.std(axis=1).replace(0, 1)
                df_pivot = df_pivot.sub(mean_cid, axis=0).div(std_cid, axis=0)
                cmap = 'RdBu_r' 
            elif "Média do Médico" in modo_heatmap:
                mean_med = df_pivot.mean(axis=0)
                std_med = df_pivot.std(axis=0).replace(0, 1)
                df_pivot = df_pivot.sub(mean_med, axis=1).div(std_med, axis=1)
                cmap = 'RdBu_r'

            # 4. Matriz de Formatação Visual (Textos em 2D)
            if "Absoluto" in modo_heatmap:
                df_text = df_pivot.round(0).astype(int).astype(str)
            else:
                df_text = df_pivot.apply(lambda col: col.map(lambda x: f"{x:+.1f}"))

            # --- PLOTAGEM SOTA ---
            fig_heat = px.imshow(
                df_pivot, 
                aspect="auto", 
                color_continuous_scale=cmap,
                color_continuous_midpoint=0 if "Desvio" in modo_heatmap else None, 
                title=f"Matriz de Concentração: Top {top_x} CIDs vs Top {top_x} Médicos",
                labels=dict(x="Médico Solicitante", y="Diagnóstico (CID)", color="Métrica")
            )
            
            fig_heat.update_traces(
                text=df_text.values,
                texttemplate="%{text}",
                customdata=df_pivot_vol.values,
                hovertemplate="<b>Médico:</b> %{x}<br><b>CID:</b> %{y}<br><b>Volume Real:</b> %{customdata} pacientes<br><b>Z-Score:</b> %{text}<extra></extra>"
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
                # FIX: render_mode="svg" proíbe o WebGL de crashar browsers limitados (SRE Architect Choice)
                fig_out = px.scatter(df_outliers, x='DiasFila', y='Pontos', color='Risco Cor', 
                                    size='Pontos', hover_data=['Protocolo'],
                                    color_discrete_map=cmap, title="Matriz RCA: Gravidade vs Tempo na Fila", render_mode="svg")
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
