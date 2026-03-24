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
    </style>
    """, unsafe_allow_html=True)

# --- 2. INFRASTRUCTURE: DUCKDB ENGINE ---
@st.cache_resource
def get_connection():
    con = duckdb.connect(database=':memory:')
    con.execute(f"CREATE OR REPLACE VIEW gercon AS SELECT * FROM read_parquet('{settings.OUTPUT_FILE}')")
    return con

def query_db(sql_query: str) -> pd.DataFrame:
    return get_connection().execute(sql_query).df()

def get_dynamic_options(column: str, current_where: str) -> list:
    """Busca opções válidas no DB respeitando os filtros já aplicados acima dele."""
    try:
        q = f"SELECT DISTINCT \"{column}\" FROM gercon WHERE {current_where} AND \"{column}\" IS NOT NULL AND \"{column}\" != '' ORDER BY 1"
        return query_db(q)[column].tolist()
    except Exception:
        return []

@st.cache_data(ttl=3600)
def get_global_bounds(column: str, is_date=False):
    """Busca limites absolutos para sliders não quebrarem a UI"""
    cast = "DATE" if is_date else "INTEGER"
    try:
        df = query_db(f"SELECT MIN(TRY_CAST(\"{column}\" AS {cast})) as vmin, MAX(TRY_CAST(\"{column}\" AS {cast})) as vmax FROM gercon")
        return df['vmin'].iloc[0], df['vmax'].iloc[0]
    except:
        return None, None

# --- 3. STATE MANAGEMENT (CALLBACKS) ---
def clear_filter_state(state_dict_to_clear: dict):
    """Callback SOTA: Injeta valores default no session_state para forçar o React a resetar o frontend."""
    for key, default_val in state_dict_to_clear.items():
        st.session_state[key] = default_val

# --- 4. UI COMPONENTS (DOMAIN FILTERS & TRACKING) ---
def render_include_exclude(label: str, column: str, clauses: list, current_where: str, key: str, ui_tracker: list, state_dict: dict):
    state_dict[f"{key}_in"] = []
    state_dict[f"{key}_ex"] = []
    
    options = get_dynamic_options(column, current_where)
    if not options: return current_where
    
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    incl = c1.multiselect("✅ Incluir", options, key=f"{key}_in", label_visibility="collapsed", placeholder="✅ Incluir...")
    excl = c2.multiselect("❌ Excluir", options, key=f"{key}_ex", label_visibility="collapsed", placeholder="❌ Excluir...")
    
    def sanitize(v): return str(v).replace("'", "''")
    if incl: 
        ui_tracker.append(f"{label}: {', '.join([str(v) for v in incl])}")
        clauses.append(f"\"{column}\" IN ({', '.join([f"'{sanitize(v)}'" for v in incl])})")
    if excl: 
        clauses.append(f"\"{column}\" NOT IN ({', '.join([f"'{sanitize(v)}'" for v in excl])})")
    
    return " AND ".join(clauses)

def render_range_slider(label: str, column: str, clauses: list, key: str, ui_tracker: list, state_dict: dict):
    vmin, vmax = get_global_bounds(column, is_date=False)
    if vmin is not None and vmax is not None and vmin != vmax:
        state_dict[key] = (int(vmin), int(vmax))
        val = st.slider(label, int(vmin), int(vmax), (int(vmin), int(vmax)), key=key)
        if val[0] > vmin or val[1] < vmax:
            ui_tracker.append(f"{label}: {val[0]} a {val[1]}")
            clauses.append(f"TRY_CAST(\"{column}\" AS INTEGER) BETWEEN {val[0]} AND {val[1]}")
    return " AND ".join(clauses)

def render_advanced_text_search(label: str, column: str, clauses: list, key: str, ui_tracker: list, state_dict: dict):
    state_dict[f"{key}_and"] = ""
    state_dict[f"{key}_or"] = ""
    state_dict[f"{key}_not"] = ""
    
    with st.popover(f"🔎 Busca: {label}", use_container_width=True):
        st.markdown(f"**Lógica de busca para `{label}`**")
        and_terms = st.text_input("✅ Contém TODAS as expressões (AND)", key=f"{key}_and")
        or_terms = st.text_input("⚠️ Contém QUALQUER expressão (OR)", key=f"{key}_or")
        not_terms = st.text_input("❌ NÃO contém as expressões (NOT)", key=f"{key}_not")
        
        def sanitize(v): return str(v).replace("'", "''")

        if and_terms:
            ui_tracker.append(f"{label} (AND): {and_terms}")
            for w in [sanitize(w.strip()) for w in and_terms.split(',') if w.strip()]:
                clauses.append(f"\"{column}\" ILIKE '%{w}%'")
        if or_terms:
            ui_tracker.append(f"{label} (OR): {or_terms}")
            words = [sanitize(w.strip()) for w in or_terms.split(',') if w.strip()]
            if words: clauses.append(f"({' OR '.join([f'\"{column}\" ILIKE \'{w}\'' for w in words])})")
        if not_terms:
            for w in [sanitize(w.strip()) for w in not_terms.split(',') if w.strip()]:
                clauses.append(f"\"{column}\" NOT ILIKE '%{w}%'")

# --- 5. MAIN APP ---
def main():
    inject_custom_css()
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f"⚠️ Base Parquet não encontrada ({settings.OUTPUT_FILE}).")
        return

    st.title("🎯 Gercon SRE | Advanced Root Cause Analysis")
    
    clauses = ["1=1"]
    curr_where = "1=1"

    # Dicionários para rastrear filtros ativos e chaves de estado (com valor default) por categoria
    ui_filters = {
        "📅 Ciclo de Vida": [], "🌍 Demografia & Rede": [], 
        "🩺 Clínico & Regulação": [], "⚠️ Triagem & Pontuação": [], 
        "🏛️ Governança & Atores": [], "📝 Logs Clínicos": []
    }
    state_keys = {k: {} for k in ui_filters.keys()}

    # ==========================================
    # CASCADING SIDEBAR (TOP-DOWN FLOW)
    # ==========================================
    st.sidebar.header("🎛️ Filtros em Cascata")

    # --- 1. CICLO DE VIDA (Datas) ---
    cat = "📅 Ciclo de Vida"
    with st.sidebar.expander(cat, expanded=False):
        state_keys[cat]["dt_solic"] = ()
        state_keys[cat]["dt_cad"] = ()
        state_keys[cat]["dt_evo"] = ()
        
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

    # --- 2. DEMOGRAFIA E LOCALIZAÇÃO ---
    cat = "🌍 Demografia & Rede"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Município de Residência", "Município de Residência", clauses, curr_where, "mun", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Bairro", "Bairro", clauses, curr_where, "bai", ui_filters[cat], state_keys[cat])
        
        render_advanced_text_search("Logradouro", "Logradouro", clauses, "txt_logr", ui_filters[cat], state_keys[cat])
        
        st.write(" ")
        state_keys[cat]["num_min"] = 0
        state_keys[cat]["num_max"] = 99999
        num_min, num_max = st.columns(2)
        v_nmin = num_min.number_input("Número Min", value=0, step=10, key="num_min")
        v_nmax = num_max.number_input("Número Max", value=99999, step=100, key="num_max")
        if v_nmin > 0 or v_nmax < 99999: 
            ui_filters[cat].append(f"Número: {v_nmin} a {v_nmax}")
            clauses.append(f"TRY_CAST(\"Número\" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}")
        
        state_keys[cat]["dt_nasc"] = ()
        dt_nasc = st.date_input("Data Nascimento (Range)", value=[], key="dt_nasc")
        if len(dt_nasc) == 2: 
            ui_filters[cat].append(f"Nascimento: {dt_nasc[0].strftime('%d/%m/%Y')} a {dt_nasc[1].strftime('%d/%m/%Y')}")
            clauses.append(f"CAST(\"Data de Nascimento\" AS DATE) BETWEEN '{dt_nasc[0]}' AND '{dt_nasc[1]}'")
        
        curr_where = " AND ".join(clauses)
        curr_where = render_include_exclude("Nacionalidade", "Nacionalidade", clauses, curr_where, "nac", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Cor/Raça", "Cor", clauses, curr_where, "cor", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Sexo", "Sexo", clauses, curr_where, "sex", ui_filters[cat], state_keys[cat])

    # --- 3. CLÍNICO & REGULAÇÃO ---
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
        curr_where = render_include_exclude("Especialidade (Fina)", "Especialidade", clauses, curr_where, "espf", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        curr_where = render_include_exclude("CID Código", "CID Código", clauses, curr_where, "cid_cod", ui_filters[cat], state_keys[cat])
        render_advanced_text_search("CID Descrição", "CID Descrição", clauses, "txt_cid_desc", ui_filters[cat], state_keys[cat])
        curr_where = " AND ".join(clauses)

    # --- 4. TRIAGEM & GRAVIDADE ---
    cat = "⚠️ Triagem & Pontuação"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Risco Cor (Atual)", "Risco Cor", clauses, curr_where, "r_cor", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Cor do Regulador", "Cor Regulador", clauses, curr_where, "c_reg", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Complexidade", "Complexidade", clauses, curr_where, "cpx", ui_filters[cat], state_keys[cat])
        
        curr_where = render_range_slider("Pontos Gravidade", "Pontos Gravidade", clauses, "pt_grav", ui_filters[cat], state_keys[cat])
        curr_where = render_range_slider("Pontos Tempo", "Pontos Tempo", clauses, "pt_tmp", ui_filters[cat], state_keys[cat])
        curr_where = render_range_slider("Pontuação Total", "Pontuação", clauses, "pt_tot", ui_filters[cat], state_keys[cat])

    # --- 5. GOVERNANÇA E ATORES ---
    cat = "🏛️ Governança & Atores"
    with st.sidebar.expander(cat, expanded=False):
        state_keys[cat]["oj_radio"] = "Ambos"
        oj = st.radio("Ordem Judicial", ["Ambos", "Sim", "Não"], horizontal=True, key="oj_radio")
        if oj == "Sim": 
            ui_filters[cat].append("Ordem Judicial: Sim")
            clauses.append("(\"Ordem Judicial\" IS NOT NULL AND \"Ordem Judicial\" != '')")
        if oj == "Não": 
            ui_filters[cat].append("Ordem Judicial: Não")
            clauses.append("(\"Ordem Judicial\" IS NULL OR \"Ordem Judicial\" = '')")
        
        curr_where = " AND ".join(clauses)
        curr_where = render_include_exclude("Unidade Solicitante", "Unidade Solicitante", clauses, curr_where, "usol", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        curr_where = render_include_exclude("Médico Solicitante", "Médico Solicitante", clauses, curr_where, "med_sol", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Operador do Sistema", "Operador", clauses, curr_where, "oper", ui_filters[cat], state_keys[cat])
        curr_where = render_include_exclude("Usuário Solicitante", "Usuário Solicitante", clauses, curr_where, "usr_sol", ui_filters[cat], state_keys[cat])
        curr_where = " AND ".join(clauses)

    # --- 6. LOG CLÍNICO ---
    cat = "📝 Logs Clínicos"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude("Tipo de Informação", "Tipo_Informacao", clauses, curr_where, "tinf", ui_filters[cat], state_keys[cat])
        
        st.markdown("---")
        render_advanced_text_search("Origem da Info", "Origem_Informacao", clauses, "txt_orig_inf", ui_filters[cat], state_keys[cat])
        render_advanced_text_search("Texto da Evolução", "Texto_Evolucao", clauses, "txt_evo", ui_filters[cat], state_keys[cat])

    # ==========================================
    # VISUALIZAÇÃO DE FILTROS ATIVOS (TOP BAR)
    # ==========================================
    has_active_filters = any(len(v) > 0 for v in ui_filters.values())
    
    if has_active_filters:
        with st.expander("🔍 **VISUALIZAR FILTROS ATIVOS**", expanded=True):
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
            all_keys = {}
            for subdict in state_keys.values():
                all_keys.update(subdict)
            st.button("🗑️ Limpar Todos os Filtros Globais", type="primary", on_click=clear_filter_state, args=(all_keys,))
    else:
        st.info("ℹ️ Nenhum filtro aplicado. A exibir a totalidade da base de dados.")

    # ==========================================
    # CLÁUSULA FINAL PARA OS GRÁFICOS
    # ==========================================
    FINAL_WHERE = " AND ".join(clauses)

    # --- KPIs DE TOPO ---
    with st.spinner("Processando Modelo de Leitura (OLAP)..."):
        kpis = query_db(f"""
            SELECT COUNT(DISTINCT Protocolo) as pacientes, COUNT(*) as eventos, COUNT(DISTINCT Especialidade) as especialidades
            FROM gercon WHERE {FINAL_WHERE}
        """)

    m1, m2, m3 = st.columns(3)
    m1.metric("👥 Pacientes Impactados", f"{int(kpis['pacientes'].iloc[0]):,}".replace(',', '.'))
    m2.metric("📋 Eventos Auditados", f"{int(kpis['eventos'].iloc[0]):,}".replace(',', '.'))
    m3.metric("🎯 Especialidades Distintas", f"{int(kpis['especialidades'].iloc[0]):,}".replace(',', '.'))
    st.divider()

    # ==========================================
    # VISUALIZAÇÕES ESTRATÉGICAS (MULTI-TABS)
    # ==========================================
    t_geral, t_loc, t_clin, t_perf, t_micro = st.tabs(["📊 Visão Geral", "🌍 Demografia & Geometria", "⚕️ Inteligência Clínica", "⏱️ Tráfego & SLA", "🔎 Raw Data"])

    with t_geral:
        c1, c2 = st.columns(2)
        with c1:
            df_sit = query_db(f"SELECT Situação, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1 ORDER BY 2 DESC")
            st.plotly_chart(px.bar(df_sit, x='Situação', y='Vol', color='Situação', title="Volume por Situação Atual", template="plotly_white"), use_container_width=True)
        with c2:
            df_lista = query_db(f"SELECT \"Origem da Lista\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1")
            st.plotly_chart(px.pie(df_lista, values='Vol', names='Origem da Lista', hole=0.4, title="Distribuição por Lista do Gercon"), use_container_width=True)

    with t_loc:
        c1, c2 = st.columns(2)
        with c1:
            df_mun = query_db(f"SELECT \"Município de Residência\", Bairro, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Município de Residência\" != '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30")
            if not df_mun.empty:
                try:
                    st.plotly_chart(px.treemap(df_mun, path=['Município de Residência', 'Bairro'], values='Vol', title="Mapa de Origem", color='Vol', color_continuous_scale='Magma'), use_container_width=True)
                except Exception:
                    st.warning("Não foi possível gerar o Treemap.")
        with c2:
            df_demo = query_db(f"SELECT Sexo, Cor, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1, 2")
            st.plotly_chart(px.bar(df_demo, x='Sexo', y='Vol', color='Cor', barmode='group', title="Perfil Demográfico"), use_container_width=True)

    with t_clin:
        df_esp = query_db(f"SELECT \"Especialidade Mãe\", Especialidade, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 40")
        if not df_esp.empty:
            try:
                st.plotly_chart(px.sunburst(df_esp, path=['Especialidade Mãe', 'Especialidade'], values='Vol', color='Vol', color_continuous_scale='Blues', title="Relação Especialidade Mãe > Fina"), use_container_width=True)
            except Exception:
                st.warning("Não foi possível gerar o Sunburst.")

        c1, c2 = st.columns(2)
        with c1:
            df_cid = query_db(f"SELECT \"CID Descrição\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"CID Descrição\" != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 15")
            fig_cid = px.bar(df_cid, x='Vol', y='CID Descrição', orientation='h', title="Top 15 Diagnósticos")
            fig_cid.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_cid, use_container_width=True)
        with c2:
            df_risco = query_db(f"SELECT \"Risco Cor\", \"Ordem Judicial\" IS NOT NULL as Jud, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Risco Cor\" != '' GROUP BY 1, 2")
            cmap = {'VERMELHO': '#ef4444', 'AMARELO': '#eab308', 'VERDE': '#22c55e', 'AZUL': '#3b82f6', 'LARANJA': '#f97316'}
            st.plotly_chart(px.bar(df_risco, x='Risco Cor', y='Vol', color='Risco Cor', pattern_shape='Jud', color_discrete_map=cmap, title="Triagem vs Ordem Judicial"), use_container_width=True)

    with t_perf:
        c1, c2 = st.columns(2)
        with c1:
            df_time = query_db(f"SELECT CAST(\"Data Solicitação\" AS DATE) as Dia, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Data Solicitação\" IS NOT NULL GROUP BY 1 ORDER BY 1")
            fig_in = px.area(df_time, x='Dia', y='Vol', title="Throughput: Criação de Protocolos", template="plotly_white")
            fig_in.update_traces(line_color='#10b981')
            st.plotly_chart(fig_in, use_container_width=True)
        with c2:
            df_evo_time = query_db(f"SELECT CAST(\"Data_Evolucao\" AS DATE) as Dia, COUNT(*) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Data_Evolucao\" IS NOT NULL GROUP BY 1 ORDER BY 1")
            fig_ev = px.line(df_evo_time, x='Dia', y='Vol', title="Velocidade de Trabalho (Evoluções Diárias)", template="plotly_white")
            fig_ev.update_traces(line_color='#8b5cf6')
            st.plotly_chart(fig_ev, use_container_width=True)
            
        df_tipo = query_db(f"SELECT Tipo_Informacao, COUNT(*) as Vol FROM gercon WHERE {FINAL_WHERE} AND Tipo_Informacao != '' GROUP BY 1 ORDER BY 2 DESC")
        fig_tipo = px.bar(df_tipo, x='Vol', y='Tipo_Informacao', orientation='h', title="O que está a ser feito no sistema?")
        fig_tipo.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_tipo, use_container_width=True)

    with t_micro:
        st.subheader("Auditoria Clínica")
        limit = st.slider("Amostra (Linhas)", 10, 500, 50)
        df_grid = query_db(f"""
            SELECT Protocolo, CAST(\"Data Solicitação\" AS DATE) as Solicitação, CAST(Data_Evolucao AS TIMESTAMP) as Data_Evolução, 
            \"Origem da Lista\", Situação, Especialidade, \"Risco Cor\", Tipo_Informacao, Origem_Informacao, Texto_Evolucao 
            FROM gercon WHERE {FINAL_WHERE} ORDER BY \"Data Solicitação\" DESC, Data_Evolucao DESC LIMIT {limit}
        """)
        st.dataframe(df_grid, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
