import os
import logging
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
    try:
        q = f"SELECT DISTINCT \"{column}\" FROM gercon WHERE {current_where} AND \"{column}\" IS NOT NULL AND \"{column}\" != '' ORDER BY 1"
        return query_db(q)[column].tolist()
    except Exception as e:
        return []

@st.cache_data(ttl=3600)
def get_global_bounds(column: str, is_date=False):
    cast = "DATE" if is_date else "INTEGER"
    try:
        df = query_db(f"SELECT MIN(TRY_CAST(\"{column}\" AS {cast})) as vmin, MAX(TRY_CAST(\"{column}\" AS {cast})) as vmax FROM gercon")
        return df['vmin'].iloc[0], df['vmax'].iloc[0]
    except:
        return None, None

# --- 3. UI COMPONENTS (DOMAIN FILTERS) ---
def render_include_exclude(label: str, column: str, clauses: list, current_where: str, key: str):
    """Filtros Categóricos (Dropdown) com Inclusão e Exclusão."""
    options = get_dynamic_options(column, current_where)
    if not options: return current_where
    
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    incl = c1.multiselect("✅ Incluir", options, key=f"{key}_in", label_visibility="collapsed", placeholder="✅ Incluir...")
    excl = c2.multiselect("❌ Excluir", options, key=f"{key}_ex", label_visibility="collapsed", placeholder="❌ Excluir...")
    
    def sanitize(v): return str(v).replace("'", "''")
    if incl: clauses.append(f"\"{column}\" IN ({', '.join([f"'{sanitize(v)}'" for v in incl])})")
    if excl: clauses.append(f"\"{column}\" NOT IN ({', '.join([f"'{sanitize(v)}'" for v in excl])})")
    
    return " AND ".join(clauses)

def render_text_search(label: str, column: str, clauses: list, current_where: str, key: str):
    """Filtros Textuais (Busca Livre) com Motor Booleano (Contém / Não Contém). Aceita CSV para múltiplos termos."""
    st.write(f"<span style='font-size: 0.9em; font-weight: 600; color: #4B5563;'>{label}</span>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    incl = c1.text_input("✅ Contém", key=f"{key}_txt_in", label_visibility="collapsed", placeholder="✅ Contém (ex: A, B)")
    excl = c2.text_input("❌ Não contém", key=f"{key}_txt_ex", label_visibility="collapsed", placeholder="❌ Sem (ex: C, D)")
    
    def sanitize(v): return str(v).replace("'", "''")
    
    # Processa Inclusões (OR - Traz se contiver A ou B)
    if incl:
        terms = [t.strip() for t in incl.split(',') if t.strip()]
        if terms:
            incl_clauses = [f"\"{column}\" ILIKE '%{sanitize(t)}%'" for t in terms]
            clauses.append(f"({' OR '.join(incl_clauses)})")
            
    # Processa Exclusões (AND - Tira se contiver A e tira se contiver B)
    if excl:
        terms = [t.strip() for t in excl.split(',') if t.strip()]
        if terms:
            excl_clauses = [f"\"{column}\" NOT ILIKE '%{sanitize(t)}%'" for t in terms]
            clauses.append(f"({' AND '.join(excl_clauses)})")
            
    return " AND ".join(clauses)

def render_range_slider(label: str, column: str, clauses: list, current_where: str, key: str):
    """Slider de ranges dinâmicos."""
    vmin, vmax = get_global_bounds(column, is_date=False)
    if vmin is not None and vmax is not None and vmin != vmax:
        val = st.slider(label, int(vmin), int(vmax), (int(vmin), int(vmax)), key=key)
        if val[0] > vmin or val[1] < vmax:
            clauses.append(f"TRY_CAST(\"{column}\" AS INTEGER) BETWEEN {val[0]} AND {val[1]}")
    return " AND ".join(clauses)

# --- 4. MAIN APP ---
def main():
    inject_custom_css()
    
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f"⚠️ Base Parquet não encontrada ({settings.OUTPUT_FILE}).")
        return

    st.title("🎯 Gercon SRE | Advanced Root Cause Analysis")
    
    clauses = ["1=1"]
    curr_where = "1=1"

    # ==========================================
    # CASCADING SIDEBAR (TOP-DOWN FLOW)
    # ==========================================
    st.sidebar.header("🎛️ Filtros em Cascata")
    st.sidebar.caption("💡 Dica: Nos campos de texto, use vírgulas para separar múltiplos termos.")

    # --- 1. CICLO DE VIDA (Datas) ---
    with st.sidebar.expander("📅 Ciclo de Vida (Datas)", expanded=False):
        dt_solic = st.date_input("Data de Solicitação", value=[], key="dt_sol_root")
        if len(dt_solic) == 2: clauses.append(f"CAST(\"Data Solicitação\" AS DATE) BETWEEN '{dt_solic[0]}' AND '{dt_solic[1]}'")
        
        dt_cad = st.date_input("Data do Cadastro", value=[], key="dt_cad_root")
        if len(dt_cad) == 2: clauses.append(f"CAST(\"Data do Cadastro\" AS DATE) BETWEEN '{dt_cad[0]}' AND '{dt_cad[1]}'")
        
        dt_evo = st.date_input("Data da Evolução", value=[], key="dt_evo_root")
        if len(dt_evo) == 2: clauses.append(f"CAST(\"Data_Evolucao\" AS DATE) BETWEEN '{dt_evo[0]}' AND '{dt_evo[1]}'")
        curr_where = " AND ".join(clauses)

    # --- 2. CLÍNICO & REGULAÇÃO ---
    with st.sidebar.expander("🩺 Clínico & Regulação", expanded=False):
        curr_where = render_include_exclude("Origem da Lista", "Origem da Lista", clauses, curr_where, "lst")
        curr_where = render_include_exclude("Situação Atual", "Situação", clauses, curr_where, "sit")
        curr_where = render_include_exclude("Situação Final", "Situação Final", clauses, curr_where, "sitf")
        curr_where = render_include_exclude("Tipo de Regulação", "Tipo de Regulação", clauses, curr_where, "treg")
        curr_where = render_include_exclude("Status da Especialidade", "Status da Especialidade", clauses, curr_where, "stesp")
        curr_where = render_include_exclude("Teleconsulta", "Teleconsulta", clauses, curr_where, "tele")
        
        st.markdown("---")
        curr_where = render_include_exclude("Especialidade Mãe", "Especialidade Mãe", clauses, curr_where, "espm")
        curr_where = render_include_exclude("Especialidade (Fina)", "Especialidade", clauses, curr_where, "espf")
        
        st.markdown("---")
        curr_where = render_text_search("Código do CID", "CID Código", clauses, curr_where, "cid_cod")
        curr_where = render_text_search("Descrição do CID", "CID Descrição", clauses, curr_where, "cid_desc")

    # --- 3. TRIAGEM & GRAVIDADE ---
    with st.sidebar.expander("⚠️ Triagem & Pontuação", expanded=False):
        curr_where = render_include_exclude("Risco Cor (Atual)", "Risco Cor", clauses, curr_where, "r_cor")
        curr_where = render_include_exclude("Cor do Regulador", "Cor Regulador", clauses, curr_where, "c_reg")
        curr_where = render_include_exclude("Complexidade", "Complexidade", clauses, curr_where, "cpx")
        
        curr_where = render_range_slider("Pontos Gravidade", "Pontos Gravidade", clauses, curr_where, "pt_grav")
        curr_where = render_range_slider("Pontos Tempo", "Pontos Tempo", clauses, curr_where, "pt_tmp")
        curr_where = render_range_slider("Pontuação Total", "Pontuação", clauses, curr_where, "pt_tot")

    # --- 4. DEMOGRAFIA E LOCALIZAÇÃO ---
    with st.sidebar.expander("🌍 Demografia & Rede", expanded=False):
        curr_where = render_include_exclude("Município de Residência", "Município de Residência", clauses, curr_where, "mun")
        curr_where = render_include_exclude("Bairro", "Bairro", clauses, curr_where, "bai")
        
        curr_where = render_text_search("Logradouro", "Logradouro", clauses, curr_where, "logr")
        
        st.write(" ")
        num_min, num_max = st.columns(2)
        v_nmin = num_min.number_input("Número Min", value=0, step=10, key="nmin_root")
        v_nmax = num_max.number_input("Número Max", value=99999, step=100, key="nmax_root")
        if v_nmin > 0 or v_nmax < 99999: clauses.append(f"TRY_CAST(\"Número\" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}")
        curr_where = " AND ".join(clauses)
        
        dt_nasc = st.date_input("Data Nascimento (Range)", value=[], key="dt_nasc_root")
        if len(dt_nasc) == 2: clauses.append(f"CAST(\"Data de Nascimento\" AS DATE) BETWEEN '{dt_nasc[0]}' AND '{dt_nasc[1]}'")
        curr_where = " AND ".join(clauses)
        
        curr_where = render_include_exclude("Nacionalidade", "Nacionalidade", clauses, curr_where, "nac")
        curr_where = render_include_exclude("Cor/Raça", "Cor", clauses, curr_where, "cor")
        curr_where = render_include_exclude("Sexo", "Sexo", clauses, curr_where, "sex")

    # --- 5. GOVERNANÇA E ATORES ---
    with st.sidebar.expander("🏛️ Governança & Atores", expanded=False):
        oj = st.radio("Ordem Judicial", ["Ambos", "Sim", "Não"], horizontal=True, key="oj_root")
        if oj == "Sim": clauses.append("(\"Ordem Judicial\" IS NOT NULL AND \"Ordem Judicial\" != '')")
        if oj == "Não": clauses.append("(\"Ordem Judicial\" IS NULL OR \"Ordem Judicial\" = '')")
        curr_where = " AND ".join(clauses)
        
        curr_where = render_include_exclude("Unidade Solicitante", "Unidade Solicitante", clauses, curr_where, "usol")
        curr_where = render_text_search("Médico Solicitante", "Médico Solicitante", clauses, curr_where, "med_sol")
        curr_where = render_text_search("Operador do Sistema", "Operador", clauses, curr_where, "op_sys")
        curr_where = render_text_search("Usuário Solicitante", "Usuário Solicitante", clauses, curr_where, "usr_sol")

    # --- 6. LOG CLÍNICO ---
    with st.sidebar.expander("📝 Logs Clínicos (Eventos)", expanded=False):
        curr_where = render_include_exclude("Tipo de Informação", "Tipo_Informacao", clauses, curr_where, "tinf")
        curr_where = render_text_search("Origem da Informação (Nome/Local)", "Origem_Informacao", clauses, curr_where, "orig_inf")
        curr_where = render_text_search("Texto do Histórico (Anamnese/Laudo)", "Texto_Evolucao", clauses, curr_where, "txt_evo")

    # ==========================================
    # CLÁUSULA FINAL PARA OS GRÁFICOS
    # ==========================================
    FINAL_WHERE = curr_where

    # --- KPIs DE TOPO ---
    with st.spinner("Processando Modelo de Leitura (OLAP)..."):
        kpis = query_db(f"""
            SELECT 
                COUNT(DISTINCT Protocolo) as pacientes,
                COUNT(*) as eventos,
                COUNT(DISTINCT Especialidade) as especialidades
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
    t_geral, t_loc, t_clin, t_perf, t_micro = st.tabs([
        "📊 Visão Geral", "🌍 Demografia & Geometria", "⚕️ Inteligência Clínica", "⏱️ Tráfego & SLA", "🔎 Raw Data"
    ])

    # 1. TAB: VISÃO GERAL
    with t_geral:
        c1, c2 = st.columns(2)
        with c1:
            df_sit = query_db(f"SELECT Situação, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1 ORDER BY 2 DESC")
            fig1 = px.bar(df_sit, x='Situação', y='Vol', color='Situação', title="Volume por Situação Atual")
            st.plotly_chart(fig1, key="plt_sit_root", use_container_width=True)
        with c2:
            df_lista = query_db(f"SELECT \"Origem da Lista\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1")
            fig2 = px.pie(df_lista, values='Vol', names='Origem da Lista', hole=0.4, title="Distribuição por Lista do Gercon")
            st.plotly_chart(fig2, key="plt_list_root", use_container_width=True)

    # 2. TAB: DEMOGRAFIA
    with t_loc:
        c1, c2 = st.columns(2)
        with c1:
            df_mun = query_db(f"SELECT \"Município de Residência\", Bairro, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Município de Residência\" != '' AND Bairro != '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30")
            if not df_mun.empty:
                try:
                    fig_tree = px.treemap(df_mun, path=['Município de Residência', 'Bairro'], values='Vol', title="Mapa de Origem (Município > Bairro)", color='Vol', color_continuous_scale='Magma')
                    st.plotly_chart(fig_tree, key="plt_tree_root", use_container_width=True)
                except Exception:
                    st.warning("Não foi possível gerar o Treemap Geográfico.")
        with c2:
            df_demo = query_db(f"SELECT Sexo, Cor, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND Sexo != '' AND Cor != '' GROUP BY 1, 2")
            fig_bar = px.bar(df_demo, x='Sexo', y='Vol', color='Cor', barmode='group', title="Perfil Demográfico (Sexo vs Cor/Raça)")
            st.plotly_chart(fig_bar, key="plt_demo_root", use_container_width=True)

    # 3. TAB: INTELIGÊNCIA CLÍNICA
    with t_clin:
        st.subheader("Concentração de Demanda Clínica")
        df_esp = query_db(f"SELECT \"Especialidade Mãe\", Especialidade, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Especialidade Mãe\" != '' AND Especialidade != '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 40")
        if not df_esp.empty:
            try:
                fig_sun = px.sunburst(df_esp, path=['Especialidade Mãe', 'Especialidade'], values='Vol', color='Vol', color_continuous_scale='Blues', title="Relação Especialidade Mãe > Fina")
                st.plotly_chart(fig_sun, key="plt_sun_root", use_container_width=True)
            except Exception:
                st.warning("Não foi possível gerar o Sunburst Clínico.")

        c1, c2 = st.columns(2)
        with c1:
            df_cid = query_db(f"SELECT \"CID Descrição\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"CID Descrição\" != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 15")
            fig_cid = px.bar(df_cid, x='Vol', y='CID Descrição', orientation='h', title="Top 15 Diagnósticos (CIDs)")
            fig_cid.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_cid, key="plt_cid_root", use_container_width=True)
        with c2:
            df_risco = query_db(f"SELECT \"Risco Cor\", \"Ordem Judicial\" IS NOT NULL as Jud, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Risco Cor\" != '' GROUP BY 1, 2")
            cmap = {'VERMELHO': '#ef4444', 'AMARELO': '#eab308', 'VERDE': '#22c55e', 'AZUL': '#3b82f6', 'LARANJA': '#f97316'}
            fig_risc = px.bar(df_risco, x='Risco Cor', y='Vol', color='Risco Cor', pattern_shape='Jud', color_discrete_map=cmap, title="Triagem vs Ordem Judicial")
            st.plotly_chart(fig_risc, key="plt_risc_root", use_container_width=True)

    # 4. TAB: TRÁFEGO E SLA (SRE)
    with t_perf:
        c1, c2 = st.columns(2)
        with c1:
            df_time = query_db(f"SELECT CAST(\"Data Solicitação\" AS DATE) as Dia, COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Data Solicitação\" IS NOT NULL GROUP BY 1 ORDER BY 1")
            fig_in = px.area(df_time, x='Dia', y='Vol', title="Throughput: Criação de Protocolos")
            fig_in.update_traces(line_color='#10b981')
            st.plotly_chart(fig_in, key="plt_tin_root", use_container_width=True)
        with c2:
            df_evo_time = query_db(f"SELECT CAST(\"Data_Evolucao\" AS DATE) as Dia, COUNT(*) as Vol FROM gercon WHERE {FINAL_WHERE} AND \"Data_Evolucao\" IS NOT NULL GROUP BY 1 ORDER BY 1")
            fig_ev = px.line(df_evo_time, x='Dia', y='Vol', title="Velocidade de Trabalho (Evoluções Diárias)")
            fig_ev.update_traces(line_color='#8b5cf6')
            st.plotly_chart(fig_ev, key="plt_tev_root", use_container_width=True)
            
        st.subheader("Eventos do Lifecycle (Atividades Operacionais)")
        df_tipo = query_db(f"SELECT Tipo_Informacao, COUNT(*) as Vol FROM gercon WHERE {FINAL_WHERE} AND Tipo_Informacao != '' GROUP BY 1 ORDER BY 2 DESC")
        fig_tipo = px.bar(df_tipo, x='Vol', y='Tipo_Informacao', orientation='h', title="O que está a ser feito no sistema?")
        fig_tipo.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_tipo, key="plt_life_root", use_container_width=True)

    # 5. TAB: DATA GRID (MICRO)
    with t_micro:
        st.subheader("Auditoria Clínica")
        limit = st.slider("Amostra (Linhas)", 10, 500, 50, key="sld_limit_root")
        df_grid = query_db(f"""
            SELECT Protocolo, CAST(\"Data Solicitação\" AS DATE) as Solicitação, CAST(Data_Evolucao AS TIMESTAMP) as Data_Evolução, 
            \"Origem da Lista\", Situação, Especialidade, \"Risco Cor\", Tipo_Informacao, Origem_Informacao, Texto_Evolucao 
            FROM gercon WHERE {FINAL_WHERE} ORDER BY \"Data Solicitação\" DESC, Data_Evolucao DESC LIMIT {limit}
        """)
        st.dataframe(df_grid, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
