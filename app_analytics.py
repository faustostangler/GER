import os
import logging
import duckdb
import streamlit as st
import pandas as pd
import plotly.express as px
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import date, timedelta

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Gercon Analytics | Observability", page_icon="🚀", layout="wide", initial_sidebar_state="expanded")

class AnalyticsSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=("env/creds.env", "env/config.env"), env_file_encoding="utf-8", extra="ignore")
    OUTPUT_FILE: str = Field(default="gercon_consolidado.parquet")
    LOG_LEVEL: str = Field(default="INFO")

settings = AnalyticsSettings()

# --- INFRASTRUCTURE: DUCKDB CONNECTION ---
@st.cache_resource
def get_connection():
    con = duckdb.connect(database=':memory:')
    return con

def query_duckdb(sql_query: str) -> pd.DataFrame:
    con = get_connection()
    # A view virtual garante que não lemos o Parquet inteiro para a RAM, apenas o necessário
    con.execute(f"CREATE OR REPLACE VIEW gercon AS SELECT * FROM read_parquet('{settings.OUTPUT_FILE}')")
    return con.execute(sql_query).df()

@st.cache_data(ttl=3600)
def get_distinct_values(column: str) -> list:
    try:
        df = query_duckdb(f"SELECT DISTINCT \"{column}\" FROM gercon WHERE \"{column}\" IS NOT NULL AND \"{column}\" != '' ORDER BY 1")
        return df[column].tolist()
    except:
        return []

@st.cache_data(ttl=3600)
def get_min_max(column: str, is_date=False):
    try:
        cast_type = "DATE" if is_date else "INTEGER"
        df = query_duckdb(f"SELECT MIN(TRY_CAST(\"{column}\" AS {cast_type})) as v_min, MAX(TRY_CAST(\"{column}\" AS {cast_type})) as v_max FROM gercon")
        return df['v_min'].iloc[0], df['v_max'].iloc[0]
    except:
        return None, None

def inject_custom_css():
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
        .stPlotlyChart { background-color: #ffffff; border-radius: 8px; border: 1px solid #e5e7eb; padding: 10px; }
    </style>
    """, unsafe_allow_html=True)

# --- UI COMPONENTS ---
def include_exclude_filter(label: str, options: list, key_prefix: str):
    """Componente que permite selecionar Inclusões e Exclusões para a mesma variável"""
    st.write(f"**{label}**")
    col1, col2 = st.columns(2)
    with col1:
        incl = st.multiselect("✅ Incluir", options, key=f"{key_prefix}_incl")
    with col2:
        excl = st.multiselect("❌ Excluir", options, key=f"{key_prefix}_excl")
    return incl, excl

def main():
    inject_custom_css()
    
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f"Base Parquet não encontrada em {settings.OUTPUT_FILE}. Execute o pipeline primeiro.")
        return

    st.title("🚀 Gercon Analytics & SRE Dashboard")
    
    # ==========================================
    # SIDEBAR: ARQUITETURA DE FILTROS DINÂMICOS
    # ==========================================
    st.sidebar.header("🔍 Filtros de Domínio")
    where_clauses = ["1=1"] # Base condition
    
    # 1. CICLO DE VIDA (Datas)
    with st.sidebar.expander("📅 Ciclo de Vida (Datas)", expanded=False):
        dt_solic = st.date_input("Data de Solicitação", value=[])
        if len(dt_solic) == 2:
            where_clauses.append(f"CAST(\"Data Solicitação\" AS DATE) BETWEEN '{dt_solic[0]}' AND '{dt_solic[1]}'")

        dt_cad = st.date_input("Data do Cadastro", value=[])
        if len(dt_cad) == 2:
            where_clauses.append(f"CAST(\"Data do Cadastro\" AS DATE) BETWEEN '{dt_cad[0]}' AND '{dt_cad[1]}'")
            
        dt_evo = st.date_input("Data da Evolução", value=[])
        if len(dt_evo) == 2:
            where_clauses.append(f"CAST(\"Data_Evolucao\" AS DATE) BETWEEN '{dt_evo[0]}' AND '{dt_evo[1]}'")

    # 2. CLÍNICO & REGULAÇÃO
    with st.sidebar.expander("🩺 Clínico & Regulação", expanded=False):
        f_origem_lista, fx_origem_lista = include_exclude_filter("Origem da Lista", get_distinct_values("Origem da Lista"), "orig")
        f_sit, fx_sit = include_exclude_filter("Situação", get_distinct_values("Situação"), "sit")
        f_sit_fim, fx_sit_fim = include_exclude_filter("Situação Final", get_distinct_values("Situação Final"), "sit_fim")
        
        f_reg = st.multiselect("Tipo de Regulação", get_distinct_values("Tipo de Regulação"))
        f_tele = st.multiselect("Teleconsulta", get_distinct_values("Teleconsulta"))
        f_status_esp = st.multiselect("Status da Especialidade", get_distinct_values("Status da Especialidade"))
        
        f_esp_mae, fx_esp_mae = include_exclude_filter("Especialidade Mãe", get_distinct_values("Especialidade Mãe"), "esp_mae")
        f_esp, fx_esp = include_exclude_filter("Especialidade (Micro)", get_distinct_values("Especialidade"), "esp")
        
        cid_cod = st.text_input("CID Código (Pesquisa parcial)")
        cid_desc = st.text_input("CID Descrição (Palavra-chave)")

    # 3. TRIAGEM & GRAVIDADE
    with st.sidebar.expander("⚠️ Triagem & Gravidade", expanded=False):
        f_risco, fx_risco = include_exclude_filter("Risco Cor", get_distinct_values("Risco Cor"), "risco")
        f_cor_reg = st.multiselect("Cor Regulador", get_distinct_values("Cor Regulador"))
        f_complex = st.multiselect("Complexidade", get_distinct_values("Complexidade"))
        
        # Ranges
        pt_grav_min, pt_grav_max = get_min_max("Pontos Gravidade")
        if pt_grav_min is not None:
            pt_grav = st.slider("Pontos Gravidade", int(pt_grav_min), int(pt_grav_max), (int(pt_grav_min), int(pt_grav_max)))
            where_clauses.append(f"TRY_CAST(\"Pontos Gravidade\" AS INTEGER) BETWEEN {pt_grav[0]} AND {pt_grav[1]}")
            
        pt_tot_min, pt_tot_max = get_min_max("Pontuação")
        if pt_tot_min is not None:
            pt_tot = st.slider("Pontuação Total", int(pt_tot_min), int(pt_tot_max), (int(pt_tot_min), int(pt_tot_max)))
            where_clauses.append(f"TRY_CAST(\"Pontuação\" AS INTEGER) BETWEEN {pt_tot[0]} AND {pt_tot[1]}")

    # 4. DEMOGRAFIA E LOCALIZAÇÃO
    with st.sidebar.expander("🌍 Demografia & Localização", expanded=False):
        f_mun, fx_mun = include_exclude_filter("Município de Residência", get_distinct_values("Município de Residência"), "mun")
        f_bairro = st.multiselect("Bairro", get_distinct_values("Bairro"))
        
        logradouro = st.text_input("Logradouro (Palavra-chave)")
        num_min, num_max = st.columns(2)
        v_nmin = num_min.number_input("Nº Min", value=0)
        v_nmax = num_max.number_input("Nº Max", value=99999)
        if v_nmin > 0 or v_nmax < 99999:
            where_clauses.append(f"TRY_CAST(\"Número\" AS INTEGER) BETWEEN {v_nmin} AND {v_nmax}")
            
        dt_nasc = st.date_input("Data Nascimento (Range)", value=[])
        if len(dt_nasc) == 2:
            where_clauses.append(f"CAST(\"Data de Nascimento\" AS DATE) BETWEEN '{dt_nasc[0]}' AND '{dt_nasc[1]}'")
            
        f_cor = st.multiselect("Cor/Raça", get_distinct_values("Cor"))
        f_sexo = st.multiselect("Sexo", get_distinct_values("Sexo"))
        f_nac = st.multiselect("Nacionalidade", get_distinct_values("Nacionalidade"))

    # 5. GOVERNANÇA & OPERAÇÃO
    with st.sidebar.expander("🏛️ Governança & Operação", expanded=False):
        oj_status = st.radio("Ordem Judicial?", ["Todos", "Sim", "Não"], horizontal=True)
        if oj_status == "Sim":
            where_clauses.append("(\"Ordem Judicial\" IS NOT NULL AND \"Ordem Judicial\" != '')")
        elif oj_status == "Não":
            where_clauses.append("(\"Ordem Judicial\" IS NULL OR \"Ordem Judicial\" = '')")
            
        f_usol, fx_usol = include_exclude_filter("Unidade Solicitante", get_distinct_values("Unidade Solicitante"), "usol")
        med_sol = st.text_input("Médico Solicitante (Nome)")
        operador = st.text_input("Operador do Sistema (Nome)")
        user_sol = st.text_input("Usuário Solicitante (Nome)")

    # 6. LOG CLÍNICO (EVOLUÇÃO)
    with st.sidebar.expander("📝 Log Clínico (Evolução)", expanded=False):
        f_tipo_info = st.multiselect("Tipo de Informação", get_distinct_values("Tipo_Informacao"))
        f_orig_info = st.text_input("Origem da Informação (Nome/Unidade)")
        txt_evo = st.text_input("Palavra-chave no Texto da Evolução (Busca em Log)")

    # ==========================================
    # BUILDER DO SQL DINÂMICO
    # ==========================================
    def add_filter(col, incl, excl=None):
        def sanitize(v): return str(v).replace("'", "''")
        if incl: where_clauses.append(f"\"{col}\" IN ({', '.join([f"'{sanitize(v)}'" for v in incl])})")
        if excl: where_clauses.append(f"\"{col}\" NOT IN ({', '.join([f"'{sanitize(v)}'" for v in excl])})")

    add_filter("Origem da Lista", f_origem_lista, fx_origem_lista)
    add_filter("Situação", f_sit, fx_sit)
    add_filter("Situação Final", f_sit_fim, fx_sit_fim)
    add_filter("Especialidade Mãe", f_esp_mae, fx_esp_mae)
    add_filter("Especialidade", f_esp, fx_esp)
    add_filter("Risco Cor", f_risco, fx_risco)
    add_filter("Município de Residência", f_mun, fx_mun)
    add_filter("Unidade Solicitante", f_usol, fx_usol)
    
    add_filter("Tipo de Regulação", f_reg)
    add_filter("Teleconsulta", f_tele)
    add_filter("Status da Especialidade", f_status_esp)
    add_filter("Complexidade", f_complex)
    add_filter("Cor Regulador", f_cor_reg)
    add_filter("Bairro", f_bairro)
    add_filter("Cor", f_cor)
    add_filter("Sexo", f_sexo)
    add_filter("Nacionalidade", f_nac)
    add_filter("Tipo_Informacao", f_tipo_info)

    # Text Searches (ILIKE para Case-Insensitive no DuckDB)
    if cid_cod: where_clauses.append(f"\"CID Código\" ILIKE '%{cid_cod}%'")
    if cid_desc: where_clauses.append(f"\"CID Descrição\" ILIKE '%{cid_desc}%'")
    if logradouro: where_clauses.append(f"\"Logradouro\" ILIKE '%{logradouro}%'")
    if med_sol: where_clauses.append(f"\"Médico Solicitante\" ILIKE '%{med_sol}%'")
    if operador: where_clauses.append(f"\"Operador\" ILIKE '%{operador}%'")
    if user_sol: where_clauses.append(f"\"Usuário Solicitante\" ILIKE '%{user_sol}%'")
    if f_orig_info: where_clauses.append(f"\"Origem_Informacao\" ILIKE '%{f_orig_info}%'")
    if txt_evo: where_clauses.append(f"\"Texto_Evolucao\" ILIKE '%{txt_evo}%'")

    where_stmt = " AND ".join(where_clauses)

    # ==========================================
    # CAMADA 1: KPIs ESTRATÉGICOS
    # ==========================================
    with st.spinner("Aplicando cruzamentos via DuckDB..."):
        kpi_query = f"""
            SELECT 
                COUNT(DISTINCT Protocolo) as pacientes,
                COUNT(*) as evolucoes,
                COUNT(DISTINCT Especialidade) as especialidades
            FROM gercon 
            WHERE {where_stmt}
        """
        kpis = query_duckdb(kpi_query)

    m1, m2, m3 = st.columns(3)
    m1.metric("Pacientes Únicos Impactados", f"{int(kpis['pacientes'].iloc[0]):,}".replace(',', '.'))
    m2.metric("Evoluções Encontradas", f"{int(kpis['evolucoes'].iloc[0]):,}".replace(',', '.'))
    m3.metric("Especialidades Diferentes", f"{int(kpis['especialidades'].iloc[0]):,}".replace(',', '.'))
    st.divider()

    # ==========================================
    # CAMADA 2: VISUALIZAÇÃO GRÁFICA
    # ==========================================
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Visão Macro & Fluxo", "🗺️ Demografia & Diagnóstico", "🚀 SRE & Fila", "🔎 Auditoria (Micro)"])

    # TAB 1: FLUXO E SITUAÇÃO
    with tab1:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Situação vs Origem da Lista")
            df_sit = query_duckdb(f"SELECT Situação, \"Origem da Lista\", COUNT(DISTINCT Protocolo) as Vol FROM gercon WHERE {where_stmt} GROUP BY 1, 2")
            fig1 = px.bar(df_sit, x='Situação', y='Vol', color='Origem da Lista', barmode='group')
            st.plotly_chart(fig1, key="chart_sit", use_container_width=True)
            
        with c2:
            st.subheader("Classificação de Risco Cor")
            df_risco = query_duckdb(f"SELECT \"Risco Cor\", count(DISTINCT Protocolo) as Vol FROM gercon WHERE {where_stmt} AND \"Risco Cor\" != '' GROUP BY 1")
            color_map = {'VERMELHO': '#ef4444', 'AMARELO': '#eab308', 'VERDE': '#22c55e', 'AZUL': '#3b82f6', 'LARANJA': '#f97316'}
            fig2 = px.pie(df_risco, names='Risco Cor', values='Vol', color='Risco Cor', color_discrete_map=color_map, hole=0.4)
            st.plotly_chart(fig2, key="chart_risco", use_container_width=True)

        st.subheader("Top 15 Especialidades (Fila vs Capacidade)")
        df_top = query_duckdb(f"SELECT Especialidade, count(DISTINCT Protocolo) as Volume FROM gercon WHERE {where_stmt} GROUP BY 1 ORDER BY 2 DESC LIMIT 15")
        fig3 = px.bar(df_top, x='Volume', y='Especialidade', orientation='h', color='Volume', color_continuous_scale='Blues')
        fig3.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig3, key="chart_top", use_container_width=True)

    # TAB 2: DEMOGRAFIA
    with tab2:
        c3, c4 = st.columns(2)
        with c3:
            st.subheader("Top Diagnósticos Base (CID)")
            df_cid = query_duckdb(f"SELECT \"CID Descrição\", count(DISTINCT Protocolo) as Vol FROM gercon WHERE {where_stmt} AND \"CID Descrição\" IS NOT NULL AND \"CID Descrição\" != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 200")
            if not df_cid.empty:
                fig4 = px.treemap(df_cid, path=['CID Descrição'], values='Vol', color='Vol', color_continuous_scale='Teal')
                st.plotly_chart(fig4, key="chart_cid", use_container_width=True)
            else:
                st.info("Dados de CID não disponíveis para este filtro.")
        with c4:
            st.subheader("Municípios Solicitantes")
            df_mun2 = query_duckdb(f"SELECT \"Município de Residência\", count(DISTINCT Protocolo) as Vol FROM gercon WHERE {where_stmt} AND \"Município de Residência\" != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 5")
            fig5 = px.bar(df_mun2, x='Município de Residência', y='Vol', color='Vol', color_continuous_scale='Magma')
            st.plotly_chart(fig5, key="chart_mun", use_container_width=True)

    # TAB 3: TRÁFEGO E GRAVIDADE
    with tab3:
        st.subheader("Curva de Entrada de Solicitações (Throughput)")
        df_time = query_duckdb(f"SELECT CAST(\"Data Solicitação\" AS DATE) as Dia, count(DISTINCT Protocolo) as Volume FROM gercon WHERE {where_stmt} AND \"Data Solicitação\" IS NOT NULL GROUP BY 1 ORDER BY 1")
        fig6 = px.line(df_time, x='Dia', y='Volume', template="plotly_white")
        fig6.update_traces(line_color='#10b981')
        st.plotly_chart(fig6, key="chart_time", use_container_width=True)
        
        c5, c6 = st.columns(2)
        with c5:
            st.subheader("Outlier Detection (Gravidade vs Tempo na Fila)")
            df_scatter = query_duckdb(f"""
                SELECT Protocolo, MAX(TRY_CAST(\"Pontos Gravidade\" AS INTEGER)) as Gravidade, 
                MAX(TRY_CAST(\"Pontuação\" AS INTEGER)) as Pontuacao_Total 
                FROM gercon WHERE {where_stmt} GROUP BY 1 ORDER BY 3 DESC LIMIT 500
            """)
            if not df_scatter.empty:
                fig7 = px.scatter(df_scatter, x='Pontuacao_Total', y='Gravidade', hover_data=['Protocolo'], opacity=0.5, color='Gravidade')
                st.plotly_chart(fig7, key="chart_scatter", use_container_width=True)
            else:
                st.info("Dados de pontuação indisponíveis.")
        with c6:
            st.subheader("Classificação de Interações (Log)")
            df_tipo_info = query_duckdb(f"SELECT Tipo_Informacao, COUNT(*) as Vol FROM gercon WHERE {where_stmt} GROUP BY 1 ORDER BY 2 DESC")
            fig8 = px.bar(df_tipo_info, y='Tipo_Informacao', x='Vol', orientation='h', color='Vol', color_continuous_scale='Purples')
            fig8.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig8, key="chart_evo", use_container_width=True)

    # TAB 4: AUDITORIA MICRO
    with tab4:
        st.subheader("🔎 Log Operacional de Evoluções e Textos")
        limit = st.slider("Número de eventos para carregar na tabela:", 50, 2000, 100)
        
        df_micro = query_duckdb(f"""
            SELECT 
                Protocolo, 
                CAST(\"Data Solicitação\" AS TIMESTAMP) as Solicitacao,
                CAST(\"Data_Evolucao\" AS TIMESTAMP) as Evolucao,
                Situação, 
                \"Risco Cor\",
                Especialidade,
                Tipo_Informacao, 
                Origem_Informacao,
                Texto_Evolucao 
            FROM gercon 
            WHERE {where_stmt} 
            ORDER BY \"Data Solicitação\" DESC, Data_Evolucao DESC
            LIMIT {limit}
        """)
        st.dataframe(df_micro, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
