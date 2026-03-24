import os
import time
import logging
import duckdb
import streamlit as st
import pandas as pd
import plotly.express as px
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Gercon Analytics | Arquitetura SOTA",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- INFRASTRUCTURE: CONFIGURATION ---
class AnalyticsSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("env/creds.env", "env/config.env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )
    # Agora apontamos para o formato colunar Parquet
    OUTPUT_FILE: str = Field(default="gercon_consolidado.parquet")
    LOG_LEVEL: str = Field(default="INFO")

settings = AnalyticsSettings()

# --- INFRASTRUCTURE: DUCKDB CONNECTION (Cache Resource) ---
@st.cache_resource
def get_connection():
    con = duckdb.connect(database=':memory:')
    return con

def query_duckdb(sql_query: str):
    con = get_connection()
    con.execute(f"CREATE OR REPLACE VIEW gercon AS SELECT * FROM read_parquet('{settings.OUTPUT_FILE}')")
    return con.execute(sql_query).df()

# --- UI: STYLE INJECTION ---
def inject_custom_css():
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    </style>
    """, unsafe_allow_html=True)

# --- MAIN APPLICATION ---
def main():
    inject_custom_css()
    
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f"Base Parquet não encontrada em {settings.OUTPUT_FILE}. Execute o data_processor.py primeiro.")
        return

    st.title("🚀 Gercon SOTA Analytics")
    st.caption("Arquitetura CQRS: DuckDB Engine + Parquet Storage")

    # --- SIDEBAR: FILTROS DINÂMICOS (QUERY MODEL) ---
    st.sidebar.header("Filtros Estratégicos")
    
    origens = query_duckdb("SELECT DISTINCT \"Origem da Lista\" FROM gercon WHERE \"Origem da Lista\" IS NOT NULL ORDER BY 1")
    especialidades = query_duckdb("SELECT DISTINCT Especialidade FROM gercon WHERE Especialidade IS NOT NULL ORDER BY 1")
    
    selected_origens = st.sidebar.multiselect("Origem:", origens["Origem da Lista"].tolist())
    selected_esps = st.sidebar.multiselect("Especialidade:", especialidades["Especialidade"].tolist())
    
    where_clauses = ["1=1"]
    if selected_origens:
        where_clauses.append(f"\"Origem da Lista\" IN ({', '.join([f"'{o}'" for o in selected_origens])})")
    if selected_esps:
        where_clauses.append(f"Especialidade IN ({', '.join([f"'{e}'" for e in selected_esps])})")
    
    where_stmt = " AND ".join(where_clauses)

    # --- CAMADA 1: KPIs ---
    with st.spinner("DuckDB processando agregados..."):
        kpi_query = f"""
            SELECT 
                count(*) as total_eventos,
                count(DISTINCT Protocolo) as total_pacientes,
                count(DISTINCT Especialidade) as total_esps
            FROM gercon 
            WHERE {where_stmt}
        """
        kpis = query_duckdb(kpi_query)

    m1, m2, m3 = st.columns(3)
    m1.metric("Eventos (Evoluções)", f"{int(kpis['total_eventos'].iloc[0]):,}".replace(',', '.'))
    m2.metric("Pacientes Únicos", f"{int(kpis['total_pacientes'].iloc[0]):,}".replace(',', '.'))
    m3.metric("Especialidades", f"{int(kpis['total_esps'].iloc[0]):,}".replace(',', '.'))

    st.divider()

    # --- CAMADA 2: VISÃO MACRO & DRILL-DOWN ---
    tab_dist, tab_time, tab_micro = st.tabs(["📊 Distribuição", "📈 Tempo", "🔎 Visão Micro (Audit)"])

    with tab_dist:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Top Especialidades (Volume de Pacientes)**")
            df_top = query_duckdb(f"SELECT Especialidade, count(DISTINCT Protocolo) as qtde FROM gercon WHERE {where_stmt} GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
            fig_top = px.bar(df_top, x='qtde', y='Especialidade', orientation='h', color='qtde', color_continuous_scale='Blues')
            fig_top.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
            st.plotly_chart(fig_top, use_container_width=True)
        with col_b:
            st.markdown("**Distribuição por Risco Cor**")
            df_risco = query_duckdb(f"SELECT \"Risco Cor\", count(DISTINCT Protocolo) as qtde FROM gercon WHERE {where_stmt} AND \"Risco Cor\" IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")
            color_map = {'VERMELHO': '#ef4444', 'AMARELO': '#eab308', 'VERDE': '#22c55e', 'AZUL': '#3b82f6'}
            fig_pie = px.pie(df_risco, names='Risco Cor', values='qtde', color='Risco Cor', color_discrete_map=color_map, hole=0.4)
            st.plotly_chart(fig_pie, use_container_width=True)

    with tab_time:
        st.markdown("**Volume de Demandas por Mês de Solicitação**")
        df_time = query_duckdb(f"SELECT date_trunc('month', CAST(\"Data Solicitação\" AS DATE)) as mes, count(DISTINCT Protocolo) as qtde FROM gercon WHERE {where_stmt} AND \"Data Solicitação\" IS NOT NULL GROUP BY 1 ORDER BY 1")
        fig_time = px.area(df_time, x='mes', y='qtde', title="Tendência Temporal")
        st.plotly_chart(fig_time, use_container_width=True)

    with tab_micro:
        st.markdown("**🔎 Auditoria Operacional (Amostra de Registros)**")
        limit = st.slider("Número de registros para auditar:", 10, 500, 50)
        df_micro = query_duckdb(f"SELECT Protocolo, CAST(\"Data_Evolucao\" AS DATE) as Data, Tipo_Informacao as Tipo, Texto_Evolucao as Texto FROM gercon WHERE {where_stmt} ORDER BY \"Data_Evolucao\" DESC LIMIT {limit}")
        st.dataframe(df_micro, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
