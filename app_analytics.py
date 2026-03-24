import os
import time
import logging
import duckdb
import streamlit as st
import pandas as pd
import plotly.express as px
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from datetime import datetime, date, timedelta

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Gercon Analytics | Observability",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

class AnalyticsSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("env/creds.env", "env/config.env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )
    OUTPUT_FILE: str = Field(default="gercon_consolidado.parquet")
    LOG_LEVEL: str = Field(default="INFO")

settings = AnalyticsSettings()

# --- INFRASTRUCTURE: DUCKDB CONNECTION ---
@st.cache_resource
def get_connection():
    con = duckdb.connect(database=':memory:')
    return con

def query_duckdb(sql_query: str):
    con = get_connection()
    # A view virtual garante que não lemos o Parquet inteiro para a RAM, apenas o necessário
    con.execute(f"CREATE OR REPLACE VIEW gercon AS SELECT * FROM read_parquet('{settings.OUTPUT_FILE}')")
    return con.execute(sql_query).df()

# Helper para queries de distinct values
@st.cache_data(ttl=3600)
def get_distinct_values(column: str) -> list:
    try:
        df = query_duckdb(f"SELECT DISTINCT \"{column}\" FROM gercon WHERE \"{column}\" IS NOT NULL AND \"{column}\" != '' ORDER BY 1")
        return df[column].tolist()
    except:
        return []

def inject_custom_css():
    st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
        .stPlotlyChart { background-color: #ffffff; border-radius: 8px; border: 1px solid #e5e7eb; padding: 10px; }
    </style>
    """, unsafe_allow_html=True)

def main():
    inject_custom_css()
    
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f"Base Parquet não encontrada em {settings.OUTPUT_FILE}. Execute o data_processor.py primeiro.")
        return

    st.title("🚀 Gercon Analytics & SRE Dashboard")
    
    # ==========================================
    # SIDEBAR: ARQUITETURA DE FILTROS DINÂMICOS
    # ==========================================
    st.sidebar.header("🔍 Filtros de Domínio")
    
    with st.sidebar.expander("📅 Filtros Temporais", expanded=True):
        hoje = date.today()
        data_inicio = st.date_input("Data de Solicitação (Início)", hoje - timedelta(days=365*2))
        data_fim = st.date_input("Data de Solicitação (Fim)", hoje)

    with st.sidebar.expander("🩺 Clínico & Triagem", expanded=True):
        f_situacao = st.multiselect("Situação", get_distinct_values("Situação"))
        f_esp_mae = st.multiselect("Especialidade Mãe", get_distinct_values("Especialidade Mãe"))
        f_esp = st.multiselect("Especialidade (Micro)", get_distinct_values("Especialidade"))
        f_risco = st.multiselect("Risco Cor", get_distinct_values("Risco Cor"))
        f_complex = st.multiselect("Complexidade", get_distinct_values("Complexidade"))
        
    with st.sidebar.expander("🌍 Demografia & Governança", expanded=False):
        f_municipio = st.multiselect("Município de Residência", get_distinct_values("Município de Residência"))
        f_sexo = st.multiselect("Sexo", get_distinct_values("Sexo"))
        # Verifica se a coluna Ordem Judicial existe (pode variar entre bases)
        cols_presentes = query_duckdb("SELECT * FROM gercon LIMIT 1").columns.tolist()
        f_judicial = st.multiselect("Ordem Judicial?", get_distinct_values("Ordem Judicial")) if "Ordem Judicial" in cols_presentes else []

    # ==========================================
    # CONSTRUÇÃO DA QUERY (DYNAMIC PUSH-DOWN)
    # ==========================================
    where_clauses = ["1=1"]
    
    # Filtros de Data
    where_clauses.append(f"CAST(\"Data Solicitação\" AS DATE) BETWEEN '{data_inicio}' AND '{data_fim}'")
    
    # Helper lambda para injetar IN clauses de forma segura
    def add_filter(col, vals):
        if vals:
             sanitized_vals = [str(v).replace("'", "''") for v in vals]
             where_clauses.append(f"\"{col}\" IN ({', '.join([f"'{v}'" for v in sanitized_vals])})")
    
    add_filter("Situação", f_situacao)
    add_filter("Especialidade Mãe", f_esp_mae)
    add_filter("Especialidade", f_esp)
    add_filter("Risco Cor", f_risco)
    add_filter("Complexidade", f_complex)
    add_filter("Município de Residência", f_municipio)
    add_filter("Sexo", f_sexo)
    if f_judicial:
        add_filter("Ordem Judicial", f_judicial)

    where_stmt = " AND ".join(where_clauses)

    # ==========================================
    # CAMADA 1: KPIs ESTRATÉGICOS
    # ==========================================
    with st.spinner("Computando Métricas..."):
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
    m1.metric("Pacientes Únicos (Demandas)", f"{int(kpis['pacientes'].iloc[0]):,}".replace(',', '.'))
    m2.metric("Eventos de Evolução Auditados", f"{int(kpis['evolucoes'].iloc[0]):,}".replace(',', '.'))
    m3.metric("Especialidades Impactadas", f"{int(kpis['especialidades'].iloc[0]):,}".replace(',', '.'))
    st.divider()

    # ==========================================
    # CAMADA 2: APRESENTAÇÃO E VISUALIZAÇÃO
    # ==========================================
    tab_clinica, tab_demog, tab_sre, tab_micro = st.tabs([
        "🩺 Clínico & Triagem", "🌍 Demografia & Rede", "📈 Tráfego & SRE", "🔎 Auditoria (Micro)"
    ])

    # --- TAB 1: CLÍNICO ---
    with tab_clinica:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Top 10 Especialidades Requisitadas")
            df_top = query_duckdb(f"SELECT Especialidade, count(DISTINCT Protocolo) as Volume FROM gercon WHERE {where_stmt} GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
            fig_top = px.bar(df_top, x='Volume', y='Especialidade', orientation='h', color='Volume', color_continuous_scale='Blues')
            fig_top.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
            st.plotly_chart(fig_top, key="chart_esp", use_container_width=True)
            
        with c2:
            st.subheader("Matriz de Triagem (Risco Cor)")
            df_risco = query_duckdb(f"SELECT \"Risco Cor\", count(DISTINCT Protocolo) as Volume FROM gercon WHERE {where_stmt} AND \"Risco Cor\" IS NOT NULL AND \"Risco Cor\" != '' GROUP BY 1 ORDER BY 2 DESC")
            color_map = {'VERMELHO': '#ef4444', 'AMARELO': '#eab308', 'VERDE': '#22c55e', 'AZUL': '#3b82f6', 'LARANJA': '#f97316'}
            fig_pie = px.pie(df_risco, names='Risco Cor', values='Volume', color='Risco Cor', color_discrete_map=color_map, hole=0.4)
            st.plotly_chart(fig_pie, key="chart_risco", use_container_width=True)

        st.subheader("Top 15 Diagnósticos Base (CID)")
        df_cid = query_duckdb(f"SELECT \"CID Descrição\", count(DISTINCT Protocolo) as Volume FROM gercon WHERE {where_stmt} AND \"CID Descrição\" IS NOT NULL AND \"CID Descrição\" != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 15")
        if not df_cid.empty:
            fig_cid = px.treemap(df_cid, path=['CID Descrição'], values='Volume', color='Volume', color_continuous_scale='Teal')
            st.plotly_chart(fig_cid, key="chart_cid", use_container_width=True)
        else:
            st.info("Dados de CID não disponíveis para este filtro.")

    # --- TAB 2: DEMOGRAFIA ---
    with tab_demog:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Origem da Demanda (Município)")
            df_mun = query_duckdb(f"SELECT \"Município de Residência\", count(DISTINCT Protocolo) as Volume FROM gercon WHERE {where_stmt} AND \"Município de Residência\" != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 15")
            fig_mun = px.bar(df_mun, x='Município de Residência', y='Volume', color='Volume', color_continuous_scale='Viridis')
            st.plotly_chart(fig_mun, key="chart_mun", use_container_width=True)
            
        with c2:
            st.subheader("Demografia (Idade Aproximada)")
            df_age = query_duckdb(f"""
                SELECT 
                    Sexo,
                    date_diff('year', CAST(\"Data de Nascimento\" AS DATE), CURRENT_DATE) as Idade 
                FROM gercon 
                WHERE {where_stmt} AND \"Data de Nascimento\" IS NOT NULL
                GROUP BY Protocolo, Sexo, \"Data de Nascimento\"
            """)
            if not df_age.empty:
                fig_age = px.histogram(df_age, x="Idade", color="Sexo", marginal="box", nbins=20, color_discrete_map={'Feminino':'#ec4899', 'Masculino':'#3b82f6'})
                st.plotly_chart(fig_age, key="chart_age", use_container_width=True)
            else:
                st.info("Dados de idade indisponíveis.")

    # --- TAB 3: TRÁFEGO E SRE ---
    with tab_sre:
        st.subheader("Throughput: Volume de Solicitações (Por Mês)")
        df_time = query_duckdb(f"""
            SELECT date_trunc('month', CAST(\"Data Solicitação\" AS DATE)) as Mes, count(DISTINCT Protocolo) as Volume 
            FROM gercon WHERE {where_stmt} GROUP BY 1 ORDER BY 1
        """)
        fig_time = px.area(df_time, x='Mes', y='Volume', template="plotly_white")
        fig_time.update_traces(line_color='#10b981')
        st.plotly_chart(fig_time, key="chart_time", use_container_width=True)

        st.subheader("Tráfego de Evoluções e Interações Clínicas (Tipos de Ação)")
        df_evo = query_duckdb(f"SELECT Tipo_Informacao, count(*) as Volume FROM gercon WHERE {where_stmt} AND Tipo_Informacao IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 10")
        fig_evo = px.bar(df_evo, x='Volume', y='Tipo_Informacao', orientation='h', color='Volume', color_continuous_scale='Purples')
        fig_evo.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_evo, key="chart_evo", use_container_width=True)

    # --- TAB 4: AUDITORIA MICRO ---
    with tab_micro:
        st.subheader("🔎 Log Operacional de Evoluções (Drill-Down)")
        limit = st.slider("Número de eventos para carregar na tabela:", 50, 1000, 100)
        
        df_micro = query_duckdb(f"""
            SELECT 
                Protocolo, 
                CAST(\"Data_Evolucao\" AS TIMESTAMP) as Data_Interacao, 
                Situação, 
                \"Risco Cor\", 
                Tipo_Informacao as Tipo, 
                Origem_Informacao as Origem,
                Texto_Evolucao as Texto 
            FROM gercon 
            WHERE {where_stmt} 
            ORDER BY \"Data_Evolucao\" DESC NULLS LAST
            LIMIT {limit}
        """)
        st.dataframe(df_micro, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
