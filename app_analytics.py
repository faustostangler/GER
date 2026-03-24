\"\"\"
Dashboard Tático e Estratégico do Gercon
Implementação usando Streamlit + Plotly, focado em alta performance (Cache) e DDD.
\"\"\"
import os
import time
import logging
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# --- CONFIGURAÇÃO DA PÁGINA (Deve ser o primeiro comando Streamlit) ---
st.set_page_config(
    page_title=\"Gercon Analytics | Central de Regulação\",
    page_icon=\"📊\",
    layout=\"wide\",
    initial_sidebar_state=\"expanded\"
)

# --- INFRASTRUCTURE: CONFIGURATION ---
class AnalyticsSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(\"env/creds.env\", \"env/config.env\"),
        env_file_encoding=\"utf-8\",
        extra=\"ignore\"
    )
    OUTPUT_FILE: str = Field(default=\"gercon_consolidado.csv\")
    LOG_LEVEL: str = Field(default=\"INFO\")

settings = AnalyticsSettings()

# Configuração de Log simplificada para a UI (stdout)
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper()), format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(\"GerconAnalytics\")

# --- UI: CUSTOM STYLING (Aesthetics & DX) ---
def inject_custom_css():
    st.markdown(\"\"\"
    <style>
        /* Importação de fonte moderna (Inter) */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        
        html, body, [class*=\"css\"]  {
            font-family: 'Inter', sans-serif;
        }
        
        /* Estilização dos Métricas (KPI Cards) */
        div[data-testid=\"stMetricValue\"] {
            font-size: 2.5rem;
            font-weight: 700;
            color: #1E88E5; /* Azul moderno */
        }
        div[data-testid=\"stMetricLabel\"] {
            font-size: 1rem;
            font-weight: 500;
            color: #757575;
        }
        
        /* Suavizando as bordas e fundos dos gráficos */
        .stPlotlyChart {
            background-color: #f8f9fa;
            border-radius: 12px;
            padding: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }
    </style>
    \"\"\", unsafe_allow_html=True)

# --- INFRASTRUCTURE: DATA ACCESSS (Caching) ---
# A anotação @st.cache_data garante que o CSV pesado (1.3GB) seja carregado e processado apenas 1 vez (O(1)).
@st.cache_data(show_spinner=False)
def load_and_prepare_data() -> pd.DataFrame:
    start_time = time.time()
    
    if not os.path.exists(settings.OUTPUT_FILE):
        st.error(f\"Arquivo base não encontrado: {settings.OUTPUT_FILE}\")
        return pd.DataFrame()
        
    try:
        # Carregamento Otimizado (Limitando colunas para a UI inicial economizar RAM)
        colunas_necessarias = [
            'Protocolo', 'Situação', 'Origem da Lista', 'Complexidade', 
            'Risco Cor', 'Especialidade', 'Data Solicitação', 'Data_Evolucao'
        ]
        
        df = pd.read_csv(
            settings.OUTPUT_FILE, 
            usecols=colunas_necessarias,
            parse_dates=['Data Solicitação', 'Data_Evolucao']
        )
        
        # Categorização para performance e filtros
        cat_cols = ['Situação', 'Origem da Lista', 'Complexidade', 'Risco Cor', 'Especialidade']
        for col in cat_cols:
            df[col] = df[col].astype('category')
            
        load_time = time.time() - start_time
        logger.info(f\"[UI Performance] Dados carregados (Memória RAM otimizada) em {load_time:.2f}s\")
        return df
        
    except Exception as e:
        st.error(f\"Erro ao carregar dados consolidados: {e}\")
        return pd.DataFrame()

# --- PRESENTATION: MAIN APP ---
def main():
    inject_custom_css()
    
    with st.spinner(\"Carregando e indexando dados do Gercon (Processo na memória)...\"):
        df_raw = load_and_prepare_data()

    if df_raw.empty:
        st.warning(\"Base de dados vazia. Execute o Data Processor primeiro.\")
        return

    st.title(\"Gercon Analytics Dashboard\")
    st.markdown(\"Visão Executiva e Estratégica da fila de regulação do estado.\")

    # --- SIDEBAR: FILTROS GLOBAIS (Drill-down) ---
    st.sidebar.header(\"Filtros de Domínio\")
    
    # Filtro Primário
    origens_disponiveis = df_raw['Origem da Lista'].dropna().unique().tolist()
    filtro_origem = st.sidebar.multiselect(
        \"Origem da Regulação:\", 
        origens_disponiveis, 
        default=origens_disponiveis if origens_disponiveis else None
    )
    
    # Filtro Secundário
    especialidades = df_raw['Especialidade'].dropna().unique().tolist()
    filtro_esp = st.sidebar.multiselect(
        \"Especialidade:\", 
        especialidades,
        placeholder=\"Todas (Selecione para focar)\"
    )

    # Lógica de Filtragem (Fail-fast chain)
    df_filtered = df_raw.copy()
    if filtro_origem:
        df_filtered = df_filtered[df_filtered['Origem da Lista'].isin(filtro_origem)]
    if filtro_esp:
        df_filtered = df_filtered[df_filtered['Especialidade'].isin(filtro_esp)]

    # --- CAMADA 1: VISÃO MACRO (KPIs) ---
    st.markdown(\"### Golden Signals (Volume Atual)\")
    
    col1, col2, col3 = st.columns(3)
    total_registros = len(df_filtered)
    
    # Agrupamos por Protocolo Único para contagem precisa de pacientes, e não apenas de ocorrências de evolução.
    pacientes_unicos = df_filtered['Protocolo'].nunique()
    
    # Regras de Negócio (Ubiquitous Language)
    fila_espera_mask = df_filtered['Origem da Lista'].str.contains('Fila', case=False, na=False)
    pacientes_na_fila = df_filtered[fila_espera_mask]['Protocolo'].nunique()
    
    col1.metric(\"Evoluções Registradas (Total)\", f\"{total_registros:,}\".replace(',', '.'))
    col2.metric(\"Pacientes Únicos Impactados\", f\"{pacientes_unicos:,}\".replace(',', '.'))
    col3.metric(\"Retenção (Na Fila de Espera)\", f\"{pacientes_na_fila:,}\".replace(',', '.'))

    st.divider()

    # --- CAMADA 2: GRÁFICOS INTERATIVOS ---
    tab1, tab2 = st.tabs([\"Distribuição e Risco\", \"Evolução Temporal\"])
    
    with tab1:
        st.markdown(\"#### Top Especialidades com Maior Volume""")
        # Agrupa pelo volume de pacientes (Protocolos únicos)
        df_bar = df_filtered.groupby('Especialidade')['Protocolo'].nunique().reset_index()
        df_bar = df_bar.sort_values('Protocolo', ascending=False).head(10)
        
        fig_bar = px.bar(
            df_bar, 
            y='Especialidade', 
            x='Protocolo', 
            orientation='h',
            title=\"As 10 Especialidades mais congestionadas\",
            labels={'Protocolo': 'Nº de Pacientes', 'Especialidade': ''},
            color='Protocolo',
            color_continuous_scale=px.colors.sequential.Blues
        )
        # Limpa o design e inverte o Y para o maior ficar no topo
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_bar, use_container_width=True)
        
        st.markdown(\"#### Classificação de Risco (Sunburst)\")
        # Visualização de Risco Cor
        df_sun = df_filtered.groupby(['Origem da Lista', 'Risco Cor'])['Protocolo'].nunique().reset_index()
        df_sun = df_sun[df_sun['Protocolo'] > 0] # Remove zeros
        
        # Mapeamento estrito de cores do negócio para as fatias do gráfico
        color_map = {
            'VERMELHO': '#ef4444',
            'AMARELO': '#eab308',
            'VERDE': '#22c55e',
            'AZUL': '#3b82f6',
            'BRANCO': '#e5e7eb',
            '(?)': '#9ca3af'
        }
        
        fig_sun = px.sunburst(
            df_sun, 
            path=['Origem da Lista', 'Risco Cor'], 
            values='Protocolo',
            color='Risco Cor',
            color_discrete_map=color_map,
            title=\"Distribuição de Pacientes por Lista e Risco Cor\"
        )
        fig_sun.update_layout(margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_sun, use_container_width=True)

    with tab2:
        st.markdown(\"#### Volume de Solicitações ao Longo do Tempo""")
        # Resampling por Semana (W) baseado na Data de Solicitação
        df_temp = df_filtered.copy()
        df_temp['Data Solicitação'] = pd.to_datetime(df_temp['Data Solicitação']).dt.normalize()
        df_time = df_temp.groupby('Data Solicitação')['Protocolo'].nunique().reset_index()
        df_time = df_time.sort_values('Data Solicitação')
        
        fig_line = px.line(
            df_time, 
            x='Data Solicitação', 
            y='Protocolo',
            title=\"Curva de Entrada de Protocolos por Dia\",
            labels={'Protocolo': 'Novos Protocolos', 'Data Solicitação': ''}
        )
        fig_line.update_traces(line_color='#1E88E5', line_width=2)
        fig_line.update_layout(margin=dict(l=0, r=0, t=40, b=0), xaxis_rangeslider_visible=True)
        st.plotly_chart(fig_line, use_container_width=True)

if __name__ == \"__main__\":
    main()
