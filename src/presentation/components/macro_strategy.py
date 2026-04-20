import streamlit as st
import plotly.express as px

def render_macro_strategy(
    use_case,
    filters,
    MAPA_NOMENCLATURAS: dict,
    MAPA_CORES_RISCO: dict
):
    """
    Renders the Macro Strategy (Dynamic Explorer, Golden Signals, and Overviews)
    for the Strategy tab in the application.

    WHY: FINAL_WHERE was removed from this signature (ADR-004 / Hexagonal Architecture).
    Components now pass the domain FiltroAvancadoSpec (filters) directly. The SQL
    placeholder {{FINAL_WHERE}} inside every custom query is resolved by
    DuckDBCriteriaTranslator inside execute_custom_query — the ONLY layer permitted
    to know about SQL syntax.
    """
    # --- BLOCO 1: EXPLORADOR DINÂMICO SOTA (EXPLOSÃO SOLAR BIVARIADA) ---
    st.subheader(
        "📊 Dynamic Queue Explorer: Bivariate (Load vs Latency/Risk)"
    )

    st.info(
        "💡 **How to read (SRE Bivariate Chart):** \n"
        "- **Slice Size:** Represents **Load (Volume)**. Wide slices indicate many patients waiting.\n"
        "- **Slice Color:** Represents the selected **Risk/Latency** metric. Warm tones (red) reveal bottlenecks, critical patients, or advanced age groups, while cool tones (blue) indicate fast flow or low risk."
    )

    c_hier, c_metric = st.columns([0.7, 0.3])

    with c_hier:
        niveis_sunburst = st.multiselect(
            "Select Data Hierarchy (Max: 5 levels):",
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
            help="Drag and drop tags to reorder the funnel (path) of the chart.",
            format_func=lambda col: MAPA_NOMENCLATURAS.get(col, col),
        )

    with c_metric:
        st.write(" ")
        METRICAS_COR = {
            "⏳ Wait Time (Queue)": {
                "sql": "ROUND(AVG(SLA_Lead_Time_Total_Dias), 1)",
                "unit": "days",
            },
            "⚠️ Forgotten Time (No Evolution)": {
                "sql": "ROUND(AVG(SLA_Tempo_Regulador_Dias), 1)",
                "unit": "days",
            },
            "🚨 Severity Points": {
                "sql": "ROUND(AVG(entidade_classificacaoRisco_pontosGravidade), 1)",
                "unit": "pts",
            },
            "⏱️ Time Points": {
                "sql": "ROUND(AVG(entidade_classificacaoRisco_pontosTempo), 1)",
                "unit": "pts",
            },
            "🔥 Total Score": {
                "sql": "ROUND(AVG(entidade_classificacaoRisco_totalPontos), 1)",
                "unit": "pts",
            },
            "🎂 Average Age (Demographics)": {
                "sql": "ROUND(AVG(date_diff('year', TRY_CAST(usuarioSUS_dataNascimento AS DATE), CURRENT_DATE)), 1)",
                "unit": "years",
            },
        }

        cor_selecionada = st.selectbox(
            "Color Metric (Temperature):",
            options=list(METRICAS_COR.keys()),
            index=0,
            help="Defines what the color of each slice represents. The size will always be the volume of patients.",
        )

    if niveis_sunburst:
        levels_sql = ", ".join([f'"{n}"' for n in niveis_sunburst])
        sql_cor = METRICAS_COR[cor_selecionada]["sql"]
        unidade_cor = METRICAS_COR[cor_selecionada]["unit"]
        nome_metrica = cor_selecionada.split(" ", 1)[1]

        df_plot_sun = use_case.execute_custom_query(
            f"""
            SELECT 
                {levels_sql}, 
                COUNT(DISTINCT numeroCMCE) as Vol,
                {sql_cor} as Metrica_Cor
            FROM gercon
            WHERE {{FINAL_WHERE}}
            GROUP BY {levels_sql}
        """,
            filters,
            st.session_state.user,
        )

        if not df_plot_sun.empty:
            for col in niveis_sunburst:
                df_plot_sun[col] = (
                    df_plot_sun[col]
                    .replace("", "Not Informed")
                    .fillna("Not Informed")
                )

            paleta = "RdYlBu_r"

            fig_sun = px.sunburst(
                df_plot_sun,
                path=niveis_sunburst,
                values="Vol",
                color="Metrica_Cor",
                color_continuous_scale=paleta,
                title=f"Bivariate Analysis: Size (Load) vs Color ({nome_metrica})",
                labels={"Vol": "Patients", "Metrica_Cor": nome_metrica},
            )

            fig_sun.update_traces(
                hovertemplate=f"<b>%{{label}}</b><br>Patients (Load): %{{value}}<br>{nome_metrica}: %{{color}} {unidade_cor}<extra></extra>",
                marker=dict(line=dict(width=0)),
            )

            fig_sun.update_layout(height=700, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(
                fig_sun, width="stretch", config={"displayModeBar": False}
            )
        else:
            st.warning(
                "⚠️ No data available for the Sunburst with the current filters."
            )
    else:
        st.warning("⚠️ Select at least 1 level to render the chart.")

    st.markdown("---")
    st.subheader("⏱️ Golden Signals: Governance and Flow Health")
    c1, c2 = st.columns([0.4, 0.6])

    with c1:
        df_risco = use_case.execute_custom_query(
            "SELECT entidade_classificacaoRisco_cor, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND entidade_classificacaoRisco_cor != '' GROUP BY 1",
            spec=filters,
            current_user=st.session_state.user,
        )
        if not df_risco.empty:
            st.plotly_chart(
                px.pie(
                    df_risco,
                    values="Vol",
                    names="entidade_classificacaoRisco_cor",
                    hole=0.5,
                    color="entidade_classificacaoRisco_cor",
                    color_discrete_map=MAPA_CORES_RISCO,
                    title="Risk Matrix (Priority)",
                ),
                width="stretch",
                config={"displayModeBar": False},
            )

    with c2:
        df_funil = use_case.execute_custom_query(
            """
            SELECT '1. Requested' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE}
            UNION ALL
            SELECT '2. Triage' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND entidade_classificacaoRisco_cor != ''
            UNION ALL
            SELECT '3. Scheduled' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND situacao ILIKE '%AGENDADA%'
            UNION ALL
            SELECT '4. Accomplished' as Etapa, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND (situacao ILIKE '%ATENDIDO%' OR situacao ILIKE '%REALIZADO%')
        """,
            filters,
            st.session_state.user,
        )
        st.plotly_chart(
            px.funnel(
                df_funil,
                x="Vol",
                y="Etapa",
                title="Journey Funnel: Bottlenecks and Abandonment",
            ),
            width="stretch",
            config={"displayModeBar": False},
        )

    df_sit = use_case.execute_custom_query(
        "SELECT situacao, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} GROUP BY 1 ORDER BY 2 DESC",
        spec=filters,
        current_user=st.session_state.user,
    )
    st.plotly_chart(
        px.bar(
            df_sit,
            x="situacao",
            y="Vol",
            title="Overall Network Situation",
            color="situacao",
            template="plotly_white",
        ),
        width="stretch",
        config={"displayModeBar": False},
    )
