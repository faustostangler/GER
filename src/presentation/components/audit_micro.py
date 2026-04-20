import streamlit as st
import plotly.express as px
from datetime import date

def render_audit_micro(
    use_case,
    filters,
    MAPA_CORES_RISCO: dict
):
    """
    Renders the Audit Micro tab for outliers, top offenders, and clinical logs.

    WHY: FINAL_WHERE was removed from this signature (ADR-004 / Hexagonal Architecture).
    The SQL placeholder {{FINAL_WHERE}} inside every custom query is resolved by
    DuckDBCriteriaTranslator inside execute_custom_query — the ONLY infrastructure
    layer permitted to know about SQL syntax.
    """
    st.subheader("Audit of Outliers & Top Offenders (SRE)")

    c1, c2 = st.columns([0.7, 0.3])
    with c1:
        st.markdown("### 🔍 SLA Outlier Detection")
        df_outliers = use_case.execute_custom_query(
            f"""
            SELECT numeroCMCE, entidade_classificacaoRisco_cor, TRY_CAST(entidade_classificacaoRisco_totalPontos AS INTEGER) as Pontos, 
                DATEDIFF('day', CAST(dataSolicitacao AS DATE), CURRENT_DATE) as DiasFila,
                situacao, entidade_especialidade_descricao
            FROM gercon 
            WHERE {{FINAL_WHERE}} AND dataSolicitacao IS NOT NULL AND situacao NOT ILIKE '%ENCERRADA%'
            ORDER BY DiasFila DESC, Pontos DESC
            LIMIT 3000
        """,
            filters,
            st.session_state.user,
        )
        try:
            if (
                not df_outliers.empty
                and "DiasFila" in df_outliers.columns
                and "Pontos" in df_outliers.columns
            ):
                df_outliers["entidade_classificacaoRisco_cor"] = (
                    df_outliers["entidade_classificacaoRisco_cor"]
                    .replace("", "Not Informed")
                    .fillna("Not Informed")
                )

                fig_out = px.scatter(
                    df_outliers,
                    x="DiasFila",
                    y="Pontos",
                    color="entidade_classificacaoRisco_cor",
                    color_discrete_map=MAPA_CORES_RISCO,
                    opacity=0.7,
                    size="Pontos",
                    hover_data=["numeroCMCE"],
                    title="Outlier Detection: Queue Time vs Severity",
                    labels={
                        "DiasFila": "Wait Time (Days)",
                        "Pontos": "Severity Points",
                    },
                    render_mode="svg",
                )
                fig_out.add_hline(
                    y=40, line_dash="dot", annotation_text="High Severity"
                )
                fig_out.add_vline(
                    x=180, line_dash="dot", annotation_text="180 d SLA"
                )
                st.plotly_chart(
                    fig_out, width="stretch", config={"displayModeBar": False}
                )
        except Exception:
            st.warning("⚠️ Insufficient data for configuring outliers scatter.")

    with c2:
        st.markdown("### ⚖️ Top Offenders")
        df_medico = use_case.execute_custom_query(
            f"SELECT medicoSolicitante, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {{FINAL_WHERE}} AND medicoSolicitante != '' GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
            spec=filters,
            current_user=st.session_state.user,
        )
        try:
            if not df_medico.empty and "medicoSolicitante" in df_medico.columns:
                fig_ofensor = px.bar(
                    df_medico,
                    x="Vol",
                    y="medicoSolicitante",
                    orientation="h",
                    title="Top 10 Doctors (Volume)",
                )
                fig_ofensor.update_layout(
                    yaxis={"categoryorder": "total ascending"}, height=450
                )
                st.plotly_chart(
                    fig_ofensor, width="stretch", config={"displayModeBar": False}
                )
        except Exception:
            st.warning("⚠️ Insufficient data for doctors ranking.")

    st.markdown("---")
    st.markdown("### 📝 Clinical Evolutions Log")

    c_slider, c_export = st.columns([0.8, 0.2])
    with c_slider:
        limit = st.slider("Clinical Audit Sample", 10, 1000, 100)

    df_audit = use_case.execute_custom_query(
        f"""
        SELECT numeroCMCE, CAST(dataSolicitacao AS DATE) as Solicitação, CAST(dataCadastro AS TIMESTAMP) as Data_Evolução, 
        situacao, entidade_classificacaoRisco_cor as "Risco Cor", historico_quadro_clinico 
        FROM gercon WHERE {{FINAL_WHERE}} ORDER BY dataSolicitacao DESC, dataCadastro DESC LIMIT {limit}
    """,
        filters,
        st.session_state.user,
    )

    with c_export:
        st.write(" ")
        csv_data = df_audit.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name=f"auditoria_gercon_{date.today()}.csv",
            mime="text/csv",
            width="stretch",
        )

    st.dataframe(df_audit, width="stretch", hide_index=True)
