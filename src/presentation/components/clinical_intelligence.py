import streamlit as st
import plotly.express as px
import time

try:
    from infrastructure.telemetry.metrics import RENDER_LATENCY, SILENT_ERRORS
except ImportError:
    RENDER_LATENCY, SILENT_ERRORS = None, None

from presentation.components.clinical_heatmap import render_clinical_heatmap


def render_clinical_intelligence(
    use_case,
    filters,
    FINAL_WHERE: str,
    MAPA_NOMENCLATURAS: dict
):
    """
    Renders the Clinical Intelligence & Demographic Profile
    for the Clinical tab in the application.
    """
    st.subheader("Clinical Intelligence & Demographic Profile")

    c1, c2 = st.columns(2)
    with c1:
        try:
            start_treemap = time.time()
            df_mun = use_case.execute_custom_query(
                f"SELECT usuarioSUS_municipioResidencia_nome, usuarioSUS_bairro, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND usuarioSUS_municipioResidencia_nome != '' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 30",
                spec=filters,
                current_user=st.session_state.user,
            )

            if not df_mun.empty:
                df_mun["usuarioSUS_bairro"] = (
                    df_mun["usuarioSUS_bairro"]
                    .replace("", "Not Informed")
                    .fillna("Not Informed")
                )
                st.plotly_chart(
                    px.treemap(
                        df_mun,
                        path=[
                            "usuarioSUS_municipioResidencia_nome",
                            "usuarioSUS_bairro",
                        ],
                        values="Vol",
                        title="Geometry: Municipality ➔ usuarioSUS_bairro",
                        color="Vol",
                        color_continuous_scale="Viridis",
                    ),
                    width="stretch",
                    config={"displayModeBar": False},
                )
            if RENDER_LATENCY:
                RENDER_LATENCY.labels(component="t_clin_treemap").observe(
                    time.time() - start_treemap
                )
        except Exception:
            if SILENT_ERRORS:
                SILENT_ERRORS.labels(component="t_clin_treemap").inc()
            st.warning("⚠️ Insufficient or malformed data for the Treemap.")

    with c2:
        try:
            start_hist = time.time()
            df_demo = use_case.execute_custom_query(
                f"""
                SELECT Idade_Int, usuarioSUS_sexo, COUNT(DISTINCT numeroCMCE) as Vol
                FROM (
                    SELECT 
                        date_diff('year', TRY_CAST(usuarioSUS_dataNascimento AS DATE), CURRENT_DATE) as Idade_Int, 
                        usuarioSUS_sexo, 
                        numeroCMCE
                    FROM gercon 
                    WHERE {FINAL_WHERE}
                ) 
                WHERE Idade_Int IS NOT NULL AND Idade_Int >= 0
                GROUP BY 1, 2
            """,
                filters,
                st.session_state.user,
            )

            if not df_demo.empty:
                fig_demo = px.histogram(
                    df_demo,
                    x="Idade_Int",
                    y="Vol",
                    color="usuarioSUS_sexo",
                    barmode="group",
                    color_discrete_map={
                        "Feminino": "#ec4899",
                        "Masculino": "#3b82f6",
                    },
                    title="Demographic Profile (Age vs usuarioSUS_sexo)",
                    labels={
                        "Idade_Int": "Approximate Age",
                        "Vol": "Patient Volume",
                    },
                )
                st.plotly_chart(
                    fig_demo, width="stretch", config={"displayModeBar": False}
                )
            if RENDER_LATENCY:
                RENDER_LATENCY.labels(component="t_clin_demographics").observe(
                    time.time() - start_hist
                )
        except Exception:
            if SILENT_ERRORS:
                SILENT_ERRORS.labels(component="t_clin_demographics").inc()
            st.warning("⚠️ Silent error caught in demographic rendering.")

    df_fluxo = use_case.execute_custom_query(
        f"SELECT CAST(dataSolicitacao AS DATE) as Dia, origem_lista, COUNT(DISTINCT numeroCMCE) as Vol FROM gercon WHERE {FINAL_WHERE} AND dataSolicitacao IS NOT NULL GROUP BY 1, 2 ORDER BY 1",
        spec=filters,
        current_user=st.session_state.user,
    )
    st.plotly_chart(
        px.area(
            df_fluxo,
            x="Dia",
            y="Vol",
            color="origem_lista",
            title="Temporal Throughput: Patient Volume by Source",
        ),
        width="stretch",
        config={"displayModeBar": False},
    )

    st.markdown("---")
    st.subheader("🕵️ Clinical Pattern Audit (Location / Actor × Diagnosis)")

    _OPCOES_DIAGNOSTICO = {
        "ICD — Description": "entidade_cidPrincipal_descricao",
        "ICD — Code": "entidade_cidPrincipal_codigo",
        "Parent Specialty": "entidade_especialidade_especialidadeMae_descricao",
        "Fine Specialty": "entidade_especialidade_descricao",
        "CBO (Specialty)": "entidade_especialidade_cbo_descricao",
    }
    _OPCOES_ATOR = {
        "Requesting Doctor": "medicoSolicitante",
        "Operating Unit (UBS)": "entidade_unidadeOperador_nome",
        "Operating Unit (Corporate Name)": "entidade_unidadeOperador_razaoSocial",
        "Unit Type": "entidade_unidadeOperador_tipoUnidade_descricao",
        "Regulation Center": "entidade_centralRegulacao_nome",
        "Operator (Regulator)": "operador_nome",
        "Requesting User": "usuarioSolicitante_nome",
    }

    c_dim1, c_dim2 = st.columns(2)
    with c_dim1:
        _label_ator = st.selectbox(
            "📍 X-Axis — Location / Actor:",
            options=list(_OPCOES_ATOR.keys()),
            index=0,
            help="Defines who or what location appears on the heatmap's X-axis.",
        )
    with c_dim2:
        _label_diag = st.selectbox(
            "🔬 Y-Axis — Diagnosis / Clinical Dimension:",
            options=list(_OPCOES_DIAGNOSTICO.keys()),
            index=0,
            help="Defines which clinical dimension appears on the heatmap's Y-axis.",
        )

    _col_ator = _OPCOES_ATOR[_label_ator]
    _col_diag = _OPCOES_DIAGNOSTICO[_label_diag]

    OPT_CID = "Horizontal Analysis (Peer Comparison)"
    OPT_MED = "Vertical Analysis (Individual Profile)"

    c_top1, c_top2, c_metric = st.columns([0.15, 0.15, 0.7])
    with c_top1:
        top_x_med = st.slider(
            f"Top {_label_ator.split('(')[0].strip()}:",
            min_value=5,
            max_value=100,
            value=15,
            step=1,
            help=f"Define the amount of '{_label_ator}' items on the X-axis.",
        )
    with c_top2:
        top_x_cid = st.slider(
            f"Top {_label_diag.split('(')[0].strip()}:",
            min_value=5,
            max_value=100,
            value=15,
            step=1,
            help=f"Define the amount of '{_label_diag}' items on the Y-axis.",
        )
    with c_metric:
        st.write(" ")
        modo_heatmap = st.radio(
            "Analytical Visualization Metric (Standard Deviation):",
            options=[OPT_CID, OPT_MED],
            horizontal=True,
        )

    if modo_heatmap == OPT_CID:
        st.info(
            f"💡 **Hint: Horizontal Analysis (Peer Comparison):** Evaluates the same **{_label_diag} (row)** across all '{_label_ator}'. "
            f"Warm tones (red) indicate that the actor in question has a frequency **statistically much higher than their peers' average**."
        )
    else:
        st.info(
            f"💡 **Hint: Vertical Analysis (Individual Profile):** Evaluates the routine of a single **{_label_ator} (column)** by comparing all the clinical dimensions they present. "
            f"Warm tones (red) reveal which '{_label_diag}' are anomalies that deviate from that specific actor's normal pattern."
        )

    df_math, df_pivot_vol, df_text = use_case.get_clinical_audit_heatmap(
        col_ator=_col_ator,
        col_diag=_col_diag,
        top_x_med=top_x_med,
        top_x_cid=top_x_cid,
        modo_heatmap=modo_heatmap,
        spec=filters,
        current_user=st.session_state.user,
    )

    render_clinical_heatmap(
        df_math=df_math,
        df_pivot_vol=df_pivot_vol,
        df_text=df_text,
        label_ator=_label_ator,
        label_diag=_label_diag,
        top_x_med=top_x_med,
        top_x_cid=top_x_cid,
    )

    df_perfil_med = use_case.execute_custom_query(
        f"""
        SELECT "{_col_ator}" AS _ator, "{_col_diag}" AS _diag, COUNT(DISTINCT numeroCMCE) as Vol
        FROM gercon
        WHERE {FINAL_WHERE}
            AND "{_col_ator}" != '' AND "{_col_ator}" IS NOT NULL
            AND "{_col_diag}" != '' AND "{_col_diag}" IS NOT NULL
        GROUP BY 1, 2 HAVING COUNT(DISTINCT numeroCMCE) >= 3 ORDER BY 3 DESC LIMIT 150
        """,
        filters,
        st.session_state.user,
    )

    try:
        if (
            not df_perfil_med.empty
            and "_ator" in df_perfil_med.columns
            and "_diag" in df_perfil_med.columns
        ):
            df_perfil_med["_ator"] = df_perfil_med["_ator"].replace(
                "", f"{_label_ator} Not Informed"
            )
            df_perfil_med["_diag"] = df_perfil_med["_diag"].replace(
                "", f"{_label_diag} Not Informed"
            )
            fig_tree_med = px.treemap(
                df_perfil_med,
                path=["_ator", "_diag"],
                values="Vol",
                color="Vol",
                color_continuous_scale="Teal",
                title=f"Profile: {_label_ator} ➔ {_label_diag} (Click to expand)",
            )
            fig_tree_med.update_layout(
                height=500, margin=dict(t=40, l=10, r=10, b=10)
            )
            st.plotly_chart(
                fig_tree_med, width="stretch", config={"displayModeBar": False}
            )
    except Exception:
        st.warning("⚠️ Insufficient data to generate the clinical profile treemap.")
