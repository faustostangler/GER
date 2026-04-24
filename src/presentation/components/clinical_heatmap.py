import streamlit as st
import plotly.express as px
import pandas as pd


def render_clinical_heatmap(
    df_math: pd.DataFrame,
    df_pivot_vol: pd.DataFrame,
    df_text: pd.DataFrame,
    label_ator: str,
    label_diag: str,
    top_x_med: int,
    top_x_cid: int,
):
    """Componente isolado de renderização do Heatmap (SOTA).

    WHY: Design System (UI) fica separado de lógica de negócios.
    Recebe os DataFrames pré-calculados e foca apenas na exibição
    visual utilizando Plotly Express e Streamlit.
    """
    if df_math.empty or df_pivot_vol.empty or df_text.empty:
        st.warning("⚠️ Insufficient data to generate the clinical audit heatmap.")
        return

    paleta_heatmap = "RdBu_r"

    fig_heat = px.imshow(
        df_math,
        aspect="auto",
        color_continuous_scale=paleta_heatmap,
        color_continuous_midpoint=0,
        title=f"Deviation Matrix (Z-Score): Top {top_x_cid} {label_diag} × Top {top_x_med} {label_ator}",
        labels=dict(x=label_ator, y=label_diag, color="Z-Score"),
    )

    fig_heat.update_traces(
        text=df_text.values,
        texttemplate="%{text}",
        customdata=df_pivot_vol.values,
        hovertemplate=(
            f"<b>{label_ator}:</b> %{{x}}<br>"
            f"<b>{label_diag}:</b> %{{y}}<br>"
            f"<b>Real Volume:</b> %{{customdata}} patients<br>"
            f"<b>Z-Score:</b> %{{text}} deviations<extra></extra>"
        ),
    )

    altura_dinamica = max(500, top_x_cid * 35)
    fig_heat.update_layout(
        xaxis_tickangle=-45,
        height=altura_dinamica,
        margin=dict(l=250, b=120),
    )
    st.plotly_chart(fig_heat, width="stretch", config={"displayModeBar": False})
