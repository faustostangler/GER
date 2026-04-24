import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from presentation.components.kpi_board import render_kpi


def get_sre_stats(df, col):
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    iqr = q3 - q1
    # Limpeza para escala (3.0x IQR)
    df_clean = df[df[col] <= (q3 + 3.0 * iqr)]
    # Cálculos de Percentis (Decile Border)
    p10 = df[col].quantile(0.10)
    p90 = df[col].quantile(0.90)
    return df_clean, p10, p90


def annotate_boxplot(fig, df_clean, col, p10, p90, line_color):
    q1 = df_clean[col].quantile(0.25)
    med = df_clean[col].median()
    q3 = df_clean[col].quantile(0.75)
    iqr = q3 - q1

    # Encontra os valores de Fences (Min/Max excluindo outliers de 1.5x IQR)
    min_fence = df_clean[df_clean[col] >= q1 - 1.5 * iqr][col].min()
    max_fence = df_clean[df_clean[col] <= q3 + 1.5 * iqr][col].max()

    if pd.isna(min_fence):
        min_fence = df_clean[col].min()
    if pd.isna(max_fence):
        max_fence = df_clean[col].max()

    # Separação Top/Bottom em zigue-zague para os textos NUNCA colidirem na UI
    stats_top = {"Min": min_fence, "Q1": q1, "Q3": q3, "Max": max_fence}
    stats_bot = {"P10": p10, "Med": med, "P90": p90}

    # Aplica cor BRANCA aos valores
    for label, val in stats_top.items():
        if pd.notna(val):
            fig.add_annotation(
                x=val,
                y=0.58,
                yref="paper",
                text=f"{label}<br><b>{int(val)}</b>",
                showarrow=False,
                font=dict(size=11, color="white"),
                yanchor="bottom",
                align="center",
            )

    for label, val in stats_bot.items():
        if pd.notna(val):
            fig.add_annotation(
                x=val,
                y=0.42,
                yref="paper",
                text=f"<b>{int(val)}</b><br>{label}",
                showarrow=False,
                font=dict(size=11, color="white"),
                yanchor="top",
                align="center",
            )

    # Desenha os fences pontilhados discretos para P10 e P90 usando a cor original da linha do plot
    for val in [p10, p90]:
        if pd.notna(val):
            fig.add_shape(
                type="line",
                x0=val,
                x1=val,
                y0=0.35,
                y1=0.65,
                yref="paper",
                line=dict(color=line_color, width=2, dash="dot"),
            )


def render_comparative_anatomy(df_dist, kpi_data):
    st.markdown(
        "<div class='sre-section-title'>Comparative Anatomy: Dispersion and Waiting Scale</div>",
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div style="font-size: 0.85rem; color: #6b7280; margin-bottom: 1.5rem; line-height: 1.4;">
            <b>How to read the charts:</b> The <b>center line</b> in the box is the median (typical patient). 
            The <b>box</b> groups 50% of the queue. The <b>dashed lines</b> show the SRE boundaries: 
            <b>P10</b> (the 10% fastest/efficiency) and <b>P90</b> (the 90% network limit/guarantee). 
            The <b>individual points</b> on the right are outliers (non-standard critical cases).
        </div>
    """,
        unsafe_allow_html=True,
    )

    if df_dist.empty:
        return

    df_plot_fila, p10_fila, p90_fila = get_sre_stats(df_dist, "dias_fila")
    df_plot_esq, p10_esq, p90_esq = get_sre_stats(df_dist, "dias_esquecido")

    # Escala Unificada para comparação direta (Adicionamos margem negativa para os textos não cortarem)
    max_val = (
        max(df_plot_fila["dias_fila"].max(), df_plot_esq["dias_esquecido"].max())
        if not df_plot_fila.empty and not df_plot_esq.empty
        else 100
    )
    limite_x = [-max_val * 0.08, max_val * 1.08]

    # --- RENDERIZAÇÃO: BOXPLOT ABANDONO (VERMELHO) ---
    # SRE Performance Fix: Amostragem para evitar MessageSizeError (OOM do FrontEnd via Websocket de >200MB)
    df_render_esq = (
        df_plot_esq.sample(n=min(10000, len(df_plot_esq)), random_state=42)
        if not df_plot_esq.empty
        else df_plot_esq
    )
    fig_esq = px.box(
        df_render_esq,
        x="dias_esquecido",
        title="Abandonment: Days without Evolution",
        points="outliers",
        color_discrete_sequence=["#ef4444"],
        range_x=limite_x,
    )

    # Aplica a anotação SOTA
    annotate_boxplot(
        fig_esq, df_plot_esq, "dias_esquecido", p10_esq, p90_esq, "#ef4444"
    )

    # Remove Hover (SRE UX: Zero Distraction)
    fig_esq.update_traces(hoverinfo="skip", hovertemplate=None)
    fig_esq.update_layout(
        hovermode=False,
        height=200,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=40, b=40),
    )
    fig_esq.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#f1f5f9")
    st.plotly_chart(fig_esq, width="stretch", config={"displayModeBar": False})

    # --- RENDERIZAÇÃO: BOXPLOT CADASTRO (AZUL) ---
    df_render_fila = (
        df_plot_fila.sample(n=min(10000, len(df_plot_fila)), random_state=42)
        if not df_plot_fila.empty
        else df_plot_fila
    )
    fig_fila = px.box(
        df_render_fila,
        x="dias_fila",
        title="Registration: Days Waiting",
        points="outliers",
        color_discrete_sequence=["#3b82f6"],
        range_x=limite_x,
    )

    # Aplica a anotação SOTA
    annotate_boxplot(fig_fila, df_plot_fila, "dias_fila", p10_fila, p90_fila, "#3b82f6")

    # Remove Hover (SRE UX: Zero Distraction)
    fig_fila.update_traces(hoverinfo="skip", hovertemplate=None)
    fig_fila.update_layout(
        hovermode=False,
        height=200,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=40, b=40),
    )
    fig_fila.update_xaxes(showgrid=True, gridwidth=1, gridcolor="#f1f5f9")
    st.plotly_chart(fig_fila, width="stretch", config={"displayModeBar": False})

    if len(df_dist) > len(df_plot_fila) or len(df_dist) > len(df_plot_esq):
        st.caption(
            "ℹ️ Optimized scale (extreme outliers hidden from viewport for easier visualization). Statistics preserved."
        )

    # --- 2. INDICADORES P90 (PADRÃO ST.METRIC PARA CONSISTÊNCIA VISUAL) ---
    st.write(" ")
    g_p90_1, g_p90_2 = st.columns(2)

    with g_p90_1:
        p90_esq_str = f"{int(p90_esq)} dias" if pd.notna(p90_esq) else "N/A"
        render_kpi(
            g_p90_1,
            label_with_icon="⏳ P90 Forgotten Time",
            value=p90_esq_str,
            help_text="90% of the network has not received clinical updates up to this day limit.",
        )

    with g_p90_2:
        p90_fila_str = f"{int(p90_fila)} dias" if pd.notna(p90_fila) else "N/A"
        render_kpi(
            g_p90_2,
            label_with_icon="⏱️ P90 Queue Time",
            value=p90_fila_str,
            help_text="90% of the network waits up to this day limit from registration to appointment.",
        )

    # 3. GAUGES (FINAL DA SEÇÃO)
    st.write(" ")
    g1, g2 = st.columns(2)
    taxa_urgencia = getattr(kpi_data, "taxa_urgencia", 0.0)
    taxa_vencidos = getattr(kpi_data, "taxa_vencidos", 0.0)

    with g1:
        fig_gauge1 = go.Figure(
            go.Indicator(
                mode="gauge+number+delta",
                value=taxa_urgencia,
                number={"suffix": "%", "font": {"color": "#4B5563"}},
                title={"text": "Severity Index", "font": {"size": 14}},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#ef4444" if taxa_urgencia > 30 else "#f97316"},
                    "bgcolor": "rgba(0,0,0,0)",
                    "steps": [
                        {"range": [0, 15], "color": "#dcfce7"},
                        {"range": [15, 30], "color": "#fef08a"},
                        {"range": [30, 100], "color": "#fee2e2"},
                    ],
                },
            )
        )
        fig_gauge1.update_layout(
            height=220,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(
            fig_gauge1,
            width="stretch",
            config={"displayModeBar": False},
        )
    with g2:
        fig_gauge2 = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=taxa_vencidos,
                number={"suffix": "%", "font": {"color": "#4B5563"}},
                title={"text": "SLA Breach (>180d)", "font": {"size": 14}},
                gauge={
                    "axis": {"range": [0, 100]},
                    "bar": {"color": "#1e293b"},
                    "bgcolor": "rgba(0,0,0,0)",
                    "steps": [
                        {"range": [0, 10], "color": "#dcfce7"},
                        {"range": [10, 25], "color": "#fef08a"},
                        {"range": [25, 100], "color": "#fee2e2"},
                    ],
                },
            )
        )
        fig_gauge2.update_layout(
            height=220,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(
            fig_gauge2,
            width="stretch",
            config={"displayModeBar": False},
        )
