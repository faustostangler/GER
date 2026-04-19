import streamlit as st
import pandas as pd
import re
from domain.models import AnalyticKPIs

def render_kpi(container, label_with_icon: str, value: str, help_text: str = "", alert: bool = False):
    alert_class = "alert" if alert else ""
    help_clean = str(help_text).replace('"', "&quot;")

    icon_match = re.match(r"^([^\w\s]+)\s*(.*)$", label_with_icon)
    if icon_match:
        icon, label = icon_match.groups()
        icon_html = f'<div class="kpi-icon">{icon}</div>'
    else:
        icon_html = ""
        label = label_with_icon

    html = f"""
    <div class="kpi-card {alert_class}" title="{help_clean}">
        <div class="kpi-card-header">
            {icon_html}
            <div class="kpi-label">{label}</div>
        </div>
        <div class="kpi-value-container">
            <div class="kpi-value">{value}</div>
        </div>
    </div>
    """
    container.markdown(html, unsafe_allow_html=True)

def render_kpi_board(kpi_data: AnalyticKPIs, container=None):
    """Componente que encapsula o painel de KPIs executivos.
    
    WHY: Isola a renderização das métricas de forma que o script
    principal app_analytics.py fique focado apenas na orquestração.
    """
    if container is None:
        container = st
        
    container.markdown(
        "<div class='sre-section-title'>Performance Dashboard (SLA and Load)</div>",
        unsafe_allow_html=True,
    )

    # --- Extração Segura das Variáveis ---
    pacientes = kpi_data.pacientes
    eventos = kpi_data.eventos
    esp_mae = kpi_data.esp_mae
    sub_esp = kpi_data.sub_esp
    medicos = kpi_data.medicos
    origens = kpi_data.origens
    lead_time = kpi_data.lead_time
    max_lead_time = kpi_data.max_lead_time

    evo_por_paciente = kpi_data.evo_por_paciente
    sub_por_esp = kpi_data.sub_por_esp
    cid_por_medico = kpi_data.cid_por_medico
    evo_por_medico = kpi_data.evo_por_medico
    cad_por_mes = kpi_data.cad_por_mes

    # --- LINHA 1: Volume, Carga e Esforço ---
    r1_c1, r1_c2, r1_c3, r1_c4 = container.columns(4)
    render_kpi(
        r1_c1,
        "🏢 Gercon Sources",
        f"{origens:,}".replace(",", "."),
        help_text="Number of distinct entry points/source systems.",
    )
    render_kpi(
        r1_c2,
        "👥 Patients",
        f"{pacientes:,}".replace(",", "."),
        help_text="Total number of unique patients selected.",
    )
    render_kpi(
        r1_c3,
        "📋 Evolutions",
        f"{eventos:,}".replace(",", "."),
        help_text="Total number of events in the clinical history.",
    )
    render_kpi(
        r1_c4,
        "📈 Evolutions/Patient",
        f"{evo_por_paciente}".replace(".", ","),
        help_text="Average number of times the patient was moved or evaluated.",
    )

    container.write(" ")

    # --- LINHA 2: Complexidade Clínica e SLA ---
    r2_c1, r2_c2, r2_c3, r2_c4 = container.columns(4)
    render_kpi(
        r2_c1,
        "🏛️ Specialties (Parent)",
        f"{esp_mae:,}".replace(",", "."),
        help_text="Broad clinical areas covered (E.g.: SURGERY).",
    )
    render_kpi(
        r2_c2,
        "🎯 Subspecialties",
        f"{sub_esp:,}".replace(",", "."),
        help_text="Fine specialties covered (E.g.: HAND SURGERY).",
    )
    render_kpi(
        r2_c3,
        "🔀 Subs/Specialty",
        f"{sub_por_esp}".replace(".", ","),
        help_text="Average branches per broad clinical area.",
    )

    lead_str = (
        f"{lead_time} dias | {max_lead_time} dias"
        if pd.notna(lead_time)
        else "0 dias"
    )
    render_kpi(
        r2_c4,
        "⏱️ Queue: Average | Worst",
        lead_str,
        help_text="Average Time vs Time of the oldest patient.",
    )

    container.write(" ")

    # --- LINHA 3: Governança e Comportamento Médico ---
    r3_c1, r3_c2, r3_c3, r3_c4 = container.columns(4)
    render_kpi(
        r3_c1,
        "👨⚕️ Requesting Doctors",
        f"{medicos:,}".replace(",", "."),
        help_text="Total distinct doctors who inserted patients in this queue.",
    )
    render_kpi(
        r3_c2,
        "📅 Registrations/Month",
        f"{cad_por_mes}".replace(".", ","),
        help_text="Historical monthly average of new patients added to the queue (based on the filtered window).",
    )
    render_kpi(
        r3_c3,
        "🧠 Diagnostic Dispersion",
        f"{cid_por_medico}".replace(".", ","),
        help_text="Average distinct ICDs used per doctor.",
    )
    render_kpi(
        r3_c4,
        "⚙️ Load/Doctor",
        f"{evo_por_medico}".replace(".", ","),
        help_text="Average volume of administrative evolutions generated per doctor.",
    )

    container.divider()
