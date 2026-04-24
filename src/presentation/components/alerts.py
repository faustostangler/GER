import streamlit as st


def render_amber_alert(kpis, policy) -> None:
    """Renders the Amber Alert banner if data freshness violates the SLA.

    Amber Alert: Banner signals "Silence of Data" — Scraper might have stopped.
    Ref: docs/GLOSSARY.md — Amber Alert / Freshness Threshold.

    WHY (Humble Object + Domain Isolation): All staleness arithmetic lives in
    AnalyticKPIs.is_stale() and .age_hours (domain/models.py). This function
    only decides WHAT to render, not HOW to compute staleness.
    The three mutually exclusive states are:
      1. last_sync_at == 0  → No data ever synced (critical / fresh install).
      2. is_stale() is True  → Amber Alert: data is older than the SLA threshold.
      3. is_stale() is False → Data is fresh; no banner rendered.

    Args:
        kpis: AnalyticKPIs domain model with is_stale() and age_hours.
        policy: ClinicaPolicy domain model with data_sla_threshold_horas.
    """
    from infrastructure.telemetry.metrics import SILENT_ERRORS

    threshold_hours = policy.data_sla_threshold_horas

    # --- State 1: No data ever synchronized (fresh install / pipeline never ran) ---
    if kpis.last_sync_at <= 0.0:
        # WHY: More prominent st.error() (red) to distinguish "no data" from "stale data".
        # Clinicians must understand this is not a warning but a critical data gap.
        st.error(
            "🔴 **Critical — No Data Synchronized Yet:** "
            "The data pipeline has not completed a successful sync. "
            "Execute the consolidation pipeline before using the dashboard.",
            icon="🚨",
        )
        # WHY: Emits Prometheus metric so the SRE team can alert on day-0 deploys
        # where the Worker never ran (pipeline misconfiguration).
        SILENT_ERRORS.labels(component="data_sla_violation").inc()
        return

    # --- State 2: Amber Alert → data is stale (age > threshold) ---
    if kpis.is_stale(threshold_hours):
        age = kpis.age_hours
        # WHY: Custom HTML banner for visual prominence (Digital Surgeon aesthetic).
        # st.warning() was considered but lacks the persistent top-of-page impact
        # needed in a clinical context where this alert must not be missed.
        alert_html = f"""
        <div class="amber-alert-container">
            <div class="amber-alert-icon">⚠️</div>
            <div class="amber-alert-content">
                <div class="amber-alert-title">Amber Alert: Data Freshness SLA Violation</div>
                <div class="amber-alert-text">
                    The displayed data is <b>{age:.1f} hours old</b>
                    (threshold: {threshold_hours:.1f}h).
                    Silence of Data — the Scraper Worker might have stopped
                    or failed in the last execution cycle.
                </div>
            </div>
        </div>
        """
        st.markdown(alert_html, unsafe_allow_html=True)
        # WHY: Prometheus SILENT_ERRORS register each render so Alertmanager can
        # page the SRE on-call if the banner appears for more than N minutes.
        SILENT_ERRORS.labels(component="data_sla_violation").inc()

    # --- State 3: Data is fresh — no banner rendered (happy path) ---


def render_auth_violation_alert(exception: Exception) -> None:
    """Renders a premium clinical access denial gate for unverified doctors.

    WHY (Zero-Trust UI): Provides clear guidance when authentication succeeds but
    domain authorization (CRM verification) fails. Instead of a generic 403,
    the clinician receives a high-fidelity explanation with their context.
    """
    crm_raw = getattr(exception, "crm_raw", "Não identificado")
    user_id = getattr(exception, "user_id", "Desconhecido")

    st.markdown(
        f"""
        <div class="auth-violation-container">
            <div class="auth-violation-header">
                <span class="auth-violation-icon">🔐</span>
                <div class="auth-violation-title">Acesso Clínico Bloqueado</div>
            </div>
            <div class="auth-violation-body">
                <p>Identidade confirmada, mas seu <b>perfil profissional ainda não possui autorização</b> para este módulo.</p>
                <div class="auth-violation-meta">
                    <b>Subject ID:</b> <code>{user_id}</code><br>
                    <b>CRM Detectado:</b> <code>{crm_raw}</code>
                </div>
                <div class="auth-violation-reasons">
                    <b>Possíveis causas:</b>
                    <ul>
                        <li>Seu cadastro é novo e ainda está sendo validado pelo CFM.</li>
                        <li>Houve uma falha na sincronização do seu CRM com o Keycloak.</li>
                        <li>Sua licença médica está inativa ou suspensa.</li>
                    </ul>
                </div>
                <hr style="border: 0; border-top: 1px solid rgba(239, 68, 68, 0.2); margin: 20px 0;">
                <p style="font-size: 0.85rem; opacity: 0.8;">
                    <i>Security Protocol: Zero-Trust CRM Authorization (ADR-006).</i>
                </p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
