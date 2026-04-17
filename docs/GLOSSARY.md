# Ubiquitous Language Glossary — Gercon Clinical Subdomain

> This glossary ensures that **Clinical Stakeholders** and **Engineers** use identical terminology.
> All code, tests, metrics, and documentation must use these terms without translation.

---

## Domain Entities

| Ubiquitous Term | Code/Column | Clinical Definition |
|---|---|---|
| **Protocol** | `numeroCMCE` | Unique identifier for a consultation or procedure request in the Gercon system. Equivalent to a "ticket" in the context of clinical queues. |
| **Patient** | `usuarioSUS_*` | Person waiting for care in the regulation system. Identified by CPF (PII hash) and demographic data. |
| **Parent Specialty** | `entidade_especialidade_descricao` | Main medical category (e.g., Cardiology, Neurology). |
| **Sub-Specialty** | `entidade_subespecialidade_descricao` | Specialization within the Parent (e.g., Echocardiogram within Cardiology). |
| **Risk Color** | `entidade_classificacaoRisco_cor` | Clinical urgency classification by colors: RED (emergency) > ORANGE (urgent) > YELLOW (somewhat urgent) > GREEN (non-urgent) > BLUE (elective) > WHITE (unclassified). |
| **List Source** | `origem_lista` | Type of waiting list in Gercon (e.g., "Awaiting Vacancy", "Scheduled"). |
| **ICD** | `cid_descricao` | International Classification of Diseases — diagnostic code associated with the request. |

## Temporal Metrics

| Ubiquitous Term | Calculation | Definition |
|---|---|---|
| **Lead Time** | `DATEDIFF(dataSolicitacao, CURRENT_DATE)` | Total time in days a patient has been waiting in line since the request. It is the primary SLA metric of the regulatory system. |
| **Forgotten** | `Lead Time > SLA_DIAS_VENCIMENTO` | Patient whose Lead Time has exceeded the acceptable limit (default: 180 days). Indicates government abandonment or systemic regulation failure. |
| **Expired** | Synonym for **Forgotten** | Used interchangeably in UI filters. Represents patients who violated the temporal SLA. |
| **P90 Lead Time** | `PERCENTILE_CONT(0.9)` on Lead Time | Tail latency: 90% of patients wait less than this value. SRE metric for outlier detection. |
| **Span (Window)** | `MAX(dataSolicitacao) - MIN(dataSolicitacao)` | Temporal amplitude of the filtered dataset. Used to normalize derived metrics like "Registrations per Month". |

## Derived Metrics

| Ubiquitous Term | Formula | Interpretation |
|---|---|---|
| **Evolution per Patient** | `events / patients` | Average number of events (consultations, procedures) each patient generated. High values indicate continuous treatment or registration errors. |
| **Urgency Rate** | `(urgent_pts / patients) × 100` | Percentage of patients classified as high risk. Indicates pressure on the regulation system. |
| **Expired Rate** | `(expired_pts / patients) × 100` | Percentage of patients who exceeded the SLA. Direct indicator of operational failure. |
| **Registrations per Month** | `patients / (span_days / COMMERCIAL_MONTH)` | Normalized throughput of patients entering the queue. |

## Infrastructure Concepts

| Term | Definition |
|---|---|
| **Data Contract** | Parquet schema validation at repository initialization. If required columns are missing, the Circuit Breaker is triggered. |
| **Amber Alert** | UI alert banner when the Parquet file has an `mtime` older than `DATA_SLA_THRESHOLD` hours. Signals "Data Silence" — the Scraper might have stopped. |
| **Poison Pill** | JSON payload received from Gercon that fails Pydantic validation (`GerconPayloadContract`). Redirected to the DLQ (Dead Letter Queue). |
| **DLQ (Dead Letter Queue)** | List of records that failed contract validation and were stored for future reprocessing. |
| **Circuit Breaker** | Resilience pattern: if >5% of records are Poison Pills, it aborts the ingestion cycle to protect the integrity of the Data Lake. |
| **Graceful Degradation** | System ability to continue operating (with reduced performance) when a dependency fails (e.g., Redis unavailable → fallback to direct query). |
| **Watermark** | Timestamp of the last processed record for each list. Used to avoid reprocessing in incremental cycles. |
| **Cloud Run Auth Adapter** | Authentication adapter for serverless runtime (ADR-004). Uses a temporary password gate while Firebase Auth is not configured. Detected via `K_SERVICE`. |
| **Password Gate** | Temporary authentication mechanism for Cloud Run via `CLOUD_RUN_AUTH_PASSWORD`. Replaces Keycloak/oauth2-proxy in environments without an identity sidecar. |
| **Firebase Auth (Phase 2)** | GCP-managed identity provider to replace the Password Gate. Supports email/password, Google SSO, and custom claims (CRM, roles). |

## Business Policy

| Term | Code | Definition |
|---|---|---|
| **Clinical Policy** | `ClinicaPolicy` | Immutable Value Object in the Core Domain that encapsulates hospital domain business invariants: valid age range, expiration SLA, urgency colors, commercial month, and data freshness threshold. Source of truth for business rules. Ref: ADR-005. |
| **Expiration SLA** | `sla_dias_vencimento` | Number of days after the request date that characterizes a patient as "expired" (without care within the timeframe). Default: 180 days. Criterion for `PacienteVencidoSpec`. |
| **Freshness Threshold** | `data_sla_threshold_horas` | Maximum hours without an update to the Parquet file before the system triggers the Amber Alert in the UI, indicating potentially outdated data. Default: 2h. |
| **Commercial Month** | `mes_comercial_dias` | Standardized duration of the month for normalizing temporal KPIs (e.g., Registrations/Month). Default: 30.416 days (365/12). |
| **Clinical Age Range** | `idade_min / idade_max` | Valid age limits for patients in the waiting line. Domain invariant: `idade_min` < `idade_max`, 0 ≤ `idade_min`, `idade_max` ≤ 150. |
| **Urgency Colors** | `cores_urgencia` | Set of clinical risk classifications that qualify a patient as urgent (`RED`, `ORANGE`, `YELLOW`). Criterion for `PacienteUrgenteSpec`. |
