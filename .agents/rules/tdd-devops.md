---
trigger: always_on
---

GER Onboarding Guide: SOTA Modular Monolith
1. Architecture Overview
The GER system is designed as a SOTA (State of the Art) Modular Monolith using Hexagonal (Clean) Architecture. It always follows the separation of concerns between business logic (Domain) and technical implementation (Infrastructure).

Primary Runtime (Analytics): A Streamlit-based dashboard and FastAPI backend serving as the Presentation/BFF layer.

Async Processing (Worker): Resilient background tasks managed by Arq (Redis-based).

Event Streaming: High-performance messaging via Redpanda (Kafka-compatible).

Identity Layer: Centralized OIDC via Keycloak, protected by OAuth2-Proxy with a Redis session store to handle large JWTs.

Edge Routing: Nginx acts as an internal border router to handle redirects and root path orchestration.

Observability Stack: Prometheus (Golden Signals + Business Metrics), Grafana (DORA + SRE dashboards with auto-provisioning), Grafana Loki (log aggregation), and Sentry (real-time error tracking with LGPD-compliant breadcrumb filtering).

2. Tech Stack
Language: Python >= 3.11.

Data Layer: DuckDB (OLAP), Pandas, PyArrow, and PostgreSQL (for Keycloak).

Communication: gRPC (Service-to-Service), HTTPX, and AIOConsumer (Kafka).

Infrastructure: Docker Compose, Nginx, Terraform, Google Cloud Run (Serverless).

Caching: Redis with PyArrow IPC (Feather) serialization for distributed DataFrame cache. Graceful degradation to direct Parquet query when Redis is unavailable.

Quality & Security: 
* Testing: Pytest, Mutmut (Mutation Testing), Consumer-Driven Contract (CDC) tests, Chaos Engineering tests, and Playwright E2E.
* Error Tracking: Sentry SDK with release tagging via `GIT_SHA` and LGPD-compliant data filtering.
* Linting: Ruff, Mypy.

3. Environment Variables
Configurations are split into env/creds.env (secrets) and env/config.env (non-sensitive).

Variable	Description	Requirement
RDE_ACCESS_TOKEN	Remote Development Environment token	Min 32 chars.
EXTERNAL_DOMAIN	Base domain for service access	Usually 127.0.0.1.nip.io.
KEYCLOAK_CLIENT_SECRET	OIDC Client secret	Must match Keycloak setup.
DUCKDB_MEMORY_LIMIT	RAM allocation for DuckDB	Default: 1.5GB.
SERVICE_TYPE	Defines if container is analytics or worker	Required in Compose.
DATA_SLA_THRESHOLD	Max hours before Amber Alert triggers	Default: 2.0 hours.
SENTRY_DSN	Sentry error tracking DSN	Optional (None = no-op in dev).
GIT_SHA	Git commit SHA for release tracking	Injected via Docker build-arg / CI.

4. Directory Structure
Plaintext
/
├── .agents/rules/      # AI agent instructions (TDD/DevOps focus)
├── docs/               # Living Documentation
│   ├── adr/            # Architectural Decision Records (ADR-001 to ADR-003)
│   └── GLOSSARY.md     # Ubiquitous Language glossary (clinical domain)
├── env/                # Environment configuration files
├── infra/              # Nginx and Identity (Keycloak) exports
├── monitoring/         # Observability Configuration (GitOps)
│   ├── grafana/
│   │   ├── dashboards/     # JSON dashboard definitions (SRE + DORA)
│   │   └── provisioning/   # Auto-provisioning for datasources + dashboards
│   └── prometheus/
│       └── alerts.yml      # Symptom-based alert rules (6 rules)
├── src/                # Core Source Code
│   ├── domain/         # Entities, Mappers, Specifications, and Value Objects
│   ├── application/    # Use Cases and Port Interfaces
│   ├── infrastructure/ # Adapters (DB, Auth, Scraper, Telemetry, Sentry)
│   └── presentation/   # API and Streamlit UI
├── tests/              # Test suites (Domain, App, Infra, Chaos, E2E)
├── Makefile            # Orchestration shortcuts
└── pyproject.toml      # Dependency management (uv)

5. Component Details
ScraperUseCase: Implements the core logic for data ingestion from Gercon. Features:
* Circuit Breaker (5% error threshold, min 100 hits) that raises `DomainContractViolationException`.
* DLQ (Dead Letter Queue) for "poison pills" (corrupt payloads).
* Session-level Golden Signals: `scraper_success_total`, `scraper_failure_total`, `scraper_session_duration_seconds`.
* Granular status tracking: `SUCCESS`, `PARTIAL` (DLQ items), `FAILURE`, `CIRCUIT_BREAKER`.
* Ingestion Audit Log: writes `IngestionLogEntry` to SQLite after each cycle for Post-Mortem analysis.
* Optional `IIngestionLogRepository` port (backward-compatible via `ingestion_log=None`).

GerconPayloadContract: A Pydantic-based ACL (Anticorruption Layer) that validates incoming external data against internal domain rules.

IngestionLogEntry: Domain Value Object for audit trail. Captures: timestamp, duration_seconds, status (IngestionStatus enum), items_ingested, items_failed, bytes_processed, target_lists, error_message.

DuckDBAnalyticsRepository:
* Data Contract validation on init: verifies critical Parquet columns exist (fail-fast `ValueError`).
* Redis distributed cache using PyArrow IPC serialization (~40-60% faster than pickle, language-agnostic).
* Graceful Redis degradation: if `ping()` fails, sets `redis_client=None` and queries Parquet directly.
* Injects `last_sync_at` (Parquet mtime) into `AnalyticKPIs` for Data Freshness SLA.

DuckDBSpecificationTranslator: Translates domain Specification objects into DuckDB SQL via `match/case`. Critical dependency on Module Identity (see Hurdles section).

AnalyticsUseCase: Orchestrates queries via `IAnalyticsRepository` port. Injects `settings.MES_COMERCIAL_DIAS` and specification composition (PacienteUrgenteSpec, PacienteVencidoSpec) using config-driven business rules.

Worker: Handles heavy I/O tasks like scraping and data consolidation, exposing Prometheus metrics on port 8000.

Data Freshness Monitor (Amber Alert): The Streamlit UI computes `age_hours = (time.time() - kpi.last_sync_at) / 3600`. If `age_hours > settings.DATA_SLA_THRESHOLD`, renders `st.warning()` alerting clinicians that data is stale.

Sentry Integration: Initialized in `app_analytics.py` before Streamlit rendering. Release tagged as `gercon-analytics@{GIT_SHA}`. LGPD compliance: `send_default_pii=False`, SQL breadcrumbs redacted via `before_breadcrumb` filter.

Humble Object Pattern (Presentation): To ensure testability of the UI, `app_analytics.py` and its components must be kept "humble" (thin rendering only). All complex logic — such as SQL sanitization, term parsing, format conversions, and session state calculations — is extracted to pure Python functions in `src/presentation/adapters/` (e.g., `parsers.py`). This allows testing 90% of the presentation logic without any Streamlit overhead.

Legacy Code Eradication: The system is in continuous evolution. Legacy artifacts (raw SQL strings, hardcoded column names, manual auth headers) must be targeted for systematic eradication.
* **Boy Scout Rule**: Always leave the code cleaner than you found it. 
* **Refactoring Protocol**: When touching a module with raw SQL, migrate it to the Specification Pattern (ADR-004) as part of the task.
* **Technical Debt**: All legacy code must be wrapped in an Anti-Corruption Layer (ACL) until it can be fully replaced.

Blameless Post-Mortem: Reliability is built on transparency. When failures occur (e.g., "silent" filtering bugs), we do not seek to blame individuals but to identify systemic weaknesses.
* **Artifact**: Every critical failure must result in a Post-Mortem document in `docs/adr/` or `artifacts/`.
* **Focus**: Identify the "Why" (e.g., Module Identity mismatch) and implement "Structural Prevention" (e.g., defensive name matching in translators).

6. Design Patterns & Strict Rules
Developers MUST follow these paradigms IN THIS ORDER:

TDD (Test-Driven Development): Write tests before logic. The CI pipeline uses Mutmut; builds will fail if any "mutant" survives in the domain layer (0 survivor policy).

DDD (Domain-Driven Design): Logic belongs in the domain layer using Specifications and Mappers.

Clean Architecture: Use cases must only depend on interfaces, never on concrete infrastructure adapters.

Specification Pattern: Domain filtering logic uses composable specifications (`PacienteUrgenteSpec`, `PacienteVencidoSpec`, `LeadTimeCriticoSpec`) that are infrastructure-agnostic. The `DuckDBSpecificationTranslator` in the infrastructure layer translates them to SQL. This decouples "what to filter" (domain) from "how to query" (infrastructure).

Living Documentation: Every significant architectural decision MUST have an ADR in `docs/adr/`. All domain terms MUST be defined in `docs/GLOSSARY.md` (Ubiquitous Language).

7. Common Hurdles & Solutions
Large JWTs: Keycloak tokens can exceed header limits. Solution: Using oauth2-proxy with Redis as a session_store.

Cold Boot Database Lag: Keycloak takes time to initialize its schema. Solution: The healthcheck includes a wait start_period.

Vendor API Instability: Scraper payloads often break contracts. Solution: The ScraperUseCase uses an explicit validation loop that redirects "poison pills" to the DLQ instead of crashing the process.

Module Identity Mismatch (CRITICAL): Python `match/case` and `isinstance` checks FAIL SILENTLY when the same class is imported via different module paths (e.g., `domain.X` vs `src.domain.X`). This causes structural pattern matching to fall through to the wildcard `case _`, producing wrong results without any error.
* Solution: ABSOLUTE RULE — Zero `src.` prefixes in imports inside `src/`. Always use `from domain...` not `from src.domain...`. The `PYTHONPATH=src` in `pyproject.toml` and Makefile enables this.
* Root cause: Python creates separate class objects for the same source when loaded via different module paths, breaking identity checks.
* Detection: If a `match/case` or `isinstance` returns unexpected results, check `spec.__class__.__module__` — it MUST NOT have `src.` prefix.

Mutmut Trampoline Errors: "Failed trampoline hit" occurs due to `src/` layout namespace leaks. 
* Solution 1: Zero `src.` prefixes in imports (see above — this is the same root cause).
* Solution 2: Explicit dynamic `sys.path.insert(0, src_path)` in `tests/conftest.py` directly, ensuring `pytest` permanently acknowledges the `src/` boundary.
* Solution 3: Architect a pristine `setup.cfg` void of `{pwd}` interpolations using `runner=uv run pytest tests/ -x -q -p no:cacheprovider -p no:cov` (enabling extremely fast fail-fast exits). For absolute resets, destroy `.mutmut-cache/` completely.

Linter/Artifact Pollution: Ruff may fail on mutmut generated files.
* Solution: Explicitly exclude `mutants/` and `.mutmut-cache/` in `pyproject.toml` (`tool.ruff.exclude`).

Mutation Testing Noise in Mappers (ACL): Mutmut generates hundreds of meaningless survivors on pure dictionary extractions.
* Solution: We do not chase 100% overall Mutation. Eradicate noise strictly using `# pragma: no mutate` above complex mapping dict layers. Conversely, aggressively use Parameterized Data-Driven Tests (`@pytest.mark.parametrize`) on Specifications, Boundaries, and Pure Utilities to ensure absolute 100% Core Domain mutant extermination.

Redis Cache Serialization: Never use `pickle` for DataFrame caching in Redis.
* Solution: Use PyArrow IPC (Feather) via `pa.ipc.new_stream` / `pa.ipc.open_stream`. It's ~40-60% faster and language-agnostic (Rust, Go, Java can read the same buffer). See ADR-001.

Redis Unavailability: The system MUST NOT crash if Redis is down.
* Solution: Graceful degradation — `ping()` with 2s timeout on init. If it fails, `redis_client = None`. All cache operations check `if self.redis_client:` before execution. Log `CRITICAL` but continue serving via direct Parquet query.

8. Pipelines & Execution
CI/CD Pipeline (ci.yml) — 4 Jobs:

**Job 1: quality-gate**
1. Linting with Ruff.
2. Unit Tests (Domain/Application) with Coverage gate (85% minimum).
3. Mutation Testing Guard: Strict gate validating 0 mutants survive in Core Domain logic.
4. CDC (Consumer-Driven Contract) validation.
5. Chaos Engineering Tests: Validates graceful degradation under infrastructure failures (Redis down, corrupted Parquet, stale data).

**Job 2: security-scan**
* Runs shift-left dependency vulnerability scanning. Compiles to `requirements.txt` and executes `uvx pip-audit`. Action versions strictly target Node 24 architectures (`checkout@v5`, `setup-uv@v6`) to prevent deprecation warnings.

**Job 3: deploy-cloud-run** (Hard-gated by Jobs 1 + 2, main branch only)
* Docker build with `--build-arg GIT_SHA` and OCI labels for commit traceability.
* Cloud Run deploy with injected env_vars: `GIT_SHA`, `SENTRY_DSN`, `APP__ENVIRONMENT=production`.
* DORA deployment event logged (commit, author, timestamp, duration, status).
* Sentry release created and finalized via `sentry-cli` with auto-commit association.

**Job 4: dora-metrics** (Runs on main pushes)
* Calculates DORA Four Key Metrics:
  - Deployment Frequency (git log count over 7d/30d).
  - Lead Time for Changes (~7-10 min CI + Cloud Run rollout = Elite).
  - Change Failure Rate (0% guaranteed by 6 hard-gates = Elite).
  - MTTR strategy (Cloud Run revision revert + Sentry real-time detection = Elite).

Local CI Pipeline (Makefile):
* `make ci` runs: lint → test → test-audit → test-mutmut → test-contract.
* `make test` targets `tests/domain` and `tests/application` (fast, no infra deps).
* `make check-rde` validates RDE_ACCESS_TOKEN length for fail-fast security.

Continuous Deployment (Google Cloud Run): GitOps/K3s (Pull-based) is deprecated in favor of a Lean Serverless Push-based model via GitHub Actions. Deployment to Cloud Run relies absolutely on the app dynamically rebinding to the overriding `$PORT` variable injected by the GCP container runtime.

Scraper Sync: Uses a watermark system (via sqlite_raw_repository) and a scraper_state.json file to track pagination and avoid redundant fetches.

9. Observability Stack
**Prometheus Metrics (3 categories):**
* RED (Scraper Traffic): `gercon_scraper_pages_total`, `gercon_scraper_items_total`, `gercon_scraper_errors_total`, `gercon_scraper_duration_seconds`.
* USE (Pipeline): `gercon_pipeline_job_duration`, `gercon_parquet_size_bytes`, `gercon_pipeline_last_success`.
* Session Signals: `scraper_success_total`, `scraper_failure_total`, `scraper_session_duration_seconds`.
* Business KPIs: `gercon_data_freshness_hours`, `gercon_high_risk_patients_detected_total`, `gercon_user_decision_time_seconds`.

**Prometheus Alerts (6 rules):**
1. `ScraperAPIErrorRateSpike` — >10% errors in 15m (Critical).
2. `PipelineDurationApproachingLimit` — P90 >3h (Warning).
3. `StaleDataLake` — >27h without success (Critical).
4. `DataFreshnessSLAViolation` — >2h without Parquet update (Warning).
5. `ScraperSessionStall` — 0 success in 6h (Critical).
6. `HighRiskPatientSurge` — >500 high-risk patients (Warning, clinical-ops team).

**Grafana Dashboards (GitOps provisioned):**
* `gercon_sre.json` — Scraper Golden Signals (throughput, latency P90, error rate, saturation).
* `gercon_dora_business.json` — DORA metrics + Business KPIs (freshness, risk patients, UX decision time).

**Sentry:** Real-time error tracking. Release: `gercon-analytics@{GIT_SHA}`. LGPD: SQL queries redacted from breadcrumbs.

10. Architectural Decision Records (ADRs)
* ADR-001: Redis Distributed Cache — PyArrow IPC serialization, graceful degradation, alternatives considered.
* ADR-002: Circuit Breaker in Scraper — 5% threshold, 100 min hits, granular status (SUCCESS/PARTIAL/FAILURE/CIRCUIT_BREAKER).
* ADR-003: Sentry Error Tracking — Release tagging, LGPD compliance, traces sample rate (10%).

11. Testing Strategy & Coverage
Test suites mirror the `src/` hexagonal structure:

**Domain Tests (Pure, no infra dependencies):**
* `test_solicitacao_mapper.py` — Exhaustive data-driven tests for ACL mapper (`@pytest.mark.parametrize`).
* `test_specifications.py` — Truth tables for composite specifications + boundary value analysis.
* `test_ingestion_log.py` — IngestionLogEntry value object invariants and serialization roundtrip.
* `test_data_freshness.py` — SLA detection logic, CDC for `last_sync_at` field contract.

**Infrastructure Tests (Adapter validation):**
* `test_specification_translator.py` — DuckDB SQL translation for all specification types + composites.
* `test_gercon_contract.py` — Consumer-Driven Contract for external API payload validation.
* `test_ingestion_log_adapter.py` — SQLite audit log persistence and query correctness.
* `test_chaos_resilience.py` — Chaos Engineering: Redis down, corrupted Parquet, stale data.
* `test_sentry_integration.py` — Sentry init no-op, LGPD breadcrumb filtering, failure resilience.

**Application Tests:**
* `test_analytics_use_case.py` — Use case orchestration with mocked repository ports.

**Presentation Tests (UI layer):**
* `test_cdc_kpis.py` — CDC contract between Streamlit (consumer) and AnalyticKPIs model (provider).
* `test_app_ui.py` — Streamlit AppTest for session state validation.
* `test_smoke_app.py` — Smoke test for import and render health.

**E2E Tests:**
* `test_dashboard_flow.py` — Playwright: full navigation + metric rendering + title verification.

12. Post-Implementation Checklist
[ ] Mutation Coverage: Did you run make check-rde and ensure 0 mutmut survivors?

[ ] Contract Validation: Is the new external payload covered by a Pydantic schema in domain/schemas.py?

[ ] Telemetry: Are new operations being tracked by Prometheus metrics? Did you add Business KPIs if applicable?

[ ] Clean Start: Does the system boot from scratch using make clean-volumes && make up-iam?

[ ] Import Hygiene: Are all imports inside `src/` using `from domain...` NOT `from src.domain...`? (Module Identity Mismatch prevention)

[ ] Data Contract: If you modified the Parquet schema, did you update the column validation in `DuckDBAnalyticsRepository.__init__`?

[ ] Chaos Testing: Did you add tests for graceful degradation if your feature depends on external infrastructure (Redis, S3, API)?

[ ] ADR: Did you create an ADR in `docs/adr/` for any significant architectural decision?

[ ] Glossary: Did you add new domain terms to `docs/GLOSSARY.md`?

[ ] Sentry: If you added new error handling, does Sentry capture it with appropriate breadcrumbs (without PII)?

[ ] Redis Cache: If you modified query logic, did you test with Redis both available and unavailable?

13. Defensive Translation Pattern (Anticorruption)
To protect against Module Identity Mismatches in dynamic environments (Streamlit hot-reloads), all Infrastructure Translators should implement a "Name-Based Fallback" pattern in their `match/case` blocks:

```python
match spec:
    case _ if spec.__class__.__name__ == "PacienteUrgenteSpec":
        return "prioridade_clinica >= 4"
    case PacienteUrgenteSpec():  # Primary match
        ...
```

14. Diagnostic Protocols for Silent Failures
When UI filters appear active but data doesn't seem to update:
1. **Verification of Data Boundary**: Check if the raw KPI counts returned by the use case changed (e.g., from 123k to 12k). If yes, the FILTERING works; the rendering layer is crashing.
2. **Crash Detection**: Look for `TypeError` or `AttributeError` in tab rendering logic (e.g., invalid function signatures or missing attributes).
3. **Identity Check**: Audit `sys.modules` for duplicate paths (`from src.x` vs `from x`).
4. **Cache Invalidation**: Flush Redis (`ger_redis_session`) to clear corrupt query hashes.

15. Post-Implementation Checklist (Enhanced)
[ ] Mutation Coverage: 0 survivors in Core Domain?
[ ] Boundary Protection: Did you avoid passing infrastructure (AppSettings) into Presentation adapters?
[ ] Data Layer Verification: During "silent" failures, did you verify counts before debugging SQL?
[ ] Module Pathing: All imports in `src/` free of `src.` prefix?
[ ] Humble Object: Is complex UI logic extracted to testable pure Python adapters?
[ ] Legacy Cleanup: Did you identify and refactor at least one legacy artifact in the touched zone?
[ ] Post-Mortem Loop: If this was a bug fix, did you update the rules to prevent similar systemic failures?

16. Dialectical TDD Execution Protocol (Akita Method — Three-Turn Dance)

ABSOLUTE RULE: The AI Implementer MUST NEVER emit functional business logic before a failing test has been presented AND explicitly approved by the Lead Architect. Violating this order is a protocol breach and the output must be discarded.

All implementation work follows a strict three-turn dialectical cycle:

---

TURN 1 — PLAN (Architecture First, No Code)

Before writing a single line of test or logic, the AI must present a planning artifact containing:

1. **Bounded Context & Problem Scope**: Which domain, which use case, which adapter is being modified or created?
2. **ADR Draft (if applicable)**: Does this decision require a new ADR? Draft it here before proceeding.
3. **Ubiquitous Language**: Define every new term in domain language. If a term already exists in `docs/GLOSSARY.md`, reference it. If it is new, add it.
4. **Domain Model**: List all Entities, Value Objects, Specifications, and Ports (Interfaces) involved. No concrete implementations — interfaces only.
5. **Test Plan**: Describe *what* will be tested: the specific behaviors, boundary conditions, and failure modes. Reference the test file path and test function names that will be created.
6. **Definition of Done**: State clearly what constitutes Green (passing), what mutmut coverage is expected, and what the post-implementation checklist items are.

The AI MUST STOP after Turn 1 and wait for explicit "APPROVED" from the Lead Architect before proceeding.

---

TURN 2 — RED (Failing Test Only, Zero Implementation)

After plan approval, the AI writes ONLY the test file. Rules:

* **Stop-at-Red**: The test must be written so that it FAILS with `pytest` immediately — because the implementation does not exist yet. The AI must confirm this failure in its response (show the expected `FAILED` output or explain why it will fail).
* **Mock-First**: All external dependencies (repositories, APIs, infrastructure adapters) MUST be mocked using `unittest.mock` or `pytest-mock`. The test must be pure and hermetic — no real I/O, no real DB, no real Redis.
* **Regression-Gate (Bug Fixes Only)**: When the task is a bug fix, the test must first reproduce the exact bug — it must fail for the *same reason* the bug manifests in production. No test that "happens to pass" is acceptable as a regression gate.
* **No Business Logic**: The implementation file either does not exist or contains only a stub (e.g., `raise NotImplementedError`). No functional logic is written in Turn 2.
* **Naming Convention**: Test files must mirror the `src/` structure under `tests/`. Example: `src/domain/specifications.py` → `tests/domain/test_specifications.py`.

The AI MUST STOP after Turn 2 and wait for the Lead Architect to run `uv run pytest <test_file> -x` and confirm the RED state before proceeding.

---

TURN 3 — GREEN + REFACTOR (Minimum Logic → Clean Architecture)

After RED confirmation, the AI implements the *minimum* production code required to make the test pass, then immediately refactors:

* **Green (Minimum Viable Logic)**: Write only what the failing test demands. No premature generalization. No "while I'm at it" additions.
* **Refactor (Clean Architecture)**: Once green, apply the full design standards:
  - Strict hexagonal layering (Domain → Application → Infrastructure, never reversed).
  - Type hints on all signatures (`-> None`, `-> str`, etc.).
  - Google Style structured docstrings on all public methods.
  - Zero `src.` prefixes in imports (Module Identity Mismatch prevention).
  - Inline comments ONLY for ACL boundaries, infra limits, or non-obvious exceptions.
* **Mutation Check**: Run `uv run mutmut run --paths-to-mutate src/domain/` and confirm 0 survivors in Core Domain for the touched files.
* **Checklist Sweep**: Complete all items in Section 15 before declaring the cycle done.

After Turn 3, the cycle closes. A new feature or sub-task begins a new three-turn cycle from Turn 1.

---

Summary Table:

| Turn | Name    | AI Output                          | Gate (Human Action)                      |
|------|---------|------------------------------------|------------------------------------------|
| 1    | Plan    | ADR draft, model, test plan        | Explicit "APPROVED" from Lead Architect  |
| 2    | Red     | Failing test file + stub only      | Run pytest, confirm FAILED               |
| 3    | Green   | Min implementation + refactor      | Run pytest + mutmut, confirm 0 survivors |

Anti-Patterns Explicitly Forbidden:
* Emitting implementation in the same response as the test ("Red-Green in one shot") — FORBIDDEN.
* Writing tests that pass immediately because they test a mock, not behavior — FORBIDDEN.
* Skipping the Plan turn for "small" features — FORBIDDEN. Every change, no matter how small, starts with Turn 1.
* Using `# type: ignore` or `# noqa` to silence warnings instead of fixing the root cause — FORBIDDEN.