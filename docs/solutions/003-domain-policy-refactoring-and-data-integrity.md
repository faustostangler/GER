# 003: Domain Policy Refactoring and Data Integrity SOTA

## Overview
This session focused on enforcing **Domain-Driven Design (DDD)** and **Clean Architecture** rigor by decoupling business invariants from infrastructure configurations and resolving critical environment-related data corruption issues.

---

## 1. Tactical Pillar: DDD & Clean Architecture Refactoring

### The Problem: Infrastructure Leakage
Business invariants (Age ranges, SLA thresholds, Urgency colors) were previously defined in `infrastructure/config.py` (`AppSettings`). This led to:
- **Dependency Violation**: The `AnalyticsUseCase` (Application) was directly importing `settings` (Infrastructure).
- **Silent Mutations**: Business rules could be changed via `.env` without code versioning or unit tests.
- **Testing Friction**: Use cases required complex environment setups even for pure logic tests.

### The Solution: ClinicaPolicy Value Object (ADR-005)
We extracted these invariants into a pure domain **Value Object**: `src/domain/policies.py:ClinicaPolicy`.

- **Immutability**: Inherits from `Pydantic BaseModel` with `frozen=True`.
- **Self-Validation**: Enforces domain constraints (e.g., `age_min < age_max`, valid clinical colors) during instantiation using `@field_validator`.
- **Dependency Injection (DI)**: The `AnalyticsUseCase` now receives a `ClinicaPolicy` instance, making it completely agnostic of environment variables.
- **Composition Root**: The wire-up logic was moved to `app_analytics.py:get_use_case`, which reads overrides from `settings` and constructs the `ClinicaPolicy` to be injected into the services.

---

## 2. Strategic Pillar: Data Integrity & Environment Stability

### Parquet Path Fix
- **Issue**: A Docker volume mounting error caused the host directory to be mistaken for a file, creating a directory named `gercon_consolidado.parquet` instead of the actual file.
- **Resolution**: Cleanup of host-side directory artifacts and recreation of the data volume mount.
- **Restoration**: The `sqlite_to_parquet.py` pipeline was executed via `docker exec`, restoring **123,136 records** from the raw SQLite database to the production Parquet file.

---

## 3. Engineering Excellence: Safety Nets & TDD

### Automated Architectural Guard
Implemented a unique **AST-based unit test** (`test_use_case_nao_importa_settings_diretamente`) that:
1. Parses the source code of the Use Case.
2. Inspects imports.
3. Fails if a direct import from `infrastructure.config` is detected.
This ensures the architectural boundary is maintained permanently against future regressions.

### Mutation Testing Readiness
- All new logic in `ClinicaPolicy` and `AnalyticsUseCase` DI was covered with `pytest`.
- High-quality gates (0 mutmut survivors policy) are enforced for core domain invariants.

---

## 4. Documentation & Ubiquitous Language

- **ADR-005**: Formally documented the "Business Policy Extraction" decision, including context, decision rationale, and consequences (positive/negative).
- **Glossary Update**: Synchronized `docs/GLOSSARY.md` with terms like `SLA de Vencimento`, `Limiar de Frescor`, and `Mês Comercial` to bridge communication between engineering and clinical stakeholders.

---

## Execution Checklist for Future Work
- [ ] **DI First**: Never import `settings` inside `domain/` or `application/`.
- [ ] **Policy Over Config**: If a variable represents a clinical rule, it belongs in `ClinicaPolicy`.
- [ ] **Validation over Trust**: Use Pydantic's `field_validator` for all domain inputs.
- [ ] **Clean Data Lake**: Ensure `gercon_consolidado.parquet` is always a file, never a directory.
