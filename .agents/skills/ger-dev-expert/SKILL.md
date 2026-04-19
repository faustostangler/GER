---
name: ger-dev-expert
description: Expert for GER project development following SOTA Modular Monolith, TDD, and DDD standards. Use when developing new features, fixing bugs, or refactoring in the GER codebase to ensure architectural integrity, 0-mutant survivors, and clean hexagonal boundaries.
---

# GER Dev Expert

This skill enforces the SOTA Modular Monolith standards for the GER project. It prioritizes TDD, DDD, and Hexagonal Architecture to maintain a high-performance, resilient, and observable ecosystem.

## Core Protocols

### 1. TDD First (Red-Green-Refactor)
TDD is the mandatory design tool for all functional code.
- **Red**: Write a failing unit or integration test FIRST. For bug fixes, an explicit failing regression test is mandatory.
- **Green**: Implement the MINIMUM code required to pass the test.
- **Refactor**: Clean up the code to adhere to Clean Architecture standards.
- **Verification**: Run `make check-rde` to ensure 0 mutants survive in core domain logic.

### 2. Import Hygiene (Module Identity Protection)
Python `match/case` and `isinstance` fail silently if module paths mismatch.
- **Rule**: NEVER use `src.` prefixes in imports within `src/`.
- **Example**: Use `from domain.models import X`, NOT `from src.domain.models import X`.
- **Validation**: Check `spec.__class__.__module__` if patterns fail to match; it MUST NOT have a `src.` prefix.

### 3. Hexagonal Boundaries
Logic must flow from Domain out to Infrastructure.
- **Domain**: Entities, Value Objects, Mappers, and Specifications. Zero framework dependencies.
- **Application**: Use Cases and Port Interfaces. Orchestrates domain logic.
- **Infrastructure**: Adapters (DB, Auth, Scraper, Sentry). Implements ports.
- **Presentation**: Thin rendering layer (FastAPI/Streamlit). Complex UI logic belongs in `src/presentation/adapters/parsers.py`.

### 4. Specification Pattern
Decouple "what to filter" from "how to query".
- Define filtering logic in `src/domain/specifications.py`.
- Translate to SQL in `DuckDBSpecificationTranslator` (Infrastructure layer).
- Compose specifications (Urgente + Vencido) in Use Cases.

### 5. Observability & Resilience
- **Metrics**: Implement Golden Signals (Latency, Traffic, Errors, Saturation) for all new scrapers or heavy I/O.
- **Sentry**: Initialize in presentation layer. Release-tag with `GIT_SHA`. Redact PII from breadcrumbs (LGPD).
- **Graceful Degradation**: Always handle infrastructure failures (Redis down, Parquet corrupt) without crashing. Fall back to direct queries or cached state.

### 6. Humble Object Pattern (Presentation)
All complex logic in `app_analytics.py` must be extracted to pure Python adapters.
- **Rule**: The UI must only render. Sanitization, parsing, and data shaping belong in `src/presentation/adapters/parsers.py`.
- **Goal**: Enable unit testing of UI logic without running Streamlit.

### 7. Legacy Code Eradication
Treat legacy code (raw SQL, hardcoded strings) as a toxin.
- **Boy Scout Rule**: Refactor legacy SQL to the Specification Pattern (ADR-004) whenever you touch a module.
- **ACL**: Force all legacy interactions through an Anti-Corruption Layer until they are completely removed.

### 8. Blameless Post-Mortem Loop
Every failure is a learning opportunity for the system, not a blame game.
- **Protocol**: After fixing a bug, identify the systemic cause (e.g., identity mismatch) and implement structural prevention (e.g., fallback matching).
- **Documentation**: Update `docs/adr/` or `docs/rules/` to share the systemic fix.

## Workflow Decision Tree

1. **New Feature?**
   - Start with `docs/adr/` if architectural.
   - Define Domain models and Specifications.
   - Write failing Domain tests.
   - Implement Mappers and Specifications.

2. **New Scraper?**
   - Implement `ScraperUseCase` with Circuit Breaker (5% threshold).
   - Add Prometheus metrics.
   - Implement DLQ for "poison pills".
   - Test with Chaos Engineering (simulated timeouts/failures).

3. **Bug Fix?**
   - Write a regression test that reproduces the failure.
   - Fix the logic.
   - Verify 0 mutants survive in the fix area using `mutmut`.

## Verification Checklist
- [ ] Imports are clean (no `src.` prefix).
- [ ] Unit tests pass AND cover boundary conditions.
- [ ] Mutmut reports 0 survivors in Core Domain.
- [ ] ADR and Glossary updated.
- [ ] Sentry breadcrumbs are LGPD-compliant.
- [ ] Complex UI logic is isolated (Humble Object).
- [ ] Legacy code in the module has been eradicated or encapsulated (ACL).
- [ ] Structural prevention implemented for the reported failure (Post-Mortem).
