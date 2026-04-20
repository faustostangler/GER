# Rule 012: Test Stability, Custom Components, and Security Hardening

## Context
During the evolution of the GER dashboard into a high-performance "clinical surgeon" UI, we implemented custom HTML/CSS components to replace standard Streamlit widgets. This shift, while essential for aesthetics and UX, introduced new challenges in E2E testing, middleware isolation, and pipeline security.

## 1. Selector Reliability for Custom Components
**Problem**: Standard Playwright selectors like `[data-testid="stMetricValue"]` fail when widgets are replaced by raw HTML (Humble Object pattern).
**Rule**: UI components using custom HTML must have stable CSS classes (e.g., `.kpi-value`, `.kpi-card`) for E2E identification.
**Action**: Update `tests/e2e/test_dashboard_flow.py` whenever a widget is "pixel-perfected" into a custom component.

## 2. Presentation Middleware Test Purity
**Problem**: Tests for `auth_middleware.py` were previously mocking internal calls to `infrastructure.auth.streamlit_auth.build_logout_url`. This coupled the presentation logic to a specific infrastructure adapter.
**Rule**: Presentation Middlewares must receive their dependencies (like the logout URL) as explicit parameters.
**Benefit**: Tests become pure, verifying the *rendering logic* without needing to mock half of the infrastructure stack. 
**Logic**: `render_user_widget(user, logout_url=...)` instead of internal service discovery.

## 3. Resilient Identity Translation (Defensive Specs)
**Problem**: Small changes in `FiltroAvancadoSpec` (like removing `busca_avancada` for a refactor) can break infrastructure translators if not guarded.
**Rule**: Translators in the infrastructure layer must be tested against `FakeFiltro` objects that strictly mirror the expected Domain contract.
**Protocol**: When refactoring specification attributes, verify that the `DuckDBCriteriaTranslator` handles the absence or presence of these attributes gracefully (AttributeError prevention).

## 4. Security-First Lifecycle (Audit Gates)
**Problem**: `make ci` contains a `test-audit` step that fails the build on any known CVE in the dependency tree.
**Rule**: Dependency vulnerabilities (e.g., `cryptography`) are treated as **Blockers**.
**Action**: Immediate update via `uv add "package>=safe_version"` is mandatory to restore pipeline health. Local development must never bypass the audit gate.

## 5. Clean Code: Unused Artifacts
**Rule**: Logic artifacts like unused exception variables (`except Exception as e: pass` where `e` is not logged) must be eradicated via `ruff --fix` to minimize noise and maintain a SOTA codebase.

***

**Status**: Active
**Reference**: PR-Consolidation-012, ADR-006, ADR-008
**Tags**: #TDD #E2E #Security #CleanArchitecture #Streamlit
