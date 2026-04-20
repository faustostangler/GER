# 008: Presentation Layer Decoupling & UI Humble Object Protocol

## Status
✅ Active (2026-04-19)

## Context
The `app_analytics.py` entrypoint was evolving into a "God Module" (Monolith), accumulating infrastructure configuration, dependency injection, domain dictionaries, and hundreds of lines of procedural UI logic (Sidebar). This violated the **Single Responsibility Principle (SRP)** and obscured the coordination logic of the "Maestro" layer.

## Decisions

### 1. Presentation DI Container (Composition Root)
- **Action**: Extract `get_use_case` and `get_identity_service` from UI entrypoints.
- **Location**: `src/presentation/di_container.py`.
- **Reason**: Decouple the UI from infrastructure details (Settings, DuckDB, Prometheus init, and Auth Adapters).
- **Benefit**: Allows other presentation layers (e.g., FastAPI) to reuse the same injection logic.

### 2. Sidebar Builder Pattern (Factory/Builder)
- **Action**: Relocate procedural Streamlit sidebar code (cascading filters, expanders, listener state) from `app_analytics.py`.
- **Location**: `src/presentation/builders/sidebar_builder.py`.
- **Implementation**: A `build_sidebar(use_case, builder, st_user)` function returns `(ui_filters, state_keys, curr_where)`.
- **Reason**: UI files must be **Humble Objects** (pixels/capture only). Complex layout assembly belongs in specialized builders.

### 3. Domain Constant Extraction (Ubiquitous Language)
- **Action**: Move `MAPA_NOMENCLATURAS` (the translation dictionary) from UI to Core Domain.
- **Location**: `src/domain/constants.py`.
- **Reason**: Avoid forcing the UI to understand the database schema. The Domain is the **Single Source of Truth** for business nomenclature. 

### 4. Infrastructure Dialect Isolation (SQL ACL)
- **Action**: Prohibit passing raw SQL strings (e.g., `FINAL_WHERE`) from UI to Use Cases or Repositories.
- **Pattern**: Use the **Specification Pattern**. UI components declare intent using `FiltroAvancadoSpec` objects.
- **Reason**: Decouple the Presentation from the database dialect (DuckDB). If the database changes to PostgreSQL, the UI remains untouched.
- **Placeholders**: Custom queries in components must use the `{FINAL_WHERE}` placeholder, which is injected solely by the Infrastructure Repository.

## Architectural Rules (SOTA Standards)

1. **SRP for Entrypoints**: `app_analytics.py` must have only ONE reason to change: the orchestration of main view tabs and global session flow.
2. **Humble UI**: Streamlit scripts must not contain SQL logic, business policies, or procedural builders exceeding 50 lines. Large UI blocks MUST be extracted to `builders/` or `components/`.
3. **Identity Pathing**: Never import using `from src.domain...` inside `src/`. Use `from domain...`. (Ref: Rule 007).
4. **Dialect Blindness**: Components MUST be database-dialect blind. They only handle Domain Specifications.
5. **State Management Isolation**: Component states (keys used in `st.session_state`) must be returned by the builders to the maestro layer to maintain visibility of global state context.

## Diagnostic Protocol
If the UI grows above 200 lines:
1. **Infrastructure?** -> Move to `di_container.py`.
2. **Large Dictionary?** -> Move to `domain/constants.py`.
3. **Procedural Filters/Sidebar?** -> Move to `builders/sidebar_builder.py`.
4. **Complex Metric Math?** -> Move to `presentation/adapters/`.
