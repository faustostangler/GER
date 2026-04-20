# 011: Clean Architecture Infrastructure Leakage Prevention

## Status
✅ Active (2026-04-19)

## Context
As the application grew, infrastructure details like `os.path.isfile(settings.OUTPUT_FILE)` began to leak into the Application layer (`AnalyticsUseCase`) and Presentation layer (`app_analytics.py`). This violated the Dependency Rule in Clean Architecture by coupling the core business orchestration and UI directly to filesystem checks and the `infrastructure.config` settings.

## Decisions

### 1. Domain Exception Standardization
- **Action**: Created `DataNotReadyError` inside `src/domain/models.py`.
- **Reason**: The Domain and Application layers must communicate errors using Domain constructs, not generic system errors (like `FileNotFoundError`) or by relying on the UI to perform infrastructure validation. This establishes a Ubiquitous Language for system unreadiness.

### 2. Strict Repository Port Isolation
- **Action**: Added the `verify_data_readiness()` method contract into `IAnalyticsRepository` (Application Port).
- **Implementation**: `DuckDBAnalyticsRepository` (Infrastructure Adapter) implements `verify_data_readiness()` using the underlying infrastructure dependencies (`os.path`, `self.db_file`). If the data is missing, it raises the Domain Exception (`DataNotReadyError`).
- **Reason**: The Application Use Case must orchestrate functionality, not interrogate the disk. It safely delegates data health checks to the Repository. This prevents `infrastructure.config` or standard IO libraries from polluting the Application layer.

### 3. UI as a Humble Object (Exception Translation)
- **Action**: `app_analytics.py` executes a Pre-Flight Readiness Check by invoking `use_case.verify_data_readiness()`.
- **Reason**: The UI shouldn't know about `settings.OUTPUT_FILE` or OS behavior anymore. It catches the expected `DataNotReadyError` and gracefully displays user-friendly visual feedback (showing `st.error` and instructions on executing the data pipeline).

## Architectural Rules (SOTA Standards)

1. **Zero Infrastructure in Application Layer**: Use Cases MUST NOT import `from infrastructure...`. All infrastructure details (e.g., config, database files, loggers) must either be injected via DI or delegated to a Port (Repository). Integration tests must actively prohibit these imports (e.g., via AST node evaluation assertions).
2. **Domain Native Exceptions**: External systems (databases, APIs, filesystem) must translate their specific faults (e.g., `FileNotFoundError`, `duckdb.IOException`) into Domain Exceptions (e.g., `DataNotReadyError`, `DomainContractViolationException`) before crossing the boundary into the Application or Presentation layer.
3. **Pre-flight Checks in UI**: UI Orchestrators should wrap critical Use Case invocations in `try...except` blocks that watch for exact Domain Exceptions, rendering appropriate visual fallbacks.
