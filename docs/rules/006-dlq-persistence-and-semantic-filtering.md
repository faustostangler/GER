# 006: DLQ Persistence and Semantic Filtering Refactoring

## 1. Architectural Integrity: Dead Letter Queue (DLQ) Persistence

### Core Change
Replaced the volatile in-memory storage of "poison pill" payloads in the Scraper with a persistent SQLite-backed repository. This ensures data durability for SRE post-mortem analysis across container restarts.

### Components
- **Port (`IDLQRepository`)**: Defined in `src/application/use_cases/scraper_interfaces.py`. Decouples the Use Case from the specific database implementation.
- **Adapter (`SQLiteDLQRepository`)**: Implemented in `src/infrastructure/repositories/sqlite_raw_repository.py`. Manages the `dead_letter_queue` table and handles JSON serialization of payloads.
- **Dependency Injection**: Wired in `master_scraper.py` to inject the concrete repository into the `ScraperUseCase`.

### SRE Observability
Integrated Prometheus metrics into the persistence layer. The counter `scraper_errors_total` now includes `error_type="DLQ_PERSISTED"`, allowing real-time monitoring of invalid payloads being intercepted.

## 2. Presentation Layer Refactoring: Semantic Filtering

### Specification Pattern (ADR-004)
Completed the elimination of raw SQL string manipulation within the Streamlit dashboard (`app_analytics.py`). All UI filters now populate a `FiltroAvancadoSpecBuilder`, which generates a domain-pure `FiltroAvancadoSpec`.

### Bug Squashing: NameError Remediation
- **Signature Correction**: Fixed a mismatch where the `builder` parameter was misnamed as `clauses` in `render_advanced_text_search` and `render_smart_date_range`.
- **Consumer Cleanup**: Replaced all remaining references to the deprecated `clauses` list with `builder.build()` and `DuckDBCriteriaTranslator.translate(filters)`.

### Synchronization of KPI Models
Ensured that the `get_executive_summary` use case and raw SQL audit tabs consume the same filter logic. The use case now correctly receives the fully built `Specification` object, preventing the "unfiltered metrics" bug where data counts displayed 100% of the database despite active UI filters.

## 3. Operations & Troubleshooting

### Session State Management
Stale browser sessions can contain legacy state keys (e.g., the old `clauses` list) that conflict with the new builder-based filter logic.
**MANDATORY**: After architectural migrations affecting the UI state, a **Hard Refresh (Ctrl+Shift+R)** is required to synchronize the environment.

### Unit & Infrastructure Testing
- **DLQ Adapter Validation**: New tests in `tests/infrastructure/test_dlq_adapter.py` verify persistence and idempotency.
- **Scraper Persistence**: Updated `tests/application/test_scraper_use_case.py` to validate that validation failures automatically trigger a repository push.
