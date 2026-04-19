# 007: Module Identity and Defensive Architectural Integrity

## 1. Module Identity Discipline (The "System Identity" Rule)

### The Problem: Multi-Import Pollution
Python's `sys.modules` creates separate class objects for the same file if imported via different paths (e.g., `src.domain.specifications` vs `domain.specifications`). This causes `isinstance()` and `match-case` structural patterns to **fail silently** because the runtime identity of the objects does not match the imported class reference.

### Mandatory Directive
- **Absolute Rule**: Zero `src.` prefixes in imports within `src/` or entry points like `app_analytics.py`.
- **Reasoning**: The project uses `PYTHONPATH=src`. Using `from src.domain` instead of `from domain` violates the module boundary and breaks the Specification Pattern's translation logic.
- **Enforcement**: Any "no-filtering" or "unmapped specification" issue must first be audited for import hygiene.

## 2. Defensive Structural Mapping (Translator Robustness)

### Pattern: Name-Based Fallback
To mitigate potential runtime identity shifts (common in hot-reload environments like Streamlit), all Infrastructure Adapters (specifically `DuckDBCriteriaTranslator`) must implement a **Defensive Structural Mapping** pattern.

```python
# SOTA Defensive Translation
match spec:
    case _ if spec.__class__.__name__ == "PacienteUrgenteSpec":
        return "prioridade_clinica >= 4"
    # Fallback to direct class match
    case PacienteUrgenteSpec():
        ...
```

### Purpose
Ensures that even if a Module Identity violation occurs, the system maintains **Functional Integrity** by falling back to semantic name matching while a `CRITICAL` log or Sentry alert is triggered to fix the import.

## 3. Hexagonal Boundary Protection (Signature Integrity)

### The Principle: Clean Driving Adapters
Presentation components (Streamlit, CLI) are **Driving Adapters**. They must remain "dumb" regarding implementation details. 

### Avoid "Infrastructure Leak"
- **Leak**: Passing `AppSettings` or infrastructure-heavy objects into UI components.
- **Fix**: Inject `Policy` Value Objects (e.g., `ClinicaPolicy`) created at the entry point.
- **Maintenance**: Ensure that Use Case and Component signatures are strictly synchronized. A `TypeError` in the presentation layer (e.g., passing extra parameters like `policy=policy` to a component that doesn't expect it) can mask critical business logic failures.

## 4. Diagnostic Protocol (Silent Failure Troubleshooting)

When UI states appear correct but data does not reflect changes:

1. **Verify Data Boundary First**: Check the raw counts (KPIs) returned by the Use Case. If the count changed from the universe (e.g., 123k → 12k), the **Filter Pipeline is working**.
2. **Isolate the Rendering Layer**: If the data is filtered but the UI doesn't "look" right, look for `TypeError` or `AttributeError` in downstream tab rendering.
3. **Flush Telemetry**: Flush Redis (`ger_redis_session`, `fly_cache`) to ensure zero stale query results are polluting the investigation.
4. **Identity Audit**: Run `import sys; print(sys.modules.keys())` to check for duplicate entries (one with `src.` and one without).
