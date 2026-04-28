# ADR-006: Domain Authorization and Thundering Herd Mitigation

## Status
Accepted

## Context

We identified two critical technical debts in the GERCON authentication and authorization flow:

1. **SRE Vulnerability: "Thundering Herd" on JWKS Refresh**
   The JWT validator fetched Keycloak's JSON Web Key Set (JWKS) to verify token signatures. When the cache expired (or keys rotated), multiple concurrent API requests (e.g., resulting from a frontend retry storm or burst traffic) would independently encounter a cache miss and trigger simultaneous HTTP requests to Keycloak. This "Thundering Herd" behavior risks exhausting Keycloak's connection pool, leading to cascading failures across our microservices.

2. **DDD Violation: Identity Provider Acting as Authorization Provider**
   The system implicitly trusted the JWT claims (`crm_numero`, `crm_uf`) provided by Keycloak for clinical authorization. 
   Keycloak is an Identity Provider (AuthN - "Who are you?"), not a Clinical Authorization Provider (AuthZ - "Are you legally allowed to practice?"). Depending on Keycloak claims meant that our Core Domain (clinical regulation) was coupled to OIDC mapping rules, and bypassing rigorous clinical status validation against the Federal Council of Medicine (CFM).

## Decision

We enacted a two-phase architectural refactoring to harden IAM and Resilience:

### 1. Thundering Herd Mitigation via Double-Checked Locking
In `src/infrastructure/auth/jwt_validator.py`, we implemented the thread-safe **Double-Checked Locking** pattern to govern JWKS fetching.
*   We use a Python `threading.Lock` (since FastAPI handles synchronous background threads for our middleware).
*   When a cache miss occurs, the current thread attempts to acquire the lock. 
*   Once acquired, the thread performs a *second* cache check. If another thread already refreshed the cache while the current thread was waiting for the lock, it immediately reuses the fresh cache, preventing redundant network calls.

### 2. Domain Authorization via `DoctorProfile` Entity
We decoupled AuthZ from AuthN by establishing Keycloak strictly for Identity (verifying the UUID `sub`) and establishing a local `DoctorProfile` entity for Authorization.
*   **Domain Entity:** Created `DoctorProfile` in `src/domain/identity.py`, containing `crm_numero`, `crm_uf`, and `crm_verified`. It acts as the boundary truth.
*   **Authorization Gate Predicate:** The entity provides an `is_authorized()` method. It requires `crm_verified` to be `True`.
*   **Infrastructure Adapter:** The `jwt_validator.verify_token` method now acts as an Anti-Corruption Layer. It extracts the `sub` (Identity ID) and queries the local repository for a `DoctorProfile`. 
*   **Strict Segregation:** The API will return `403 Forbidden` if the `DoctorProfile` does not exist or `is_authorized()` is `False`, cleanly separating it from a `401 Unauthorized` (Token invalid/expired). `ValidatedUserToken` no longer depends on JWT claims for clinical fields but strictly on the domain lookup.
*   **Event-Driven Ingestion:** We updated the `keycloak_kafka_consumer` to intercept `USER_REGISTERED` events. It uses an adapter to validate CRM details with the CFM API before persisting the newly constructed `DoctorProfile`. 

## Consequences

### Positive
*   **Resilience (MTTR/Failure Rates):** Guaranteed at most one outbound JWKS request per Keycloak realm during a cache invalidation window event under extreme concurrency.
*   **Architectural Purity:** Clean Hexagonal decoupling. Keycloak handles passwords/2FA (Identity); GER handles CRM validation (Domain Authorization).
*   **Security Posture:** Zero-Trust approach. A compromised JWT claim with fake CRM numbers will be ignored, as authorization solely relies on our protected `DoctorProfile`.

### Negative / Trade-offs
*   **Performance Hit:** A minimal lookup cost is added to every API request to fetch the `DoctorProfile`. This must be mitigated by an ultra-fast Redis cache in the `DoctorProfileRepository` implementation.
*   **Complexity:** The Kafka consumer now requires a DLQ for CFM API timeouts or failed validations, increasing the asynchronous orchestration logic.

## References
*   [Double-Checked Locking Pattern](https://en.wikipedia.org/wiki/Double-checked_locking)
*   Rule `012-test-stability-custom-components-and-security-hardening.md`
