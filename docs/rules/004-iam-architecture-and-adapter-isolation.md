# Rule 004: IAM Architecture & Adapter Isolation

## 1. Context & Objective
The Identity & Access Management (IAM) logic was previously tightly coupled within the `app_analytics.py` entry point. This created a high-risk area where security, infrastructure detection, and UI rendering were interleaved. The objective was to isolate IAM into a dedicated, testable adapter following **Hexagonal Architecture** and **Clean Architecture** principles.

## 2. The IAM Adapter (`streamlit_auth.py`)
Authentication logic is now encapsulated in `src/presentation/adapters/streamlit_auth.py`. This adapter acts as a **Facade**, shielding the business application from the complexities of different runtime environments.

### Supported Authentication Modes:
*   **OIDC/IAP (Production)**: Validates `x-forwarded-access-token` headers (from Keycloak/OAuth2-Proxy).
*   **Cloud Run Password Gate**: A lightweight security layer for serverless deployments using SHA-256 password hashing.
*   **Dev Mock**: A secure local bypass for rapid development.

## 3. Key Design Patterns
*   **Facade Pattern**: The application calls `resolve_authenticated_user()`, a single entry point that manages all environment detection and identity resolution.
*   **Humble Object Pattern**: Logic interacting with the global `st.session_state` or execution flow (`st.stop()`) is isolated from pure identity resolution logic, allowing for granular unit testing.
*   **Anti-Corruption Layer (ACL)**: External JWT headers are mapped directly to the internal `ValidatedUserToken` domain model using `token_acl.py`.

## 4. Security Enforcement Policies
### Double-Guard Dev Mock
To prevent accidental security bypasses in production-like environments, the Dev Mock is only activated if:
1.  `ENVIRONMENT == "dev"`
2.  `ALLOW_UNAUTHENTICATED_DEV == "true"` (Explicit opt-in)

### Cloud Run Password Gate
In serverless environments without an IAP proxy, a password gate is enforced.
*   Uses `CLOUD_RUN_AUTH_PASSWORD_HASH` for validation.
*   Prevents rendering any clinical data until the password challenge is solved.

### Dynamic Logout Resolution
Logout URLs are built dynamically based on the runtime:
*   **Keycloak**: Includes `post_logout_redirect_uri` for proper redirection.
*   **Cloud Run**: Clears the session state to reset the password gate.

## 5. Testing & Quality Gates
*   **100% Logic Coverage**: The adapter is covered by 18+ test cases in `tests/presentation/test_streamlit_auth.py`.
*   **TDD First**: Every authentication path (Token valid, Token expired, Password wrong, ENV mismatch) was defined by a failing test before implementation.
*   **Humble Test Strategy**: Uses `AppTest` from Streamlit to simulate UI rendering of the login gate while ensuring the Core Domain remains purely mocked.

## 6. SRE & Observability Integration
*   **Sentry Release Tagging**: The IAM resolution process serves as the hook to finalize Sentry initialization with the correct `GIT_SHA` and user context (with PII redacted).
*   **Fail-Secure Default**: If authentication resolution fails or is ambiguous, the system defaults to a "Deny All" state, stopping the Streamlit script execution.
