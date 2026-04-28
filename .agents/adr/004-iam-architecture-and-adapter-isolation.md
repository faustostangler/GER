# Rule 004: State-of-the-Art (SOTA) IAM & Session Lifecycle Architecture

## 1. Architectural Strategy: IAM Adapter Isolation
The Identity & Access Management (IAM) logic has been fully decoupled from the presentation entry point (`app_analytics.py`). Security and identity resolution now live exclusively in a specialized adapter: `src/presentation/adapters/streamlit_auth.py`.

### Principles Applied:
- **Facade Pattern**: Exposing a single `require_authentication()` gatekeeper.
- **Middleware Orchestration**: Managing the session lifecycle automatically.
- **Humble Object Pattern**: Decoupling pure logic from UI side-effects to enable 100% test coverage.
- **Fail-Secure Architecture**: Defaulting to "Deny All" if environment detection or identity resolution is ambiguous.

## 2. The 3-Layer Authentication Lifecycle
The `require_authentication()` facade implements a strict 3-layered strategy for session management:

1. **Layer 1: Active & Valid Session**:
   - Checks `st.session_state` for an existing `user` and a non-expired `token_exp`.
   - Returns the user immediately (Zero I/O overhead on interaction reruns).
2. **Layer 2: Expired Token**:
   - Detects if a session exists but the 24h token window has closed.
   - Renders a **Renewal CTA** (Redirection to OAuth2-Proxy or session clear for Cloud Run).
   - Invokes `st.stop()` to prevent any data exposure.
3. **Layer 3: First-Time Load / Authentication**:
   - Detects environment (Dev Mock / Cloud Run Gate / IAP Proxy).
   - Resolves identity, populates session state, and triggers `st.rerun()`.

## 3. Canonical Application Structure
The application entry point follows the **"1-2-3 Rule"** for architectural purity:

```python
def main():
    # 1. Boot Infra & DX
    setup_ui() # Injects CSS, Sentry, and basic config
    
    # 2. Identity Gatekeeper
    user = require_authentication() # Single point of entry for security
    
    # 3. Domain Execution
    render_dashboard(user) # User-driven clinical logic
```

## 4. Multi-Runtime Security Guards
### Double-Guard Dev Mock:
- Activated ONLY if `ENVIRONMENT == "dev"` AND `ALLOW_UNAUTHENTICATED_DEV == "true"`.
- Prevents accidental bypass in production via environment variable leaks.

### Cloud Run Password Gate:
- Uses `CLOUD_RUN_AUTH_PASSWORD_HASH` (SHA-256) as the primary validator.
- Graceful fallback to plain text only in local development environments.

### Redirection Integrity:
- `build_logout_url()` dynamically constructs redirection chains: `OAuth2-Proxy -> Keycloak -> post_logout_redirect_uri`.
- Ensures users are correctly redirected back to the dashboard after clearing OIDC sessions.

## 5. UI & Humble Components
UI-dependent error messages and debug panels were extracted into internal helpers:
- `_render_auth_error()`: Visual feedback for missing headers or configuration errors.
- `_render_debug_headers()`: Protected by `APP__DEBUG`, allowing developers to audit IAP headers without exposing them to end-users.

## 6. Testing & Quality Gates
- **Total Tests**: 128 passing across domain, application, and presentation layers.
- **Identity Mocking**: Presentation tests must mock `_policy` and `ClinicaPolicy` to prevent `StreamlitMixedNumericTypesError`.
- **Assertion Migration**: UI tests must assert against `at.markdown` values for custom HTML/CSS headers instead of native `at.title` elements.
