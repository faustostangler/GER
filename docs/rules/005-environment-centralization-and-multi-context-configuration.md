# Rule 005: Environment Centralization and Multi-Context Configuration

## 1. Context and Problem Statement
The GER system was previously suffering from "Context Friction," where switching between local development (`127.0.0.1.nip.io`) and remote deployment required manual code changes in multiple files (`docker-compose.yml`, `config.py`, `streamlit_auth.py`). Hardcoded service names and hostnames violated the **12-Factor App** principles and introduced risks of environment leakage.

## 2. The Three-Pillar Configuration System
We have centralized all environment variables into the `env/` directory, following a strict separation of concerns:

| File | Purpose | Git Tracking |
| :--- | :--- | :--- |
| `env/compose.env` | **Canonical Source** for Docker Compose YAML interpolation. Defines hostnames, service names, and subdomains. | **Tracked** |
| `env/config.env` | **Runtime Config** for application containers (Analytics, Worker). Defines business logic toggles and non-sensitive infra paths. | **Tracked** |
| `env/creds.env` | **Secrets and Credentials** (DB Passwords, Client Secrets, Sentry DSNs). | **Ignored** |
| `.env` (root) | **Merged Output**. Generated dynamically by `make env` from the above files. Used by Docker Compose as the default variable provider. | **Ignored** |

## 3. Implementation Details

### A. Makefile Orchestration (`make env`)
The project root `.env` is a derivative artifact. It is generated via the Makefile to ensure that changing a variable in `env/compose.env` propagates correctly to the Docker Compose engine:
```makefile
env:
	cat env/compose.env env/config.env > .env
	if [ -f env/creds.env ]; then cat env/creds.env >> .env; fi
```

### B. Split-Horizon DNS and Parameterized Mesh
Hardcoded references like `keycloak:8080` were replaced with variables in `docker-compose.yml`. This allows for "Split-Horizon" discovery:
- **Internal Service Name**: used by `analytics` to talk to `keycloak` within the Docker network (e.g., `IAM_INTERNAL_SERVICE:KEYCLOAK_INTERNAL_PORT`).
- **External Domain**: used by the browser/UI for redirects and JWT issuer validation (e.g., `${IAM_SUBDOMAIN}.${EXTERNAL_DOMAIN}`).

### C. Configuration Validation (Pydantic)
The `src/infrastructure/config.py` uses `pydantic-settings` to enforce type safety and provide computed fields for dynamic URL building:
- **`base_url`**: Automatically constructed using `PROTOCOL` and `EXTERNAL_DOMAIN`.
- **Logout URL**: Built dynamically to point to the correct Keycloak endpoint based on the deployment context.

## 4. Operational Workflows

### Switching to Local Development
1. Edit `env/compose.env`: Set `EXTERNAL_DOMAIN=127.0.0.1.nip.io`.
2. Edit `env/config.env`: Set `PROTOCOL=http`.
3. Run `make env && make up-iam`.

### Switching to Remote Production
1. Edit `env/compose.env`: Set `EXTERNAL_DOMAIN=yourproduction.com`.
2. Edit `env/config.env`: Set `PROTOCOL=https`.
3. Ensure `env/creds.env` contains production secrets.
4. Run `make env && make up-iam`.

## 5. Decision Records (ADR Alignment)
- **ADR-004 Integration**: The Auth adapter now strictly receives its configuration from the centralized `settings` object, ensuring no I/O leakage into the presentation layer.
- **Fail-Fast Principle**: If any required variable (like `KEYCLOAK_INTERNAL_SERVICE`) is missing from the `.env` generation or `config.env`, the application will crash during the `Pydantic` validation phase before the server starts.
