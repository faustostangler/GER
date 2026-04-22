# Rule 013: IAM Troubleshooting and Zero-Trust Authorization

## Context
The GER system utilizes a sophisticated IAM stack involving Keycloak, OAuth2-Proxy, and Nginx. This rule codifies troubleshooting protocols for common failures in the identity and clinical authorization flow.

## 1. Identity Infrastructure Integrity (Keycloak)
**Problem**: Keycloak fails to recognize users or clients due to corrupted or incomplete `realm-export.json`.
*   **Root Cause**: Exporting from the Keycloak UI without including secrets/users results in masked fields (`**********`) and missing `users` blocks.
*   **Protocol**:
    *   **Verify Secrets**: Ensure `KEYCLOAK_CLIENT_SECRET` in `realm-export.json` matches `env/creds.env`.
    *   **User Persistence**: The `users` block must be preserved in the export file for local RDE portability.
    *   **Hard Reset**: To force a clean re-import of the fixed file, stop Keycloak and remove the persistent volume:
        ```bash
        docker compose stop keycloak postgres-keycloak
        docker compose rm -f keycloak postgres-keycloak
        docker volume rm ger_keycloak_data
        make up-iam
        ```

## 2. Nginx Buffer Limits (Large JWTs)
**Problem**: `400 Bad Request: Request Header Or Cookie Too Large` from Nginx.
*   **Root Cause**: Keycloak tokens (ID/Access/Session) are large. OAuth2-Proxy injects these into headers/cookies, exceeding Nginx's default 4k/8k buffers.
*   **Protocol**:
    *   Configure `large_client_header_buffers` and `client_header_buffer_size` in `infra/router/nginx.conf`.
    *   Standard SRE Fix:
        ```nginx
        large_client_header_buffers 4 32k;
        client_header_buffer_size 8k;
        ```

## 3. Zero-Trust CRM Authorization (AuthZ vs AuthN)
**Problem**: "Acesso Clínico Bloqueado" error despite successful login.
*   **Context**: Adheres to **ADR-006**. Keycloak handles Identity (AuthN), but the application manages Clinical Authorization (AuthZ) via the local `DoctorProfile` entity.
*   **Root Cause**: The user's `sub` ID is not present in the local `doctor_profiles` table or `crm_verified` is false.
*   **Diagnostic Protocol**:
    1.  Get the `sub` ID from the error message.
    2.  Check the application database: `SELECT * FROM doctor_profiles WHERE id = 'SUB_ID';`.
    3.  If missing, check if the user was part of a `realm-export.json` import (which bypasses the Kafka SPI events) or if the Kafka consumer logs show a CFM validation failure.
*   **Resolution (Manual Intervention)**: Manually verify and insert the profile to unblock access:
    ```sql
    INSERT INTO doctor_profiles (id, crm_numero, crm_uf, crm_verified) 
    VALUES ('SUB_ID', 'CRM_NUMBER', 'UF', true);
    ```

## 4. Synchronization Gaps (Imports vs Events)
**Context**: The `keycloak_kafka_consumer` only reacts to real-time `USER_REGISTERED` events.
*   **The Gap**: Users imported via the `realm-export.json` during Keycloak's `--import-realm` bootstrap DO NOT trigger Kafka events.
*   **Constraint**: Every user imported manually or via IaC must have a corresponding entry manually created in the `doctor_profiles` table if they are to be used immediately for clinical modules.
*   **Future Mitigation**: A migration script or a startup sync task should be considered to reconcile Keycloak users with the `DoctorProfile` store.

## 5. Observability and Sentry
*   **Release Tagging**: Always ensure `GIT_SHA` is injected during builds for Sentry correlation.
*   **PII Filtering**: Breadcrumbs must redact SQL queries containing patient PII to remain LGPD compliant.
