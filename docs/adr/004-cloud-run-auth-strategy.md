# ADR-004: Cloud Run Authentication & Permanent User Database

## Status
**Accepted** — 2026-04-07

## Context

The GER system migrated from a Docker Compose RDE (with Keycloak + OAuth2-Proxy + PostgreSQL)
to **Google Cloud Run serverless**. This creates a fundamental architecture mismatch:

| Capability | Docker Compose RDE | Cloud Run Serverless |
|---|---|---|
| Identity Provider | Keycloak (stateful, PostgreSQL-backed) | ❌ Not available |
| Auth Proxy | OAuth2-Proxy (sidecar, Redis sessions) | ❌ Not available |
| JWT Headers | `x-forwarded-access-token` injected by proxy | ❌ No proxy to inject |
| User Database | PostgreSQL (Keycloak schema) | ❌ Ephemeral filesystem |
| Session Store | Redis (OAuth2-Proxy cookies) | ❌ No Redis |

**Problem**: The current `get_authenticated_user()` expects IAP Proxy headers that don't
exist on Cloud Run, causing **100% of users to see "Acesso Bloqueado"**.

**Constraint**: `--allow-unauthenticated` was removed from Cloud Run, so GCP's IAM layer
blocks all HTTP requests before they reach the Streamlit app.

## Decision

### Phase 1 — Immediate Unblock (this PR)

1. **Re-add `--allow-unauthenticated`** to Cloud Run deployment.
   - WHY: The Streamlit app must be reachable to render its own auth UI.
   - Security: Application-level auth (domain layer) replaces infra-level IAM gating.

2. **Implement `CloudRunAuthAdapter`** using **Google Cloud Identity Platform (Firebase Auth)**.
   - WHY: It's the GCP-native managed identity provider, serverless, and free-tier generous.
   - Firebase Auth provides: email/password, Google SSO, custom claims (CRM roles).
   - No infrastructure to manage — fully managed by GCP.

3. **Use Firestore** for persistent user profiles (CRM, roles, preferences).
   - WHY: Serverless, auto-scaling, free tier (50K reads/day, 20K writes/day).
   - Stores domain-specific claims (crm_numero, crm_uf, clinical roles).
   - Alternative considered: Cloud SQL PostgreSQL (~$7-15/mo) — rejected for MVP
     due to operational overhead and cost for <50 users.

### Phase 2 — Future (post-MVP)

4. Migrate to **Google Cloud IAP** if the team moves to Google Workspace.
   - Cloud IAP is the true "Zero Trust" GCP solution.
   - Requires Google Workspace or Cloud Identity organization.

## Architecture (Phase 1)

```
┌──────────────────────────────────────────────────────┐
│                   Cloud Run (Serverless)              │
│                                                       │
│  ┌─────────────┐   ┌──────────────────────────────┐  │
│  │  Streamlit   │──▶│  get_authenticated_user()    │  │
│  │  (BFF)       │   │                              │  │
│  └─────────────┘   │  K_SERVICE? ─▶ Firebase Auth  │  │
│                     │  else      ─▶ Keycloak/IAP   │  │
│                     └──────────┬───────────────────┘  │
│                                │                      │
│                     ┌──────────▼───────────────────┐  │
│                     │  Firestore (User Profiles)   │  │
│                     │  CRM, Roles, Preferences     │  │
│                     └──────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

## Consequences

### Positive
- Users can access the app on Cloud Run immediately.
- User database is permanent, serverless, and free-tier.
- Clean separation: Firebase Auth handles "who are you?", Firestore handles "what can you do?".
- The `ValidatedUserToken` domain model remains unchanged (Hexagonal Architecture preserved).

### Negative
- Two auth adapters to maintain (Keycloak for RDE, Firebase for Cloud Run).
- Firebase dependency (vendor lock-in to GCP).
- Custom claims sync needed between Firebase Auth and Firestore.

### Risks
- `--allow-unauthenticated` exposes the app URL publicly (mitigated by app-level auth).
- Firebase free tier limits may not suffice if user base grows significantly.

## Alternatives Considered

| Option | Pros | Cons | Decision |
|---|---|---|---|
| Cloud SQL PostgreSQL | Full Keycloak compatibility | $7-15/mo, operational overhead | Rejected (MVP) |
| GCS JSON file for users | Zero cost, already mounted | No ACID, no auth provider | Rejected |
| Streamlit `st.secrets` | Simplest | No user management, hardcoded | Rejected |
| Google Cloud IAP | True Zero Trust | Requires Google Workspace | Future (Phase 2) |
| **Firebase Auth + Firestore** | **Managed, serverless, free** | **GCP vendor lock-in** | **Accepted** |
