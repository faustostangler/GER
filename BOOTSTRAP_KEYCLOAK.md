# 🔐 Bootstrap Guide: Keycloak Identity (SOTA)

This guide details the two ways to configure Keycloak to enable the project's Zero-Trust security mesh for the GER system.

---

## 🛠️ Option 1: Automatic Configuration (Recommended / IaC)

Our architecture follows the **Infrastructure as Code (IaC)** philosophy. The state of the Realm, Clients, and Redirect URIs is already declared in `infra/identity/gercon-realm-export.json`.

1. **Start the FULL Stack**:
   ```bash
   make up-iam
   ```
2. **What happens**: Docker injects the JSON into `/opt/keycloak/data/import/` and the `--import-realm` flag automatically applies the configurations at boot.
3. **Entry Point**: Access [http://127.0.0.1.nip.io](http://127.0.0.1.nip.io) to test the protected Analytics dashboard.

---

## 🖱️ Option 2: Manual Configuration (Step-by-Step)

Use this option if you want to configure the environment from scratch or understand the internal components.

### 0. Start ONLY the Identity Provider
```bash
make bootstrap
```
Access: [http://127.0.0.1.nip.io:8080](http://127.0.0.1.nip.io:8080) and log in with the credentials `admin_stangler` / `pass_stangler` (defined in your `env/creds.env`).

### 1. Create the Realm (App Domain)
The `master` realm is for global administration only. Let's create an isolated space.
1. In the upper left corner, click **Master** ➔ **Create Realm**.
2. **Realm name**: `gercon-realm`.
3. Click **Create**.

### 2. Create the Client (Proxy ID)
The `oauth2-proxy` needs a registration to identify itself.
1. In the side menu, click **Clients** ➔ **Create client**.
2. **Client ID**: `gercon-analytics`.
3. Click **Next**.
4. **Capability config**:
   - **Client authentication**: Switch to **ON** (Enables the Secret).
   - Keep **Standard flow** and **Direct access grants** enabled.
5. Click **Next**.
6. **Login settings**:
   - **Valid redirect URIs**: `http://localhost/*` and `http://127.0.0.1.nip.io/*`.
   - **Web Origins**: `*`.
7. Click **Save**.

### 3. Capture the Client Secret
1. In the `gercon-analytics` client, click the **Credentials** tab.
2. Copy the **Client Secret** value.
3. **Code Action**: Open your `env/creds.env` file and update `OAUTH2_PROXY_CLIENT_SECRET` and `KEYCLOAK_CLIENT_SECRET` with this code.

### 4. Configure CRM Attributes (Zero Trust Data)
For the Analytics dashboard to filter data by physician, the JWT Token must carry the CRM (Medical ID).

**Step A: Define User Profile**
1. Go to **Realm settings** ➔ **User profile** tab ➔ **Attributes** ➔ **Create attribute**.
2. Create `crm_numero` and `crm_uf`. Check **View** and **Edit** for the user and click **Save**.

**Step B: Map to the Token**
1. Go to **Clients** ➔ **gercon-analytics** ➔ **Client scopes** tab ➔ **gercon-analytics-dedicated** link.
2. Click **Add mapper** ➔ **By configuration** ➔ **User Attribute**.
3. Configure `crm_numero` and `crm_uf` (Name, User Attribute, and Claim Name should be identical). Save.

**Step C: OIDC Audience Mapper**
1. In the same **Client scopes** ➔ **gercon-analytics-dedicated** tab ➔ **Add mapper** ➔ **By configuration** ➔ **Audience**.
2. **Included Client Audience**: Select `gercon-analytics`. Enable `Add to access token`. Save.

### 5. Provision the First Identity
1. Ensure you are in the **gercon-realm**.
2. Click **Users** ➔ **Create new user**.
3. Fill in the fields (Username, Email, etc.). Click **Create**.
4. **Credentials** tab ➔ **Set password**. **TURN OFF** the **Temporary** switch to speed up your testing. Click **Save**.

---

## ⚡ Observability & Resilience (Redis)

Our security mesh uses **Redis** as the `session_store`.

* **Why?**: Keycloak JWT tokens can exceed the 4KB HTTP header limit (especially with many mappers/groups).
* **How it works**: `oauth2-proxy` stores the actual token in Redis and sends only a short `session_id` to the browser via a cookie.
* **Monitoring**: In production, check the health of the `redis-session` service if you encounter intermittent login failures ("Cookie too large").

---

## 🚀 Finishing the Setup
After any manual configuration, you can consolidate the state by running:
```bash
make up-iam
```
👉 Access the application at: **http://127.0.0.1.nip.io**
