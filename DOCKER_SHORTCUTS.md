# GER Healthcare Analytics Docker Quick Reference

Quick guide for orchestration, monitoring, and debugging the **GER (Gestão de Estratégia de Rede)** infrastructure.

## Project Lifecycle (via Makefile)

| Command | Action |
| :--- | :--- |
| `make up` | Start Core Analytics (Streamlit only, no IAM) |
| `make up-iam` | Start FULL Stack (App + Keycloak + OAuth2 Proxy) |
| `make bootstrap` | Start ONLY IAM infra for initial setup/configuration |
| `make sync` | Pull from GitHub and Full Build (SOTA Sync) |
| `make update` | Fast pull from GitHub (Code changes only) |
| `make logs` | Real-time stream of Analytics container logs |
| `make restart` | Restart all active containers |
| `make down` | Stop and remove all containers |
| `make clean` | Prune old images and Docker clutter |
| `make clean-volumes`| Hard Reset: Nuke all persistent volumes (Erase DB data) |

---

## 🔍 Inspection & Monitoring

### Status & Health
```bash
# Check running containers and health status
docker compose --env-file env/creds.env --env-file env/config.env ps

# View resource consumption (CPU/Memory)
docker stats
```

### Logging (SRE Focused)
```bash
# General Streamlit Analytics logs
make logs

# Follow OAuth2 Proxy (Authentication Gate)
docker compose --env-file env/creds.env --env-file env/config.env logs -f oauth2-proxy

# Follow Worker (Async Tasks)
docker compose --env-file env/creds.env --env-file env/config.env logs -f worker

# Follow Keycloak (Identity Provider)
docker compose --env-file env/creds.env --env-file env/config.env logs -f keycloak
```

---

## 🛠️ Interactive Access

| Target | Command |
| :--- | :--- |
| **Analytics Shell** | `docker exec -it ger_analytics bash` |
| **Worker Shell** | `docker exec -it ger_arq_worker bash` |
| **Keycloak DB Shell**| `docker exec -it ger_keycloak_db psql -U admin_stangler -d keycloak` |
| **Redis (Queue)** | `docker exec -it ger_redis_queue redis-cli` |

---

## 🌐 Connectivity Map (Local Access)

Based on `EXTERNAL_DOMAIN=127.0.0.1.nip.io`.

| Service | Port (Host) | Access URL |
| :--- | :--- | :--- |
| **BFF (Entry Point)** | `80` | [http://127.0.0.1.nip.io](http://127.0.0.1.nip.io) |
| **Keycloak Admin** | `8080` | [http://localhost:8080](http://localhost:8080) |
| **Worker Metrics** | `8000` | [http://localhost:8000/metrics](http://localhost:8000/metrics) |
| **OAuth2 Health** | `80` | [http://127.0.0.1.nip.io/ping](http://127.0.0.1.nip.io/ping) |
| **PostgreSQL** (IAM) | `5432` | `localhost:5432` (Only if exposed) |

> [!NOTE]
> Streamlit (8501) is part of the internal mesh and **not exposed** to the host. Always access via the Proxy URL.

---

## 🔒 Security & IAM Components

| Component | Role | Logic |
| :--- | :--- | :--- |
| **OAuth2 Proxy** | Zero Trust Gate | Reverses proxy to `ger_analytics:8501` after OIDC check |
| **Keycloak** | IdP | Manages user identities and "gercon-realm" |
| **Redis Session** | Token Cache | Stores JWTs and sessions for the proxy |
| **Kafka** | Event Bus | SOTA Sync between Auth and Domain events |

---

## 🧼 Maintenance & Deep Cleanup

> [!CAUTION]
> `make clean-volumes` is destructive. It will erase the PostgrSQL database (Keycloak configurations) and Kafka data.

```bash
# Wipe everything and restart clean
make down
make clean-volumes
make up-iam
```

### Docker Daemon Level (If local engine hangs)
```bash
# System level cleanup (Linux)
sudo systemctl restart docker
docker system prune -a --volumes
```
