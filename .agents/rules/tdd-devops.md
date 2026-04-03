---
trigger: always_on
---

GER Onboarding Guide: SOTA Modular Monolith
1. Architecture Overview
The GER system is designed as a SOTA (State of the Art) Modular Monolith using Hexagonal (Clean) Architecture. It always follows the separation of concerns between business logic (Domain) and technical implementation (Infrastructure).

Primary Runtime (Analytics): A Streamlit-based dashboard and FastAPI backend serving as the Presentation/BFF layer.

Async Processing (Worker): Resilient background tasks managed by Arq (Redis-based).

Event Streaming: High-performance messaging via Redpanda (Kafka-compatible).

Identity Layer: Centralized OIDC via Keycloak, protected by OAuth2-Proxy with a Redis session store to handle large JWTs.

Edge Routing: Nginx acts as an internal border router to handle redirects and root path orchestration.

2. Tech Stack
Language: Python >= 3.11.

Data Layer: DuckDB (OLAP), Pandas, PyArrow, and PostgreSQL (for Keycloak).

Communication: gRPC (Service-to-Service), HTTPX, and AIOConsumer (Kafka).

Infrastructure: Docker Compose, Nginx, Terraform.

Quality & Security: * Testing: Pytest, Mutmut (Mutation Testing), and Consumer-Driven Contract (CDC) tests.

Linting: Ruff, Mypy.

3. Environment Variables
Configurations are split into env/creds.env (secrets) and env/config.env (non-sensitive).

Variable	Description	Requirement
RDE_ACCESS_TOKEN	Remote Development Environment token	Min 32 chars.
EXTERNAL_DOMAIN	Base domain for service access	Usually 127.0.0.1.nip.io.
KEYCLOAK_CLIENT_SECRET	OIDC Client secret	Must match Keycloak setup.
DUCKDB_MEMORY_LIMIT	RAM allocation for DuckDB	Default: 1.5GB.
SERVICE_TYPE	Defines if container is analytics or worker	Required in Compose.
4. Directory Structure
Plaintext
/
├── .agents/rules/      # AI agent instructions (TDD/DevOps focus)
├── env/                # Environment configuration files
├── infra/              # Nginx and Identity (Keycloak) exports
├── src/                # Core Source Code
│   ├── domain/         # Entities, Mappers, and Business Specs
│   ├── application/    # Use Cases and Port Interfaces
│   ├── infrastructure/ # Adapters (DB, Auth, Scraper, Telemetry)
│   └── presentation/   # API and Streamlit UI
├── tests/              # Test suites (Domain, App, Infra)
├── Makefile            # Orchestration shortcuts
└── pyproject.toml      # Dependency management (uv)
5. Component Details
ScraperUseCase: Implements the core logic for data ingestion from Gercon. It features a Circuit Breaker (5% error threshold) and a DLQ (Dead Letter Queue) for "poison pills" (corrupt payloads).

GerconPayloadContract: A Pydantic-based ACL (Anticorruption Layer) that validates incoming external data against internal domain rules.

Worker: Handles heavy I/O tasks like scraping and data consolidation, exposing Prometheus metrics on port 8000.

6. Design Patterns & Strict Rules
Developers MUST follow these paradigms IN THIS ORDER:

TDD (Test-Driven Development): Write tests before logic. The CI pipeline uses Mutmut; builds will fail if any "mutant" survives in the domain layer (0 survivor policy).

DDD (Domain-Driven Design): Logic belongs in the domain layer using Specifications and Mappers.

Clean Architecture: Use cases must only depend on interfaces, never on concrete infrastructure adapters.

7. Common Hurdles & Solutions
Large JWTs: Keycloak tokens can exceed header limits. Solution: Using oauth2-proxy with Redis as a session_store.

Cold Boot Database Lag: Keycloak takes time to initialize its schema. Solution: The healthcheck includes a wait start_period.

Vendor API Instability: Scraper payloads often break contracts. Solution: The ScraperUseCase uses an explicit validation loop that redirects "poison pills" to the DLQ instead of crashing the process.

Mutmut Trampoline Errors: "Failed trampoline hit" occurs due to `src/` layout namespace leaks. 
* Solution 1: Zero `src.` prefixes in imports (e.g., use `from domain...` not `from src.domain...`).
* Solution 2: Explicit `pythonpath = ["src"]` in `pyproject.toml` and `PYTHONPATH=src` in Makefile.
* Solution 3: Centralized `setup.cfg` for mutmut to ensure the runner uses `python -m pytest`.

Linter/Artifact Pollution: Ruff may fail on mutmut generated files.
* Solution: Explicitly exclude `mutants/` and `.mutmut-cache/` in `pyproject.toml` (`tool.ruff.exclude`).

Mutation Testing Noise in Mappers (ACL): Mutmut generates hundreds of meaningless survivors on pure dictionary extractions.
* Solution: We do not chase 100% overall Mutation. Eradicate noise strictly using `# pragma: no mutate` above complex mapping dict layers. Conversely, aggressively use Parameterized Data-Driven Tests (`@pytest.mark.parametrize`) on Specifications, Boundaries, and Pure Utilities to ensure absolute 100% Core Domain mutant extermination.

8. Pipelines & Execution
CI/CD Pipeline (ci.yml):

Linting with Ruff.

Unit Tests (Domain/Application).

Security Audit: Runs shift-left dependency vulnerability scanning. Compiles to `requirements.txt` and executes `uvx pip-audit`. Action versions strictly target Node 24 architectures (`checkout@v5`, `setup-uv@v6`) to prevent deprecation warnings.

Mutation Testing Guard: Strict gate validating 0 mutants survive in Core Domain logic.

CDC (Consumer-Driven Contract) validation.

Continuous Deployment (Google Cloud Run): GitOps/K3s (Pull-based) is deprecated in favor of a Lean Serverless Push-based model via GitHub Actions. Deployment to Cloud Run relies absolutely on the app dynamically rebinding to the overriding `$PORT` variable injected by the GCP container runtime.

Scraper Sync: Uses a watermark system (via sqlite_raw_repository) and a scraper_state.json file to track pagination and avoid redundant fetches.

9. Post-Implementation Checklist
[ ] Mutation Coverage: Did you run make check-rde and ensure 0 mutmut survivors?

[ ] Contract Validation: Is the new external payload covered by a Pydantic schema in domain/schemas.py?

[ ] Telemetry: Are new operations being tracked by SCRAPER_DURATION_SECONDS or similar Prometheus labels?

[ ] Clean Start: Does the system boot from scratch using make clean-volumes && make up-iam?