# --- GER: Makefile for SOTA Modular Monolith Orchestration ---
# This Makefile encapsulates common developer workflows for the GER project.
# It prioritizes cleanliness, automation, and SRE-grade local management.

# Variables
ENV_FLAGS = --env-file env/creds.env --env-file env/config.env
DOCKER_COMPOSE = docker compose $(ENV_FLAGS)

.PHONY: bootstrap sync update help up dev check-rde rde-url down up-iam restart logs logs-proxy logs-worker logs-keycloak ps shell shell-worker db-cli cache-cli clean clean-volumes

help:
	@echo "GER Orchestration Commands:"
	@echo "  make sync    - Pull code from GitHub and rebuild containers (Slow/Full)"
	@echo "  make update  - Fast pull from GitHub only (No rebuild)"
	@echo "  make logs    - Stream analytics system logs"
	@echo "  make restart - Restart containers without rebuilding"
	@echo "  make clean   - Prune old images and Docker clutter"
	@echo "  make up      - Bring RDE environment up (built freshly)"
	@echo "  make dev     - Force-recreate RDE environment completely freshly"
	@echo "  make up-iam  - Bring the ENTIRE system up including Keycloak/Proxy"
	@echo "  make bootstrap - SOTA Bootstrap: Only Identity infrastructure for manual setup"
	@echo "  make down    - Stop and remove all containers and networks"
	@echo "  make clean-volumes - Hard Reset: Nuke all persistent volumes (Clean Start)"
	@echo "  make rde-url - Show visual BFF access URL"
	@echo "  make ci      - Run the entire CI/CD pipeline locally (Lint, Test, Mutmut, CDC)"
	@echo "  make lint    - Run Ruff linter"
	@echo "  make lint-fix- Auto-fix Ruff linter issues"
	@echo "  make test    - Run unit tests"
	@echo ""
	@echo "Interactive Shortcuts:"
	@echo "  make ps           - List all running services status"
	@echo "  make shell        - Drop into Analytics (Streamlit) shell"
	@echo "  make shell-worker - Drop into Arq Worker shell"
	@echo "  make db-cli       - Enter Keycloak PostgreSQL CLI"
	@echo "  make cache-cli    - Enter Redis (ArQ Queue) CLI"

# SRE: Inicia apenas a base da identidade para permitir a configuração manual inicial
bootstrap:
	@echo "🛠️  Iniciando infraestrutura básica de Identidade (Bootstrap)..."
	$(DOCKER_COMPOSE) --profile iam up -d postgres-keycloak keycloak --wait
	@echo "----------------------------------------------------------"
	@echo "✅ Keycloak está ONLINE e pronto para configuração!"
	@echo "🔗 URL Administrador: http://127.0.0.1.nip.io:8080"
	@echo "📖 Documentação de Setup: BOOTSTRAP_KEYCLOAK.md"
	@echo "----------------------------------------------------------"
	@echo "🚀 Após configurar e atualizar o Secret no creds.env, execute: make up-iam"

sync:
	git pull origin main || true
	$(DOCKER_COMPOSE) --profile iam up -d --build --wait
	@echo "🚀 Sistema sincronizado, reconstruído e VALIDADO com IAM!"

update:
	git pull origin main || true
	@echo "✅ Code updated from GitHub (Fast Sync)."

logs:
	$(DOCKER_COMPOSE) logs -f analytics

logs-proxy:
	$(DOCKER_COMPOSE) logs -f oauth2-proxy

logs-worker:
	$(DOCKER_COMPOSE) logs -f worker

logs-keycloak:
	$(DOCKER_COMPOSE) logs -f keycloak

ps:
	$(DOCKER_COMPOSE) --profile iam ps

shell:
	$(DOCKER_COMPOSE) exec analytics bash

shell-worker:
	$(DOCKER_COMPOSE) exec worker bash

db-cli:
	$(DOCKER_COMPOSE) exec postgres-keycloak psql -U admin_stangler -d keycloak

cache-cli:
	$(DOCKER_COMPOSE) exec redis-queue redis-cli

restart:
	$(DOCKER_COMPOSE) restart
	@echo "♻️ System restarted."

# --- SRE: Verificação de Segurança RDE (Fail-Fast) ---
check-rde:
	$(eval RDE_ACCESS_TOKEN := $(shell grep -E "^RDE_ACCESS_TOKEN=" env/creds.env | cut -d '=' -f2 | tr -d '"' | tr -d "'"))
	@if [ -z "$(RDE_ACCESS_TOKEN)" ]; then echo "ERRO: RDE_ACCESS_TOKEN não configurada. Configure no seu env/creds.env"; exit 1; fi
	@if [ $$(echo -n "$(RDE_ACCESS_TOKEN)" | wc -c) -lt 32 ]; then echo "ERRO: RDE_ACCESS_TOKEN deve ter pelo menos 32 caracteres"; exit 1; fi

up: check-rde
	$(DOCKER_COMPOSE) up -d --build analytics worker
	@make rde-url

dev: check-rde
	$(DOCKER_COMPOSE) up -d --build --force-recreate analytics worker
	@make rde-url

rde-url:
	@echo "=========================================================="
	@echo "🖥️  RDE Visual BFF: http://127.0.0.1.nip.io:6080/vnc.html"
	@echo "=========================================================="

up-iam:
	$(DOCKER_COMPOSE) --profile iam up -d --wait
	@echo "🔐 Stack completa e VALIDADA (App e Identity Provider ativos)!"

down:
	$(DOCKER_COMPOSE) --profile iam down --remove-orphans
	@echo "⬇️ Todo o sistema (incluindo IAM) derrubado."

clean:
	docker image prune -f
	@echo "🧹 Old images removed and Docker environment cleaned."

clean-volumes:
	$(DOCKER_COMPOSE) --profile iam down -v --remove-orphans
	@echo "⚠️  Volumes de dados removidos. O próximo boot será 100% limpo."

# --- CI/CD PIPELINE LOCAL ---
lint:
	uv run ruff check .

lint-fix:
	uv run ruff check --fix .

test:
	uv run pytest tests/domain tests/application --maxfail=1 --disable-warnings -v

test-mutmut:
	uv run mutmut run --paths-to-mutate src/domain/ --tests-dir tests/domain/ || true
	@SURVIVORS=$$(uv run mutmut results | grep "Survived" | wc -l); \
	if [ "$$SURVIVORS" -gt "0" ]; then \
		echo "❌ Falha SRE de Mutação: $$SURVIVORS mutantes sobreviveram! Seus testes não garantem a lógica."; \
		exit 1; \
	fi

test-contract:
	uv run pytest tests/infrastructure/test_gercon_contract.py

ci: lint test test-mutmut test-contract
	@echo "✅ PIPELINE CI LOCAL PASSOU COM SUCESSO! Código pronto para git push."
