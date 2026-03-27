# --- GER: Makefile for SOTA Modular Monolith Orchestration ---
# This Makefile encapsulates common developer workflows for the GER project.
# It prioritizes cleanliness, automation, and SRE-grade local management.

# Variables
ENV_FLAGS = --env-file env/creds.env --env-file env/config.env
DOCKER_COMPOSE = docker compose $(ENV_FLAGS)

.PHONY: bootstrap sync update help up down restart logs clean clean-volumes

help:
	@echo "GER Orchestration Commands:"
	@echo "  make sync    - Pull code from GitHub and rebuild containers (Slow/Full)"
	@echo "  make update  - Fast pull from GitHub only (No rebuild)"
	@echo "  make logs    - Stream system logs in real-time"
	@echo "  make restart - Restart containers without rebuilding"
	@echo "  make clean   - Prune old images and Docker clutter"
	@echo "  make up      - Bring the system up in detached mode (App Only, No IAM)"
	@echo "  make up-iam  - Bring the ENTIRE system up including Keycloak/Proxy"
	@echo "  make bootstrap - SOTA Bootstrap: Only Identity infrastructure for manual setup"
	@echo "  make down    - Stop and remove all containers and networks"
	@echo "  make clean-volumes - Hard Reset: Nuke all persistent volumes (Clean Start)"

# SRE: Inicia apenas a base da identidade para permitir a configuração manual inicial
bootstrap:
	@echo "🛠️  Iniciando infraestrutura básica de Identidade (Bootstrap)..."
	$(DOCKER_COMPOSE) --profile iam up -d postgres-keycloak keycloak --wait
	@echo "----------------------------------------------------------"
	@echo "✅ Keycloak está ONLINE e pronto para configuração!"
	@echo "🔗 URL Administrador: http://localhost:8080"
	@echo "📖 Documentação de Setup: BOOTSTRAP_KEYCLOAK.md"
	@echo "----------------------------------------------------------"
	@echo "🚀 Após configurar e atualizar o Secret no creds.env, execute: make up-iam"

sync:
	git pull origin main
	$(DOCKER_COMPOSE) --profile iam up -d --build --wait
	@echo "🚀 Sistema sincronizado, reconstruído e VALIDADO com IAM!"

update:
	git pull origin main
	@echo "✅ Code updated from GitHub (Fast Sync)."

logs:
	$(DOCKER_COMPOSE) logs -f analytics

restart:
	$(DOCKER_COMPOSE) restart
	@echo "♻️ System restarted."

up:
	$(DOCKER_COMPOSE) up -d --wait
	@echo "⬆️ App Core Analytics subiu e está saudável (Sem IAM)."

up-iam:
	$(DOCKER_COMPOSE) --profile iam up -d --wait
	@echo "🔐 Stack completa e VALIDADA (App e Identity Provider ativos)!"

down:
	$(DOCKER_COMPOSE) --profile iam down
	@echo "⬇️ Todo o sistema (incluindo IAM) derrubado."

clean:
	docker image prune -f
	@echo "🧹 Old images removed and Docker environment cleaned."

clean-volumes:
	$(DOCKER_COMPOSE) --profile iam down -v
	@echo "⚠️  Volumes de dados removidos. O próximo boot será 100% limpo."
