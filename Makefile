# --- GER: Makefile for SOTA Modular Monolith Orchestration ---
# This Makefile encapsulates common developer workflows for the GER project.
# It prioritizes cleanliness, automation, and SRE-grade local management.

# Variables
ENV_FLAGS = --env-file env/creds.env --env-file env/config.env
DOCKER_COMPOSE = sudo docker compose $(ENV_FLAGS)

.PHONY: sync update help up down restart logs clean

help:
	@echo "GER Orchestration Commands:"
	@echo "  make sync    - Pull code from GitHub and rebuild containers (Slow/Full)"
	@echo "  make update  - Fast pull from GitHub only (No rebuild)"
	@echo "  make logs    - Stream system logs in real-time"
	@echo "  make restart - Restart containers without rebuilding"
	@echo "  make clean   - Prune old images and Docker clutter"
	@echo "  make up      - Bring the system up in detached mode (App Only, No IAM)"
	@echo "  make up-iam  - Bring the ENTIRE system up including Keycloak/Proxy"
	@echo "  make down    - Stop and remove all containers and networks"

sync:
	git pull origin main
	$(DOCKER_COMPOSE) --profile iam up -d --build
	@echo "🚀 Sistema sincronizado e reconstruído com IAM."

update:
	git pull origin main
	@echo "✅ Code updated from GitHub (Fast Sync)."

logs:
	$(DOCKER_COMPOSE) logs -f analytics

restart:
	$(DOCKER_COMPOSE) restart
	@echo "♻️ System restarted."

up:
	$(DOCKER_COMPOSE) up -d
	@echo "⬆️ App Core Analytics subiu (Sem IAM)."

up-iam:
	$(DOCKER_COMPOSE) --profile iam up -d
	@echo "🔐 Stack completa com Identity Access Management ativa."

down:
	$(DOCKER_COMPOSE) --profile iam down
	@echo "⬇️ Todo o sistema (incluindo IAM) derrubado."

clean:
	sudo docker image prune -f
	@echo "🧹 Old images removed and Docker environment cleaned."
