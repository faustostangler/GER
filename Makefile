# --- GER: Makefile for SOTA Modular Monolith Orchestration ---
# This Makefile encapsulates common developer workflows for the GER project.
# It prioritizes cleanliness, automation, and SRE-grade local management.

# Variables
DOCKER_COMPOSE = sudo docker compose

.PHONY: sync update help up down restart logs clean

help:
	@echo "GER Orchestration Commands:"
	@echo "  make sync    - Pull code from GitHub and rebuild containers (Slow/Full)"
	@echo "  make update  - Fast pull from GitHub only (No rebuild)"
	@echo "  make logs    - Stream system logs in real-time"
	@echo "  make restart - Restart containers without rebuilding"
	@echo "  make clean   - Prune old images and Docker clutter"
	@echo "  make up      - Bring the system up in detached mode"
	@echo "  make down    - Stop and remove containers and networks"

sync:
	git pull origin main
	$(DOCKER_COMPOSE) up -d --build
	@echo "🚀 Full synchronization and rebuild completed successfully!"

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
	@echo "⬆️ System is up."

down:
	$(DOCKER_COMPOSE) down
	@echo "⬇️ System is down."

clean:
	sudo docker image prune -f
	@echo "🧹 Old images removed and Docker environment cleaned."
