COMPOSE_FILE ?= docker/docker-compose.yml

run:
	@echo "Using compose file: $(COMPOSE_FILE)"
	docker compose -f $(COMPOSE_FILE) down -v && docker compose -f $(COMPOSE_FILE) up --build