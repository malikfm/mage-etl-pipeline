up:
	docker compose up -d --build

down:
	docker compose down -v --remove-orphans

ps:
	docker compose ps
