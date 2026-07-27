.PHONY: up down migrate api reaper worker-example test test-integration lint

up:
	docker compose up -d
	@echo "Waiting for postgres..."
	@until docker compose exec -T postgres pg_isready -U queueline >/dev/null 2>&1; do sleep 1; done
	$(MAKE) migrate

down:
	docker compose down -v

migrate:
	docker compose exec -T postgres psql -U queueline -d queueline < internal/db/migrations/0001_init.sql

api:
	go run ./cmd/api

reaper:
	go run ./cmd/reaper

worker-example:
	go run ./cmd/worker-example

test:
	go test ./internal/... -v -race -cover

test-integration:
	go test ./test/integration/... -v -race

lint:
	go vet ./...
