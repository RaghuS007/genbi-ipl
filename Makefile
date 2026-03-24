.PHONY: up down logs restart-python build health test etl etl-season eval lint

# Start all services
up:
	docker compose up --build -d
	@echo "Gateway: http://localhost:8080"
	@echo "Intelligence: http://localhost:8000"

# Stop all services
down:
	docker compose down

# Tail logs from all services
logs:
	docker compose logs -f

# Restart only the Python service (faster iteration)
restart-python:
	docker compose up --build -d intelligence

build:
	docker compose build

health:
	curl http://localhost:8000/health
	curl http://localhost:8080/health

# Run tests
test:
	docker compose exec intelligence pytest tests/ -v

# Run ETL pipeline (downloads data, loads DuckDB, generates corpus, embeds)
etl:
	python etl/run_etl.py

# Reload a specific season
etl-season:
	python etl/run_etl.py --season $(SEASON)

# Run evaluation suite
eval:
	python evaluation/run_eval.py

# Lint
lint:
	cd intelligence && python -m ruff check .
	cd gateway && go vet ./...
