.PHONY: up down logs restart-python build health test etl setup-data verify help

help:
	@echo "genbi-ipl — Available targets:"
	@echo ""
	@echo "  make up           Start both services (gateway + intelligence)"
	@echo "  make down         Stop both services"
	@echo "  make logs         Tail logs from both services"
	@echo "  make health       Check health of both services"
	@echo "  make setup-data   Download Cricsheet data and run ETL (first-time setup)"
	@echo "  make test         Run the ETL test suite"
	@echo "  make verify       Run post-ETL verification queries"
	@echo "  make etl          Re-run the ETL pipeline"
	@echo "  make build        Rebuild containers"
	@echo ""

up:
	docker compose up --build -d
	@echo ""
	@echo "Gateway:      http://localhost:8080"
	@echo "Intelligence: http://localhost:8000"
	@echo ""
	@echo "First time? Run 'make setup-data' to load the cricket database."

down:
	docker compose down

logs:
	docker compose logs -f

restart-python:
	docker compose up --build -d intelligence

build:
	docker compose build

health:
	@echo "Gateway health:"
	@curl -s http://localhost:8080/health && echo ""
	@echo "Intelligence health:"
	@curl -s http://localhost:8000/health && echo ""

test:
	docker compose exec intelligence pytest etl/tests/ -v

setup-data:
	@echo "Downloading Cricsheet data..."
	docker compose exec intelligence python scripts/download_data.py
	@echo ""
	@echo "Running ETL pipeline..."
	docker compose exec intelligence python -m etl.run_etl
	@echo ""
	@echo "Verifying..."
	docker compose exec intelligence python scripts/verify_etl.py

etl:
	docker compose exec intelligence python -m etl.run_etl

verify:
	docker compose exec intelligence python scripts/verify_etl.py