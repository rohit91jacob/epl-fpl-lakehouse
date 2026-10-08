.DEFAULT_GOAL := help
SAMPLE_API ?= ./data/sample-api

.PHONY: help install lint format typecheck test test-fast sample run-sample run docker-build up down clean

help: ## Show targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-13s %s\n", $$1, $$2}'

install: ## Create the virtualenv with dev tools
	uv sync

lint: ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check .

format: ## Auto-format
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## mypy
	uv run mypy

test: ## All tests except Airflow DAG tests (needs Java 17+)
	uv run pytest -m "not airflow"

test-fast: ## Tests that need neither Spark nor Airflow
	uv run pytest -m "not spark and not airflow"

sample: ## Write the synthetic FPL API snapshot
	uv run epl sample-data --out $(SAMPLE_API)

run-sample: sample ## Full pipeline on synthetic data (offline)
	EPL_FPL_BASE_URL=file://$(abspath $(SAMPLE_API)) uv run epl run

run: ## Full pipeline against the live FPL API
	uv run epl run

docker-build: ## Build the standalone pipeline image
	docker build -t epl-fpl-lakehouse:local .

up: ## Start Airflow (http://localhost:8080)
	docker compose up -d --build

down: ## Stop Airflow and remove volumes
	docker compose down -v

clean: ## Remove local lake data and caches
	rm -rf data .pytest_cache .ruff_cache .mypy_cache coverage.xml .coverage
