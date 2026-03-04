.PHONY: install dev test lint format check demo-local demo-remote up down clean

PACKAGES = packages/spark-advisor-models packages/spark-advisor-rules packages/spark-advisor-cli packages/spark-advisor-analyzer packages/spark-advisor-hs-connector packages/spark-advisor-gateway packages/spark-advisor-mcp

install:
	uv sync

dev:
	uv sync --group dev

test:
	@for pkg in $(PACKAGES); do \
		echo "\n=== Testing $$pkg ==="; \
		cd $(CURDIR)/$$pkg && uv run pytest || exit 1; \
	done

lint:
	@for pkg in $(PACKAGES); do \
		echo "\n=== Linting $$pkg ==="; \
		cd $(CURDIR)/$$pkg && uv run ruff check src/ tests/ && uv run mypy src/ || exit 1; \
	done

format:
	@for pkg in $(PACKAGES); do \
		echo "\n=== Formatting $$pkg ==="; \
		cd $(CURDIR)/$$pkg && uv run ruff format src/ tests/ && uv run ruff check --fix src/ tests/ || exit 1; \
	done

check: lint test

demo-local:
	cd $(CURDIR)/packages/spark-advisor-cli && uv run spark-advisor analyze ../../sample_event_logs/sample_etl_job.json

demo-remote:
	curl -s -X POST http://localhost:8080/api/v1/analyze \
	  -H 'Content-Type: application/json' \
	  -d '{"app_id":"$(APP_ID)"}'

up:
	docker compose up -d

down:
	docker compose down

clean:
	rm -rf dist/ build/ *.egg-info .mypy_cache .pytest_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
