.PHONY: install dev test lint format check demo-local demo-remote up down clean docker-local minikube-deploy frontend-install frontend-build frontend-dev frontend-lint monitoring-up monitoring-down

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

SERVICES = spark-advisor-analyzer spark-advisor-gateway spark-advisor-hs-connector
LOCAL_TAG := $(shell git rev-parse --short HEAD)-$(shell date +%s)

docker-local:
	@eval $$(minikube docker-env) && \
	for svc in $(SERVICES); do \
		echo "\n=== Building $$svc:$(LOCAL_TAG) ==="; \
		docker build -f packages/$$svc/Dockerfile -t $$svc:$(LOCAL_TAG) . || exit 1; \
	done && \
	echo "\n=== Building spark-advisor-frontend:$(LOCAL_TAG) ===" && \
	docker build -f packages/spark-advisor-frontend/Dockerfile -t spark-advisor-frontend:$(LOCAL_TAG) packages/spark-advisor-frontend/

minikube-deploy: docker-local
	kubectl create namespace spark-advisor --dry-run=client -o yaml | kubectl apply -f -
	kubectl create secret generic anthropic-api-key \
		--namespace spark-advisor \
		--from-literal=api-key=$$ANTHROPIC_API_KEY \
		--dry-run=client -o yaml | kubectl apply -f -
	helm dependency update charts/spark-advisor
	helm upgrade --install spark-advisor charts/spark-advisor \
		--namespace spark-advisor --create-namespace \
		-f charts/spark-advisor/values-local.yaml \
		--set analyzer.image.tag=$(LOCAL_TAG) \
		--set gateway.image.tag=$(LOCAL_TAG) \
		--set frontend.image.tag=$(LOCAL_TAG) \
		--set hs-connector.image.tag=$(LOCAL_TAG)

FRONTEND_DIR = packages/spark-advisor-frontend

frontend-install:
	cd $(FRONTEND_DIR) && npm ci

frontend-build: frontend-install
	cd $(FRONTEND_DIR) && npm run build

frontend-dev:
	cd $(FRONTEND_DIR) && npm run dev

frontend-lint:
	cd $(FRONTEND_DIR) && npm run type-check && npm run lint

monitoring-up:
	docker compose -f monitoring/docker-compose.monitoring.yaml up -d

monitoring-down:
	docker compose -f monitoring/docker-compose.monitoring.yaml down

clean:
	rm -rf dist/ build/ *.egg-info .mypy_cache .pytest_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
