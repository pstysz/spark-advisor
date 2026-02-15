.PHONY: install dev test lint format check all clean

# Install production dependencies
install:
	uv sync

# Install with dev dependencies
dev:
	uv sync --group dev

# Run tests
test:
	uv run pytest

# Run linter
lint:
	uv run ruff check src/ tests/
	uv run mypy src/

# Auto-format code
format:
	uv run ruff format src/ tests/
	uv run ruff check --fix src/ tests/

# Run all checks (CI-ready)
check: lint test

# Run the CLI
run:
	uv run spark-advisor

# Analyze sample event log
demo:
	uv run spark-advisor analyze sample_event_logs/sample_etl_job.json --no-ai

# Clean build artifacts
clean:
	rm -rf dist/ build/ *.egg-info .mypy_cache .pytest_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
