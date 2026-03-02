FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages/spark-advisor-models/ packages/spark-advisor-models/
COPY packages/spark-advisor-rules/ packages/spark-advisor-rules/
COPY packages/spark-advisor-analyzer/ packages/spark-advisor-analyzer/

RUN uv sync --frozen --no-dev --package spark-advisor-analyzer

CMD ["uv", "run", "--package", "spark-advisor-analyzer", "spark-advisor-analyzer"]
