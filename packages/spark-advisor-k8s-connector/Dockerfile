FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages/spark-advisor-models/ packages/spark-advisor-models/
COPY packages/spark-advisor-k8s-connector/ packages/spark-advisor-k8s-connector/

RUN uv sync --frozen --no-dev --package spark-advisor-k8s-connector

CMD ["uv", "run", "--package", "spark-advisor-k8s-connector", "spark-advisor-k8s-connector"]
