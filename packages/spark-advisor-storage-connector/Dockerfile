FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages/spark-advisor-models/ packages/spark-advisor-models/
COPY packages/spark-advisor-parser/ packages/spark-advisor-parser/
COPY packages/spark-advisor-storage-connector/ packages/spark-advisor-storage-connector/

ARG CONNECTOR_TYPE=hdfs
RUN if [ "$CONNECTOR_TYPE" = "s3" ]; then \
      uv sync --frozen --no-dev --package spark-advisor-storage-connector --extra s3; \
    elif [ "$CONNECTOR_TYPE" = "gcs" ]; then \
      uv sync --frozen --no-dev --package spark-advisor-storage-connector --extra gcs; \
    else \
      uv sync --frozen --no-dev --package spark-advisor-storage-connector; \
    fi

CMD ["uv", "run", "--package", "spark-advisor-storage-connector", "spark-advisor-storage-connector"]
