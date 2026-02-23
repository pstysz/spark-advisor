FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock /app/
COPY packages/spark-advisor-shared/ /app/packages/spark-advisor-shared/
COPY packages/spark-advisor/ /app/packages/spark-advisor/

WORKDIR /app/packages/spark-advisor

RUN uv sync --no-dev --frozen

CMD ["uv", "run", "spark-advisor-serve"]
