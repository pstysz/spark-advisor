FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY packages/spark-advisor-shared/ /app/packages/spark-advisor-shared/
COPY packages/spark-advisor-hs-poller/ /app/packages/spark-advisor-hs-poller/

WORKDIR /app/packages/spark-advisor-hs-poller

RUN uv sync --no-dev --frozen

EXPOSE 8080

CMD ["uv", "run", "spark-advisor-hs-poller"]
