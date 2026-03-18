# Development Guide

[Back to main README](../README.md)

## Tech Stack

| Tool | Role |
|------|------|
| Python 3.12+ | Language |
| uv | Package manager + workspace |
| Pydantic v2 | Data models, validation, JSON Schema for Claude tools |
| pydantic-settings | Configuration (env vars, YAML, .env) |
| NATS | Message broker (15MB binary, replaces Kafka) |
| FastStream | Type-safe NATS workers |
| FastAPI | REST API (gateway) |
| nats-py | NATS client with request-reply (gateway) |
| httpx | HTTP client for History Server |
| anthropic | Claude SDK for AI analysis |
| mcp | MCP server SDK (FastMCP, stdio) |
| Typer | CLI framework |
| Rich | Terminal output (tables, colors, panels) |
| orjson | Fast JSON parsing for event logs |
| Ruff | Linter + formatter |
| mypy | Type checker (strict mode) |
| pytest | Testing with coverage and fixtures |
| SQLAlchemy | Async ORM for task persistence (SQLite + WAL mode) |
| aiosqlite | Async SQLite driver for SQLAlchemy |
| structlog | Structured logging (JSON/console, correlation IDs) |
| Prometheus | Metrics collection (gateway, optional) |
| Grafana | Monitoring dashboard (optional) |
| OpenTelemetry | Distributed tracing (W3C Traceparent, Grafana Tempo) |
| Helm | Kubernetes deployment (umbrella chart + 8 subcharts) |
| Docker | Multi-stage builds with uv, published to ghcr.io |
| React 19 | Frontend SPA (TypeScript) |
| Vite 6 | Frontend build tool with HMR |
| TanStack Query v5 | Server state management with caching |

## Make Commands

```bash
make check                # Lint + mypy + all tests (CI-ready)
make test                 # Run 570 tests across 7 packages
make lint                 # Ruff + mypy (strict)
make format               # Auto-format code
make demo-local           # Run rules-only analysis on sample event log
make up                   # Start microservices (docker compose)
make down                 # Stop microservices
make minikube-deploy      # Build + deploy to minikube
make helm-install-minikube # Helm install on minikube
make monitoring-up        # Start Prometheus + Grafana + Tempo
make monitoring-down      # Stop monitoring
make frontend-dev         # Start Vite dev server (HMR)
make frontend-build       # Build frontend for production
```

## Testing

570 tests across 7 packages, all passing.

```bash
make test    # Run all tests
make check   # Lint + mypy + tests (full CI pipeline)
```

### Test breakdown by package

| Package | Tests |
|---------|-------|
| models | 47 |
| rules | 45 |
| cli | 78 |
| analyzer | 86 |
| hs-connector | 68 |
| gateway | 171 |
| mcp | 75 |
| **Total** | **570** |

## Environment Variables

| Service | Variable | Default | Description |
|---------|----------|---------|-------------|
| All | `SA_*_NATS__URL` | `nats://localhost:4222` | NATS broker URL |
| Analyzer | `SA_ANALYZER_AI__ENABLED` | `true` | Enable AI analysis |
| Analyzer | `ANTHROPIC_API_KEY` | (required if AI enabled) | Claude API key |
| Gateway | `SA_GATEWAY_SERVER__PORT` | `8080` | REST API port |
| Gateway | `SA_GATEWAY_DATABASE_URL` | `sqlite+aiosqlite:///data/spark_advisor.db` | SQLite database URL |
| Gateway | `SA_GATEWAY_WS_HEARTBEAT_INTERVAL` | `30` | WebSocket heartbeat interval (seconds) |
| hs-connector | `SA_CONNECTOR_HISTORY_SERVER_URL` | `http://localhost:18080` | Spark History Server URL |
| hs-connector | `SA_CONNECTOR_POLL_INTERVAL_SECONDS` | `60` | Polling interval |
| hs-connector | `SA_CONNECTOR_BATCH_SIZE` | `50` | Apps per poll cycle |
| hs-connector | `SA_CONNECTOR_POLLING_ENABLED` | `false` | Enable automatic polling |
| Gateway | `SA_GATEWAY_NATS__POLLING_ANALYSIS_MODE` | `static` | Analysis mode for polled jobs |
| All | `SA_*_JSON_LOG` | `false` (Helm: `true`) | Enable JSON structured logging |
| Gateway | `SA_GATEWAY_METRICS_ENABLED` | `false` (Helm: `true`) | Enable Prometheus metrics |
| All | `SA_*_OTEL__ENABLED` | `false` (Helm: `true`) | Enable OpenTelemetry tracing |
| All | `SA_*_OTEL__ENDPOINT` | `http://localhost:4317` (Helm: `http://tempo:4317`) | OTLP gRPC endpoint |

## See also

- [Architecture](architecture.md) -- system design and data flow
- [Main README](../README.md) -- project overview and quick start
