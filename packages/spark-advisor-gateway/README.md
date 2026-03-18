# spark-advisor-gateway

REST API gateway for spark-advisor — orchestrates analysis via NATS. Part of the [spark-advisor](https://github.com/pstysz/spark-advisor) ecosystem.

## Install

```bash
pip install spark-advisor-gateway
```

## What it does

FastAPI-based REST gateway that accepts analysis requests and orchestrates the full pipeline via NATS messaging:

1. Receives `POST /api/v1/analyze` with app ID
2. Fetches job data from History Server (via hs-connector over NATS)
3. Sends job data for analysis (via analyzer over NATS)
4. Returns results via polling `GET /api/v1/tasks/{id}`

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/analyze` | Submit analysis request (202 new, 409 duplicate) |
| `GET` | `/api/v1/tasks` | List tasks with filtering and pagination |
| `GET` | `/api/v1/tasks/stats` | Task count by status |
| `GET` | `/api/v1/tasks/{id}` | Get task status and result |
| `GET` | `/api/v1/tasks/{id}/rules` | Rule violations for completed task |
| `GET` | `/api/v1/tasks/{id}/config` | Config comparison (rule + AI merged) |
| `GET` | `/api/v1/applications` | List apps from History Server (paginated) |
| `GET` | `/api/v1/apps/{app_id}/history` | Analysis history per app |
| `GET` | `/api/v1/stats/summary` | Totals, avg duration, AI usage % |
| `GET` | `/api/v1/stats/rules` | Rule violation frequency |
| `GET` | `/api/v1/stats/daily-volume` | Daily analysis count |
| `GET` | `/api/v1/stats/top-issues` | Most common issues |
| `WS` | `/api/v1/ws/tasks` | Real-time task status updates |
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe (NATS + SQLite) |

## Deployment

```bash
export SA_GATEWAY_NATS__URL=nats://localhost:4222
export SA_GATEWAY_SERVER__PORT=8080
spark-advisor-gateway
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SA_GATEWAY_NATS__URL` | `nats://localhost:4222` | NATS broker URL |
| `SA_GATEWAY_SERVER__PORT` | `8080` | REST API port |
| `SA_GATEWAY_DATABASE_URL` | `sqlite+aiosqlite:///data/spark_advisor.db` | SQLite database URL |
| `SA_GATEWAY_WS_HEARTBEAT_INTERVAL` | `30` | WebSocket heartbeat interval (seconds) |
| `SA_GATEWAY_METRICS_ENABLED` | `false` | Enable Prometheus metrics on `/metrics` |
| `SA_GATEWAY_OTEL__ENABLED` | `false` | Enable OpenTelemetry distributed tracing |

## Links

- [Main project](https://github.com/pstysz/spark-advisor)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
