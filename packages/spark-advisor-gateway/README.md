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
| `POST` | `/api/v1/analyze` | Submit analysis request |
| `GET` | `/api/v1/tasks/{id}` | Get task status and result |
| `GET` | `/api/v1/tasks` | List recent tasks |
| `GET` | `/api/v1/applications` | List apps from History Server |
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe (NATS check) |

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
| `SA_GATEWAY_MAX_STORED_TASKS` | `1000` | Max tasks in memory |

## Links

- [Main project](https://github.com/pstysz/spark-advisor)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
