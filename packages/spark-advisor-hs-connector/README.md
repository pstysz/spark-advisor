# spark-advisor-hs-connector

Spark History Server connector — fetches job data and publishes to NATS. Part of the [spark-advisor](https://github.com/pstysz/spark-advisor) ecosystem.

## Install

```bash
pip install spark-advisor-hs-connector
```

## What it does

Connects to Apache Spark History Server REST API to fetch job metrics, stage details, executor info, and configuration. Operates in two modes:

- **On-demand** — NATS subscriber for `fetch.job` requests (returns `JobAnalysis`)
- **Background polling** — periodically scans History Server for new applications and publishes them to NATS

## History Server endpoints used

- `GET /api/v1/applications/{app-id}` — app metadata
- `GET /api/v1/applications/{app-id}/environment` — spark.* config
- `GET /api/v1/applications/{app-id}/stages` — stage metrics
- `GET /api/v1/applications/{app-id}/stages/{id}/{attempt}/taskSummary` — task distribution
- `GET /api/v1/applications/{app-id}/executors` — executor metrics

## Deployment

```bash
export SA_CONNECTOR_HISTORY_SERVER_URL=http://yarn:18080
export SA_CONNECTOR_NATS__URL=nats://localhost:4222
spark-advisor-hs-connector
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SA_CONNECTOR_HISTORY_SERVER_URL` | `http://localhost:18080` | Spark History Server URL |
| `SA_CONNECTOR_NATS__URL` | `nats://localhost:4222` | NATS broker URL |
| `SA_CONNECTOR_POLL_INTERVAL_SECONDS` | `60` | Polling interval |
| `SA_CONNECTOR_BATCH_SIZE` | `50` | Apps per poll cycle |

## Links

- [Main project](https://github.com/pstysz/spark-advisor)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
