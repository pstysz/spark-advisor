# spark-advisor-analyzer

AI-powered Spark job analyzer — NATS worker service. Part of the [spark-advisor](https://github.com/pstysz/spark-advisor) ecosystem.

## Install

```bash
pip install spark-advisor-analyzer
```

## What it does

Combines deterministic rules engine with optional Claude AI analysis to provide actionable Spark configuration recommendations.

- **Rules engine** — 11 expert rules detecting data skew, GC pressure, disk spill, and more
- **AI analysis** — Claude API integration for prioritized recommendations with causal chains
- **Agent mode** — multi-turn Claude tool_use loop for autonomous job exploration
- **NATS worker** — FastStream subscriber for `analysis.run` and `analysis.run.agent`

## Deployment

As a standalone NATS worker:

```bash
export SA_ANALYZER_NATS__URL=nats://localhost:4222
export ANTHROPIC_API_KEY=sk-ant-...
spark-advisor-analyzer
```

Or use via `spark-advisor` CLI or `spark-advisor-mcp` server.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SA_ANALYZER_NATS__URL` | `nats://localhost:4222` | NATS broker URL |
| `SA_ANALYZER_AI__ENABLED` | `true` | Enable AI analysis |
| `ANTHROPIC_API_KEY` | — | Claude API key (required if AI enabled) |
| `SA_ANALYZER_OTEL__ENABLED` | `false` | Enable OpenTelemetry distributed tracing |

## Links

- [Main project](https://github.com/pstysz/spark-advisor)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
