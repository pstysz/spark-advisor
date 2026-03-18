# Helm Charts — spark-advisor

> Back to [main README](../README.md)

## Make targets

Most Helm operations are available as Make targets from the project root:

| Target | Description |
|--------|-------------|
| `make minikube-deploy` | Full deploy: docker build inside minikube + helm install |
| `make helm-install-minikube` | Docker build + helm upgrade (creates secret from `.envrc` if needed) |
| `make helm-install` | Helm install on any cluster (no docker build) |
| `make helm-uninstall` | Uninstall spark-advisor release |
| `make helm-install-monitoring` | Install Prometheus + Tempo + Grafana charts |
| `make helm-uninstall-monitoring` | Uninstall monitoring charts |

## Local development with Minikube

```bash
minikube start
export ANTHROPIC_API_KEY=sk-ant-...
make minikube-deploy                  # builds images + helm install in one step
kubectl port-forward -n spark-advisor svc/spark-advisor-gateway 8080:8080
curl http://localhost:8080/health/live
```

For subsequent deploys after code changes:

```bash
make helm-install-minikube            # rebuilds images + upgrades release
```

## Install on a cluster

```bash
# Create secret for Claude API key
kubectl create secret generic anthropic-api-key --from-literal=api-key=sk-ant-...

# Via Make
make helm-install

# Or directly via Helm
helm dependency update charts/spark-advisor
helm install spark-advisor charts/spark-advisor \
  --set analyzer.anthropicApiKey.existingSecret=anthropic-api-key \
  --set hs-connector.config.historyServerUrl=http://spark-history:18080
```

## Enable Ingress

```bash
helm install spark-advisor charts/spark-advisor \
  --set gateway.ingress.enabled=true \
  --set gateway.ingress.hosts[0].host=spark-advisor.example.com \
  --set gateway.ingress.hosts[0].paths[0].path=/ \
  --set gateway.ingress.hosts[0].paths[0].pathType=Prefix
```

## Monitoring stack

```bash
make helm-install-monitoring          # installs Prometheus + Tempo + Grafana
make helm-uninstall-monitoring        # removes monitoring charts
```

## Chart structure

```
charts/
├── spark-advisor/       # Umbrella chart (installs everything)
├── analyzer/            # Rules engine + AI worker (NATS subscriber)
├── gateway/             # REST API (Service + optional Ingress)
├── hs-connector/        # History Server poller (NATS subscriber)
├── frontend/            # Web dashboard (nginx + reverse proxy)
├── prometheus/          # Prometheus metrics collection
├── grafana/             # Grafana dashboards (3 dashboards)
└── tempo/               # Grafana Tempo (distributed tracing)
```

Each service has a ConfigMap mapping `values.yaml` to `SA_*` environment variables expected by pydantic-settings. NATS URL is auto-resolved from the release name when deployed via the umbrella chart.

## Observability

| Feature | Env var | Default (docker-compose) | Default (Helm) | Description |
|---------|---------|-------------------------|----------------|-------------|
| JSON logging | `SA_*_JSON_LOG` | `false` | `true` | Structured JSON logs (structlog) |
| Prometheus metrics | `SA_GATEWAY_METRICS_ENABLED` | `false` | `true` | `/metrics` endpoint on gateway |
| Distributed tracing | `SA_*_OTEL__ENABLED` | `false` | `true` | OpenTelemetry spans with W3C Traceparent via NATS |
| Tracing endpoint | `SA_*_OTEL__ENDPOINT` | `http://localhost:4317` | `http://tempo:4317` | OTLP gRPC collector (Grafana Tempo) |
| Health checks | Always on | — | — | `/health/ready` (gateway), NATS exec probes (analyzer, hs-connector) |

Observability is disabled by default for docker-compose (local dev) and enabled by default in Helm charts (production).

## Docker images

All images are published to GitHub Container Registry:

- `ghcr.io/pstysz/spark-advisor-analyzer`
- `ghcr.io/pstysz/spark-advisor-gateway`
- `ghcr.io/pstysz/spark-advisor-hs-connector`
- `ghcr.io/pstysz/spark-advisor-frontend`

## See also

- [Main project](../../README.md)
- [Monitoring](../monitoring/README.md)
- [Architecture](../docs/architecture.md)
- [Development](../docs/development.md)
