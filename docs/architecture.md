# spark-advisor Architecture

## Overview

spark-advisor is a distributed system for automated Apache Spark job performance analysis. It collects job metrics from Spark History Server, storage systems (HDFS/S3/GCS), or local event log files, runs a deterministic rules engine, optionally invokes Claude AI for recommendations, and delivers structured results via REST API or CLI.

```
                              NATS Message Broker
                 ┌────────────┬──────────────────┬──────────────────┐
                 │            │                  │                  │
            job.fetch    analysis.run     analysis.submit   storage.fetch.*
           (req-reply)   (req-reply)      (pub/sub)         (req-reply)
                 │            │                  │                  │
                 ▼            ▼                  │                  ▼
 ┌──────────────────┐  ┌──────────────┐  ┌───────┴─────────┐  ┌───────────────────┐
 │  HS Connector    │  │   Analyzer   │  │     Gateway     │  │ Storage Connector │
 │  (FastStream)    │  │ (FastStream) │  │    (FastAPI)    │  │   (FastStream)    │
 │                  │  │              │  │                 │  │                   │
 │ • fetch jobs     │  │ • rules (11) │  │ • REST API (18) │  │ • HDFS (WebHDFS)  │
 │ • poll HS        │  │ • AI advisor │  │ • WebSocket     │  │ • S3 (aiobotocore)│
 │ • list apps      │  │ • agent mode │  │ • task store    │  │ • GCS (gcloud-aio)│
 └────────┬─────────┘  └──────┬───────┘  └─────────────────┘  └────────┬──────────┘
          │                   │                  ▲                      │
          ▼                   ▼                  │                      ▼
     Spark History       Claude API         ┌────┴──────┐         HDFS / S3 / GCS
       Server                               │ Frontend  │
                                            │ (nginx)   │
 Event Log File ─── parse ──► CLI           │ • React 19│
   (standalone, no infrastructure)          └───────────┘
```

Five services communicate via NATS (gateway, analyzer, hs-connector, storage-connector) and HTTP (frontend → gateway). Frontend serves a React SPA via nginx, proxying `/api/` requests to gateway. A standalone CLI operates without any infrastructure for local analysis.

> Diagram: [01-system-overview.mmd](diagrams/01-system-overview.mmd)

---

## Architecture Principles

| Principle                  | How it applies                                                                                                                                                                                            |
|----------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **SOLID**                  | Single responsibility per package. Rules don't know about AI. Gateway doesn't know about rules. Dependency inversion via `Rule` ABC in rules.                                                             |
| **DRY**                    | `BaseServiceSettings` in models — inherited by all services. `fetch_job_from_hs()` shared between handler and poller. `NatsSettings` base DTO extended per service. Test factories centralized in models. |
| **KISS**                   | Plain JSON on NATS (no Avro/Protobuf). FastStream handles Pydantic serde automatically. SQLite task store. No DLQ, no circuit breaker, no rate limiter — graceful degradation instead.                     |
| **Separation of Concerns** | Each service owns its domain logic and private models. Shared `models` package contains ONLY contracts and configuration DTOs — zero infrastructure.                                                      |
| **12-Factor App**          | Config from environment (`SA_ANALYZER_*`, `SA_HS_CONNECTOR_*`, `SA_GATEWAY_*`, `SA_STORAGE_*`). Stateless services. Explicit dependencies in `pyproject.toml`. Fast startup/shutdown via FastStream lifespan.                |

---

## Service Ownership

Each package owns its domain and can be independently deployed, scaled, and tested:

| Package          | Type    | Owns                                                                                                                                                                                  | Does NOT own                            |
|------------------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| **models**       | Library | Pydantic contracts (`JobAnalysis`, `AnalysisResult`, `RuleResult`, `SparkConfig`), config DTOs (`Thresholds`, `AiSettings`), `BaseServiceSettings`, `NatsSettings`, utility functions | Any infrastructure, business logic, I/O |
| **rules**        | Library | `Rule` ABC, 11 concrete rules, `StaticAnalysisService`, `default_rules` and `rules_for_threshold` factories                                                                           | AI logic, HTTP clients, messaging       |
| **analyzer**     | Service | `AdviceOrchestrator`, `LlmAnalysisService`, `AnthropicClient`, prompts, tool schema                                                                                                   | REST API, HS client, event log parsing  |
| **gateway**      | Service | REST API, `TaskManager`, `TaskStore` (SQLAlchemy + SQLite), `TaskExecutor`                                                                                                            | Analysis logic, HS client, Claude API   |
| **hs-connector** | Service | `HistoryServerClient`, mapper, `HistoryServerPoller`, `PollingState`, `ApplicationSummary` model                                                                                      | Analysis logic, Claude API, REST API    |
| **parser**       | Library | Event log parsing with compression support (.json, .gz, .lz4, .snappy, .zstd), shared by CLI and storage-connector                                                                     | NATS messaging, REST API, storage I/O   |
| **storage-connector** | Service | `StorageConnector` protocol (HDFS/S3/GCS strategy), `StoragePoller`, `PollingStore` (SQLite), event log builder                                                                    | Analysis logic, Claude API, REST API    |
| **cli**          | App     | Rich console output, `analyze` (file + HS + optional AI), `scan` (list HS apps), `version`                                                                                             | NATS messaging, REST API serving        |
| **mcp**          | App     | MCP server (stdio), 7 tools (`analyze_spark_job`, `scan_recent_jobs`, `get_job_config`, `suggest_config`, `explain_metric`, `get_stage_details`, `compare_jobs`), markdown formatting for MCP responses | NATS messaging, REST API serving        |
| **frontend**     | SPA     | React 19 dashboard, nginx reverse proxy                                                                                                                                                                 | API logic, business rules               |

---

## Package Structure

```
spark-advisor/
├── pyproject.toml                           # workspace root
├── Makefile
├── docker-compose.yaml                      # NATS + 5 services
├── packages/
│   ├── spark-advisor-models/                # LIBRARY
│   │   └── src/spark_advisor_models/
│   │       ├── model/                       # metrics, spark_config, output, input
│   │       ├── config.py                    # Thresholds, AiSettings
│   │       ├── settings.py                  # BaseServiceSettings, NatsSettings
│   │       ├── logging.py                   # configure_logging(), bind_nats_context(), nats_handler_context()
│   │       ├── tracing.py                   # TracingConfig, configure_tracing(), get_tracer(), inject/extract
│   │       └── util/                        # bytes, stats
│   │
│   ├── spark-advisor-rules/                 # LIBRARY
│   │   └── src/spark_advisor_rules/
│   │       ├── rules.py                     # Rule ABC + 11 rules + factories
│   │       └── static_analysis.py           # StaticAnalysisService
│   │
│   ├── spark-advisor-analyzer/              # SERVICE (FastStream + NATS)
│   │   ├── Dockerfile
│   │   └── src/spark_advisor_analyzer/
│   │       ├── app.py                       # FastStream app + @on_startup (orchestrator init)
│   │       ├── config.py                    # AnalyzerSettings
│   │       ├── handlers.py                  # @broker.subscriber("analysis.run", "analysis.run.agent")
│   │       ├── orchestrator.py              # AdviceOrchestrator (rules + AI + agent)
│   │       ├── ai/                          # client, service, prompts, tool_config
│   │       └── agent/                       # orchestrator, tools, handlers, context, prompts
│   │
│   ├── spark-advisor-gateway/               # SERVICE (FastAPI + nats-py)
│   │   ├── Dockerfile
│   │   └── src/spark_advisor_gateway/
│   │       ├── app.py                       # FastAPI app factory + lifespan
│   │       ├── config.py                    # GatewaySettings
│   │       ├── api/                         # routes, schemas, health
│   │       └── task/                        # models, store, manager, executor
│   │
│   ├── spark-advisor-hs-connector/          # SERVICE (FastStream + NATS)
│   │   ├── Dockerfile
│   │   └── src/spark_advisor_hs_connector/
│   │       ├── app.py                       # FastStream app + asyncio polling loop
│   │       ├── config.py                    # ConnectorSettings
│   │       ├── handlers.py                  # @subscriber("job.fetch"), @subscriber("apps.list")
│   │       ├── hs_fetcher.py               # Shared fetch logic (DRY)
│   │       ├── history_server_client.py     # HistoryServerClient (httpx)
│   │       ├── history_server_mapper.py     # HS REST → JobAnalysis
│   │       ├── model/output.py              # ApplicationSummary, Attempt (private)
│   │       ├── poller.py                    # HistoryServerPoller (batch)
│   │       └── polling_state.py             # PollingState (in-memory)
│   │
│   ├── spark-advisor-parser/                # LIBRARY (shared event log parser)
│   │   └── src/spark_advisor_parser/
│   │       └── parser.py                    # parse_event_log(), compression support (.gz, .lz4, .snappy, .zstd)
│   │
│   ├── spark-advisor-storage-connector/     # SERVICE (FastStream + NATS)
│   │   ├── Dockerfile                       # ARG CONNECTOR_TYPE — conditional extras install
│   │   └── src/spark_advisor_storage_connector/
│   │       ├── app.py                       # FastStream app + lazy connector import
│   │       ├── config.py                    # StorageConnectorSettings, ConnectorType enum
│   │       ├── handlers.py                  # create_router(subject, connector_type)
│   │       ├── connectors/                  # protocol.py, hdfs.py, s3.py, gcs.py
│   │       ├── event_log_builder.py         # fetch bytes → temp file → parse_event_log()
│   │       ├── poller.py                    # StoragePoller (batch)
│   │       └── store.py                     # PollingStore (SQLite)
│   │
│   ├── spark-advisor-cli/                   # APP (Typer + Rich)
│   │   └── src/spark_advisor_cli/
│   │       ├── app.py                       # Typer entry point
│   │       ├── commands/                    # analyze, scan, version
│   │       └── output/                      # Rich console
│   │
│   ├── spark-advisor-mcp/                   # APP (MCP Server, stdio)
│   │   └── src/spark_advisor_mcp/
│   │       ├── __main__.py                  # python -m spark_advisor_mcp
│   │       ├── server.py                    # FastMCP + 7 tool definitions
│   │       ├── formatting.py                # Markdown formatters for MCP responses
│   │       └── metric_explanations.py       # Metric definitions + threshold-based assessment
│   │
│   └── frontend/                            # SPA (React 19 + Vite 6 + nginx)
│       ├── Dockerfile                       # Multi-stage: node build → nginx
│       ├── nginx.conf                       # Reverse proxy /api/ → gateway, SPA fallback
│       └── src/
│           ├── pages/                       # Tasks, TaskDetail, Analyze, Statistics
│           ├── components/                  # Shared UI components
│           └── hooks/                       # WebSocket, data fetching
│
├── sample_event_logs/
└── docs/
    ├── architecture.md                      # this file
    ├── mcp-setup.md                         # MCP server setup guide
    └── diagrams/                            # Mermaid diagrams (*.mmd)
```

---

## Package Dependency Graph

```
spark-advisor-models          (contracts, ~500 lines)
    ▲  ▲  ▲  ▲  ▲  ▲
    │  │  │  │  │  │
    │  │  │  │  │  spark-advisor-rules       (rules engine, ~450 lines)
    │  │  │  │  │      ▲  ▲
    │  │  │  │  │      │  │
    │  │  │  │  │      │  spark-advisor-analyzer    (AI worker)
    │  │  │  │  │      │      ▲
    │  │  │  │  │      │      │
    │  │  │  │  spark-advisor-hs-connector          (HS integration)
    │  │  │  │       ▲        │
    │  │  │  │       │        │
    │  │  │  │       spark-advisor-cli              (CLI — depends on rules, analyzer, hs-connector, parser)
    │  │  │  │       spark-advisor-mcp              (MCP — depends on rules, analyzer, hs-connector, cli, parser)
    │  │  │  │
    │  │  │  spark-advisor-parser                   (event log parser, compression)
    │  │  │       ▲
    │  │  │       │
    │  │  │       spark-advisor-storage-connector   (HDFS/S3/GCS, depends on models + parser)
    │  │  │
    │  │  spark-advisor-gateway                     (API gateway)
```

Key constraints:
- **gateway** depends ONLY on models — doesn't know about rules or AI
- **hs-connector** depends ONLY on models — owns HS client and mapper
- **storage-connector** depends on models and parser — owns storage clients (HDFS/S3/GCS) and polling
- **parser** is a shared library — used by CLI, MCP, and storage-connector
- **rules** is pure business logic — zero I/O, zero infrastructure dependencies
- **analyzer** depends on models and rules (runs static analysis + AI)
- **cli** depends on models, rules, analyzer, hs-connector, and parser

> Diagram: [02-package-dependencies.mmd](diagrams/02-package-dependencies.mmd)

---

## Detailed Service Descriptions

### spark-advisor-models (Library)

Foundation package. All Pydantic contracts that flow through NATS live here. No infrastructure, no I/O.

**Owns:**
- `model/metrics.py` — `JobAnalysis`, `StageMetrics`, `ExecutorMetrics`, `TaskMetrics`, `Quantiles`, `TaskMetricsDistributions`
- `model/spark_config.py` — `SparkConfig` (typed property accessors for `spark.*` configuration)
- `model/output.py` — `AnalysisMode`, `Severity`, `OutputFormat`, `RuleResult`, `Recommendation`, `AdvisorReport`, `AnalysisResult`
- `model/input.py` — `AnalysisToolInput`, `RecommendationInput` (Claude tool schema, generated via `model_json_schema()`)
- `config.py` — `Thresholds` (rule thresholds), `AiSettings` (model, timeout, enabled flag)
- `settings.py` — `BaseServiceSettings` (env → .env → YAML source chain), `OtelSettings`, `NatsSettings` (base NATS DTO)
- `logging.py` — `configure_logging()` (structlog setup with JSON/Console), `bind_nats_context()` (correlation context), `nats_handler_context()` (async context manager combining logging + tracing for NATS handlers)
- `tracing.py` — `TracingConfig` (class wrapping OTel global provider), `configure_tracing()`, `get_tracer()`, `inject_trace_context()` / `extract_trace_context()` (W3C Traceparent via NATS headers), `add_otel_trace_context` (structlog processor)
- `util/bytes.py` — `format_bytes()` (human-readable byte formatting)
- `util/stats.py` — `percentile_value()`, `median_value()`, `quantiles_5()`

**Config env vars:** None (library, not a service)

**Key design decisions:**
- All Pydantic models use `frozen=True` for immutability
- `__all__` defined in every `__init__.py` for explicit public API
- `BaseServiceSettings` uses `pydantic-settings` with source priority: init → env → .env → YAML → file secrets
- `NatsSettings` is a `BaseModel` (not `BaseSettings`) — inherited by service-specific NATS configs

> Diagram: [03-models-package.mmd](diagrams/03-models-package.mmd)

### spark-advisor-rules (Library)

Deterministic rules engine. Pure business logic operating on models. Used in two places: analyzer (full analysis) and CLI.

**Owns:**
- `rules.py` — `Rule` ABC with `_check_stage()` template method, 11 concrete rules, `rules_for_threshold()` and `default_rules()` factories
- `static_analysis.py` — `StaticAnalysisService` (runs all rules, returns sorted results)

**Rules (11 total):**

| Rule                         | Condition                                                | Severity                                    |
|------------------------------|----------------------------------------------------------|---------------------------------------------|
| `DataSkewRule`               | max_task_duration / median > 5x                          | CRITICAL if >10x, WARNING if >5x            |
| `SpillToDiskRule`            | diskBytesSpilled > 0                                     | CRITICAL if >1GB, WARNING if >0.1GB         |
| `GCPressureRule`             | GC time > 20% of task time                               | CRITICAL if >40%, WARNING if >20%           |
| `ShufflePartitionsRule`      | Actual partitions far from optimal (128MB/partition)     | WARNING                                     |
| `ExecutorIdleRule`           | slot utilization < 40%                                   | CRITICAL if <20%, WARNING if <40% (skipped when CPU data unavailable) |
| `TaskFailureRule`            | failed_task_count > 0                                    | CRITICAL if >=10, WARNING if >0             |
| `SmallFileRule`              | avg input bytes per task < 10MB                          | CRITICAL if <1MB, WARNING if <10MB          |
| `BroadcastJoinThresholdRule` | threshold disabled (-1) with shuffle stages              | WARNING if disabled, INFO if < 10MB         |
| `SerializerChoiceRule`       | Java serializer with shuffle stages                      | INFO                                        |
| `DynamicAllocationRule`      | enabled without bounds, or disabled with low utilization | WARNING                                     |
| `ExecutorMemoryOverheadRule` | GC > 20% AND memory utilization > 80%                    | WARNING                                     |

**Key design decisions:**
- Rules take `Thresholds` via constructor injection (configurable thresholds, not hardcoded)
- `Rule` is abstract. Each rule overrides `_check_stage()` for per-stage rules or `evaluate()` for job-level rules
- Zero infrastructure dependencies — only depends on models

> Diagram: [08-rules-hierarchy.mmd](diagrams/08-rules-hierarchy.mmd)

### spark-advisor-analyzer (Service)

AI-powered analysis worker. The only service that talks to Claude API. Subscribes to NATS `analysis.run`, runs rules + AI, replies with `AnalysisResult`.

**Owns:**
- `handlers.py` — FastStream NATS handlers (`@broker.subscriber("analysis.run")`, `@broker.subscriber("analysis.run.agent")`)
- `orchestrator.py` — `AdviceOrchestrator` (coordinates rules + optional AI + optional agent)
- `ai/client.py` — `AnthropicClient` (thin wrapper over `anthropic.Anthropic`)
- `ai/service.py` — `LlmAnalysisService` (Claude API call + response validation)
- `ai/prompts.py` — `build_user_message()`, `build_system_prompt()`
- `ai/tool_config.py` — `ANALYSIS_TOOL`, `SYSTEM_PROMPT_TEMPLATE`
- `agent/orchestrator.py` — `AgentOrchestrator` (multi-turn Claude tool_use loop, max 10 iterations)
- `agent/tools.py` — 6 agent tool definitions as Pydantic models + `AGENT_TOOLS` list
- `agent/handlers.py` — tool dispatch via match statement + 5 handler functions
- `agent/context.py` — `AgentContext` dataclass (mutable session state)
- `agent/prompts.py` — `AGENT_SYSTEM_PROMPT`, `build_initial_message()`
- `config.py` — `AnalyzerSettings` (inherits `BaseServiceSettings`)

**Config env vars:**
- `SA_ANALYZER_NATS__URL` — NATS server URL
- `SA_ANALYZER_AI__ENABLED` — enable/disable AI (default: true)
- `SA_ANALYZER_AI__MODEL` — Claude model (default: claude-sonnet-4-6)
- `SA_ANALYZER_AI__API_TIMEOUT` — API call timeout in seconds
- `ANTHROPIC_API_KEY` — Claude API key (via Secret, not ConfigMap)
- `SA_ANALYZER_SERVICE_NAME` — service name for logging and tracing (default: spark-advisor-analyzer)
- `SA_ANALYZER_OTEL__ENABLED` — enable OpenTelemetry tracing (default: false, Helm: true)
- `SA_ANALYZER_OTEL__ENDPOINT` — OTLP gRPC endpoint (default: http://localhost:4317)

**Key design decisions:**
- `asyncio.to_thread()` wraps synchronous `orchestrator.run()` because `AnthropicClient` uses synchronous httpx
- Handler has both `@subscriber` and `@publisher` decorators: reply goes to caller (on-demand) AND publishes to `analysis.result` (batch flow)
- AI is optional — if `ANTHROPIC_API_KEY` is missing, analyzer logs warning and runs rules-only
- Tool schema generated from Pydantic models (`model_json_schema()`) — single source of truth
- Agent mode: `AgentOrchestrator` runs multi-turn loop where Claude calls 6 tools locally (no API calls). Tools operate on in-memory `JobAnalysis`. Loop terminates on `submit_final_report` or max iterations (force-submit fallback)

> Diagram: [04-analyzer-pipeline.mmd](diagrams/04-analyzer-pipeline.mmd)

### spark-advisor-gateway (Service)

API Gateway — the only externally-exposed service. 18 REST endpoints + WebSocket + async task orchestration via NATS. Does NOT contain analysis logic or HS client.

**Owns:**
- `api/routes.py` — 18 REST endpoints (analyze, tasks, apps, history, rules, config, stats)
- `api/schemas.py` — 18 Pydantic response/request models with OpenAPI examples and tags
- `api/health.py` — `GET /health/live`, `GET /health/ready` (NATS check)
- `ws/manager.py` — `ConnectionManager` (WebSocket broadcast, heartbeat, stale cleanup)
- `ws/routes.py` — `WS /api/v1/ws/tasks` (real-time task status updates)
- `task/models.py` — `TaskStatus` enum, `AnalysisTask` dataclass
- `task/store.py` — `TaskStore` (SQLAlchemy async + SQLite WAL, stats queries)
- `task/manager.py` — `TaskManager` (CRUD, deduplication, stats aggregation, WebSocket callback)
- `task/executor.py` — `TaskExecutor` (NATS request-reply + polling job execution)

**REST API (tagged, OpenAPI documented):**

| Method | Path | Tag | Description |
|--------|------|-----|-------------|
| `POST` | `/api/v1/analyze` | analysis | Submit analysis (202 new, 409 duplicate) |
| `GET` | `/api/v1/tasks` | tasks | List with filtering + pagination |
| `GET` | `/api/v1/tasks/stats` | tasks | Count by status |
| `GET` | `/api/v1/tasks/{id}` | tasks | Task details + result |
| `GET` | `/api/v1/tasks/{id}/rules` | tasks | Rule violations (409 if not completed) |
| `GET` | `/api/v1/tasks/{id}/config` | tasks | Config comparison: rule + AI merged |
| `GET` | `/api/v1/applications` | applications | HS apps (paginated) |
| `GET` | `/api/v1/apps/{app_id}/history` | applications | Analysis history per app |
| `GET` | `/api/v1/stats/summary` | statistics | Totals, avg_duration, ai_usage% |
| `GET` | `/api/v1/stats/rules` | statistics | Rule frequency |
| `GET` | `/api/v1/stats/daily-volume` | statistics | Daily analysis count |
| `GET` | `/api/v1/stats/top-issues` | statistics | Most common issues |
| `WS` | `/api/v1/ws/tasks` | websocket | Real-time status broadcasts |

**Config env vars:**
- `SA_GATEWAY_NATS__URL` — NATS server URL
- `SA_GATEWAY_SERVER__HOST` — bind host (default: 0.0.0.0)
- `SA_GATEWAY_SERVER__PORT` — bind port (default: 8080)
- `SA_GATEWAY_NATS__FETCH_TIMEOUT` — HS fetch timeout (default: 30s)
- `SA_GATEWAY_NATS__ANALYZE_TIMEOUT` — analysis timeout (default: 120s)
- `SA_GATEWAY_NATS__ANALYZE_AGENT_TIMEOUT` — agent mode timeout (default: 300s)
- `SA_GATEWAY_DATABASE_URL` — SQLite database URL (default: `sqlite+aiosqlite:///data/spark_advisor.db`)
- `SA_GATEWAY_WS_HEARTBEAT_INTERVAL` — WebSocket heartbeat (default: 30s)
- `SA_GATEWAY_NATS__POLLING_ANALYSIS_MODE` — analysis mode for poller-submitted tasks (default: static)
- `SA_GATEWAY_METRICS_ENABLED` — enable Prometheus metrics on `/metrics` (default: false, Helm: true)
- `SA_GATEWAY_SERVICE_NAME` — service name for logging and tracing (default: spark-advisor-gateway)
- `SA_GATEWAY_OTEL__ENABLED` — enable OpenTelemetry tracing (default: false, Helm: true)
- `SA_GATEWAY_OTEL__ENDPOINT` — OTLP gRPC endpoint (default: http://localhost:4317)

**Key design decisions:**
- Uses `nats-py` (not FastStream) — gateway needs request-reply with explicit timeout control, not subscriber pattern
- `TaskStore` uses SQLAlchemy async + SQLite with WAL mode for persistent task storage across restarts
- `TaskExecutor.submit()` fires an `asyncio.create_task()` — non-blocking for the HTTP handler
- `AnalyzeRequest.mode` uses shared `AnalysisMode` StrEnum — routes to `analysis.run` (standard) or `analysis.run.agent` (agent) with appropriate timeout
- WebSocket replaces SSE — `ConnectionManager` broadcasts via `on_status_change` callback from `TaskManager` (avoids circular imports)
- Stats endpoints aggregate from `result_json` blobs in Python — no denormalized tables, simple and sufficient for <10K tasks
- `app_id` validated with `pattern=r"^[a-zA-Z0-9_\-]+$"` + `max_length=128` for anti-traversal

> Diagram: [10-gateway-task-lifecycle.mmd](diagrams/10-gateway-task-lifecycle.mmd)

### spark-advisor-hs-connector (Service)

The sole owner of Spark History Server integration. Two responsibilities:
1. **On-demand fetch** — subscribes to NATS `job.fetch`, returns `JobAnalysis` for a single app
2. **Batch polling** — asyncio background task periodically polls HS for new jobs, publishes to `analysis.submit` (gateway creates task, then forwards to analyzer)

**Owns:**
- `handlers.py` — FastStream handlers for `job.fetch` (on-demand) and `apps.list`
- `hs_fetcher.py` — `fetch_job_analysis()` (shared logic between handler and poller — DRY)
- `history_server_client.py` — `HistoryServerClient` (httpx REST client)
- `history_server_mapper.py` — `map_job_analysis()` (HS REST responses → `JobAnalysis`)
- `poller.py` — `HistoryServerPoller` (batch polling)
- `polling_state.py` — `PollingState` (tracks last seen app to avoid duplicates)
- `model/output.py` — `ApplicationSummary`, `Attempt` (private to this service, not in models package)

**Config env vars:**
- `SA_HS_CONNECTOR_NATS__URL` — NATS server URL
- `SA_HS_CONNECTOR_HISTORY_SERVER__URL` — Spark History Server URL
- `SA_HS_CONNECTOR_HISTORY_SERVER__TIMEOUT` — HTTP timeout (default: 30s)
- `SA_HS_CONNECTOR_POLLING_ENABLED` — enable/disable background polling (default: false)
- `SA_HS_CONNECTOR_POLL_INTERVAL_SECONDS` — batch poll interval (default: 60)
- `SA_HS_CONNECTOR_BATCH_SIZE` — max jobs per poll (default: 50)
- `SA_HS_CONNECTOR_SERVICE_NAME` — service name for logging and tracing (default: spark-advisor-hs-connector)
- `SA_HS_CONNECTOR_OTEL__ENABLED` — enable OpenTelemetry tracing + httpx auto-instrumentation (default: false, Helm: true)
- `SA_HS_CONNECTOR_OTEL__ENDPOINT` — OTLP gRPC endpoint (default: http://localhost:4317)

**History Server endpoints used:**
- `GET /api/v1/applications/{app-id}` — app metadata, duration
- `GET /api/v1/applications/{app-id}/environment` — all `spark.*` config
- `GET /api/v1/applications/{app-id}/stages?status=complete` — stage metrics
- `GET /api/v1/applications/{app-id}/stages/{id}/0/taskSummary?quantiles=...` — task distribution
- `GET /api/v1/applications/{app-id}/executors` — executor metrics

**Key design decisions:**
- `ApplicationSummary` and `Attempt` are private models — they represent the HS REST API response format, not domain contracts
- `fetch_job_analysis()` is a standalone function used by both handler and poller — single place for the 5-endpoint fetch logic
- Poller publishes fire-and-forget to `analysis.submit` (gateway creates task, then forwards to analyzer — single persistence path)
- `PollingState` is in-memory — acceptable because missing a poll cycle just means re-checking the same apps

### spark-advisor-parser (Library)

Shared event log parser used by CLI, MCP, and storage-connector. Extracts `JobAnalysis` from Spark event log files with auto-detection of compression format.

**Owns:**
- `parser.py` — `parse_event_log()` function, `_open_event_log()` context manager with compression support

**Supported formats:** `.json`, `.json.gz`, `.lz4`, `.snappy`, `.zstd`, `.zst`

**Key design decisions:**
- Streams line-by-line — never loads entire file into memory (files can be 100MB+)
- Only ~1-5% of event types are relevant (SparkListenerStageCompleted, TaskEnd, etc.) — rest are skipped
- Compression auto-detected by file extension
- Extracted from CLI as shared library to avoid duplicating parsing logic in storage-connector

### spark-advisor-storage-connector (Service)

Reads Spark event logs from cloud/distributed storage systems. Strategy pattern with 3 connector implementations (HDFS, S3, GCS). Two responsibilities:
1. **On-demand fetch** — subscribes to NATS `storage.fetch.{type}`, returns `JobAnalysis` for a given event log URI
2. **Batch polling** — asyncio background task periodically polls storage for new event logs, publishes to `analysis.submit`

**Owns:**
- `connectors/protocol.py` — `StorageConnector` protocol + `EventLogRef` dataclass
- `connectors/hdfs.py` — `HdfsConnector` (httpx WebHDFS REST API)
- `connectors/s3.py` — `S3Connector` (aiobotocore, optional dep `[s3]`)
- `connectors/gcs.py` — `GcsConnector` (gcloud-aio-storage, optional dep `[gcs]`)
- `handlers.py` — `create_router(subject, connector_type)` — dynamic NATS handler
- `event_log_builder.py` — fetch bytes → temp file → `parse_event_log()` (delegates to parser package)
- `poller.py` — `StoragePoller` (batch polling)
- `store.py` — `PollingStore` (SQLite, tracks processed apps)
- `config.py` — `StorageConnectorSettings`, `ConnectorType` enum, per-connector settings

**Config env vars:**
- `SA_STORAGE_NATS__URL` — NATS server URL
- `SA_STORAGE_CONNECTOR_TYPE` — storage backend: `hdfs`, `s3`, or `gcs` (default: hdfs)
- `SA_STORAGE_POLLING_ENABLED` — enable/disable background polling (default: false)
- `SA_STORAGE_POLL_INTERVAL_SECONDS` — poll interval (default: 60)
- `SA_STORAGE_BATCH_SIZE` — max event logs per poll (default: 50)
- `SA_STORAGE_DATABASE_URL` — auto-generated per connector type (e.g. `storage_hdfs_connector.db`)
- `SA_STORAGE_HDFS__NAMENODE_URL` — HDFS NameNode WebHDFS URL (default: http://localhost:9870)
- `SA_STORAGE_S3__BUCKET`, `SA_STORAGE_S3__REGION`, `SA_STORAGE_S3__ENDPOINT_URL` — S3 settings
- `SA_STORAGE_GCS__BUCKET`, `SA_STORAGE_GCS__PROJECT_ID` — GCS settings
- `SA_STORAGE_OTEL__ENABLED` — enable OpenTelemetry tracing (default: false, Helm: true)

**NATS subjects (per connector type):**
- `storage.fetch.hdfs` — on-demand fetch from HDFS
- `storage.fetch.s3` — on-demand fetch from S3
- `storage.fetch.gcs` — on-demand fetch from GCS

**Key design decisions:**
- Strategy pattern — `StorageConnector` protocol with 3 implementations, selected at startup via `ConnectorType` enum
- `app.py` lazy-imports only the selected connector — unused storage libraries are never loaded
- Dockerfile uses `ARG CONNECTOR_TYPE` for conditional extras install (`--extra s3`, `--extra gcs`, or nothing for HDFS)
- HDFS uses httpx WebHDFS REST API — zero extra dependency (httpx already in project)
- S3/GCS top-level imports in their modules, but modules only imported on demand via match/case
- Database URL auto-includes connector type (`storage_hdfs_connector.db`) — safe to swap connector type without PVC conflicts
- Multiple instances with different `CONNECTOR_TYPE` can run on same cluster with separate NATS subjects and databases
- OpenTelemetry span names per connector type (`storage.fetch.hdfs`, `storage.fetch.s3`, `storage.fetch.gcs`)

### spark-advisor-cli (App)

Standalone CLI for Spark job analysis. Supports event log files and History Server as data sources, rules engine, and optional AI analysis via Claude.

**Owns:**
- `commands/analyze.py` — `analyze` command (event log file or History Server → rules + optional AI → report)
- `commands/scan.py` — `scan` command (list recent apps from History Server)
- `commands/version.py` — `version` command
- `output/console.py` — Rich terminal output (tables, panels, colors)

**Two data source modes:**
- File path — parses event log via `spark-advisor-parser`, runs rules (+ optional AI), prints report. Zero infrastructure needed.
- `--history-server` / `-hs` — fetches from History Server via httpx, runs rules (+ optional AI). Requires running HS.

**Key design decisions:**
- Uses `spark-advisor-parser` package for event log parsing (shared with storage-connector)
- Both HS source and event log source produce the same `JobAnalysis` model — the entire downstream pipeline is identical
- CLI imports `AdviceOrchestrator` from analyzer and `HistoryServerClient` from hs-connector — reuses existing logic without duplication

### spark-advisor-mcp (App)

MCP server exposing spark-advisor as tools for Claude Desktop, Cursor, and other MCP clients. Uses stdio transport (JSON-RPC over stdin/stdout).

**Owns:**
- `server.py` — `FastMCP("spark-advisor")` instance + 7 tool definitions (`@mcp.tool()`)
- `formatting.py` — Markdown formatters for tool responses (not Rich — MCP returns plain text)
- `metric_explanations.py` — Metric definitions + threshold-based assessment

**7 MCP tools:**

| Tool                | Parameters                                      | Reuses                                          |
|---------------------|-------------------------------------------------|-------------------------------------------------|
| `analyze_spark_job` | `source`, `history_server?`, `mode?`             | `parse_event_log()`, `AdviceOrchestrator.run()` |
| `scan_recent_jobs`  | `history_server`, `limit?`                      | `HistoryServerClient.list_applications()`       |
| `get_job_config`    | `source`, `history_server?`                     | `_load_job()` → `job.config`                    |
| `suggest_config`    | `source`, `history_server?`                     | Rules engine → `RuleResult.recommended_value`   |
| `explain_metric`    | `metric_name`, `value?`                         | `METRIC_EXPLANATIONS` knowledge base            |
| `get_stage_details` | `source`, `stage_id`, `history_server?`         | `_load_job()` → stage metrics formatting        |
| `compare_jobs`      | `source_a`, `source_b`, `history_server?`       | `_load_job()` → side-by-side comparison         |

**Key design decisions:**
- Uses `mcp` SDK (`FastMCP`) — tools are plain Python functions decorated with `@mcp.tool()`
- Docstrings become tool descriptions automatically; type hints become tool parameters
- Reuses existing components via local imports (parser from cli, orchestrator from analyzer, HS client from hs-connector)
- Logging redirected to stderr (`logging.basicConfig(stream=sys.stderr)`) — stdout is reserved for MCP JSON-RPC protocol
- AI is optional — if `ANTHROPIC_API_KEY` is not set, `analyze_spark_job` runs rules-only

> Setup guide: [mcp-setup.md](mcp-setup.md)

### frontend (SPA)

React 19 dashboard for spark-advisor. Provides a browser-based UI for submitting analyses, viewing results, and monitoring statistics.

**Tech stack:** React 19 + TypeScript + Vite 6, nginx reverse proxy in Docker/K8s.

**Pages:**
- **Tasks list** — filterable, paginated list of analysis tasks
- **Task detail** — 4 tabs: summary, rule violations, config comparison, raw result
- **Analyze** — submit new analysis with History Server app autocomplete
- **Statistics** — dashboard with summary, rule frequency, daily volume, top issues

**Deployment modes:**
- **Docker / K8s** — nginx serves the built SPA and reverse-proxies `/api/` to gateway (with WebSocket upgrade support)
- **Local dev (without Docker)** — gateway mounts `frontend/dist/` via FastAPI `StaticFiles` as a fallback, no separate web server needed

**Real-time updates:** WebSocket connection to `WS /api/v1/ws/tasks` for live task status changes.

**Key design decisions:**
- Frontend has no direct NATS dependency — all communication goes through gateway REST API and WebSocket
- nginx handles SPA routing (all non-API paths serve `index.html`)
- Reverse proxy config supports WebSocket upgrade for real-time task updates

---

## Data Flows

### On-demand Flow (User requests specific job analysis)

```
User ─── POST /api/v1/analyze {"app_id":"app-123", "mode":"standard|agent"} ──► Gateway
                                                          │
Gateway ─── NATS request("job.fetch") ──────────────────► HS Connector
HS Connector ─── REST (5 endpoints) ───────────────────► Spark History Server
HS Connector ─── reply(JobAnalysis) ───────────────────► Gateway
                                                          │
Gateway ─── NATS request("analysis.run"                ─► Analyzer (standard, 120s)
         or "analysis.run.agent") ─────────────────────── Analyzer (agent, 300s)
Analyzer ─── rules + Claude API ──────────────────────── (internal)
Analyzer ─── reply(AnalysisResult) ────────────────────► Gateway
                                                          │
Gateway ─── HTTP 200 (poll GET /tasks/{id}) ───────────► User
```

> Diagram: [07-on-demand-flow.mmd](diagrams/07-on-demand-flow.mmd)

### Batch Flow (Automatic discovery of new jobs)

> Polling is disabled by default (`SA_HS_CONNECTOR_POLLING_ENABLED=false`). When enabled, the default analysis mode for poller-submitted tasks is static (`SA_GATEWAY_NATS__POLLING_ANALYSIS_MODE=static`).

```
asyncio loop (every 60s) ──► HS Connector.poll()
HS Connector ─── REST /applications ───────────────────► Spark History Server
HS Connector ─── for each new job:
    ├── fetch_job_from_hs(app_id) ─────────────────────► Spark History Server
    └── NATS publish("analysis.submit", JobAnalysis) ──► Gateway
                                                          │
Gateway ─── creates task ─── NATS request("analysis.run") ► Analyzer
Analyzer ─── rules + Claude API ──────────────────────── (internal)
Analyzer ─── reply(AnalysisResult) ───────────────────► Gateway
                                                          │
Gateway ─── stores result (available via GET /tasks) ──► Clients
```

> Diagram: [09-batch-flow.mmd](diagrams/09-batch-flow.mmd)

### CLI Local Flow (Event log file)

```
spark-advisor analyze /path/to/event-log.json.gz
    │
    ├── parse_event_log() ──► stream line-by-line ──► JobAnalysis
    ├── AdviceOrchestrator.run(job) ──► rules + optional AI ──► AnalysisResult
    └── Rich console output ──► Terminal
```

### CLI Agent Flow

```
spark-advisor analyze /path/to/event-log.json.gz --agent
    │
    ├── parse_event_log() ──► stream line-by-line ──► JobAnalysis
    ├── AgentOrchestrator.run(job)
    │     ├── build initial message (brief job summary)
    │     └── LOOP (max 10 iterations):
    │         ├── Claude API call (tool_choice="auto")
    │         ├── if submit_final_report → parse, validate, return AnalysisResult
    │         ├── if other tool → execute locally → append result
    │         └── if text only → nudge to submit
    └── Rich console output ──► Terminal
```

### CLI History Server Flow

```
spark-advisor analyze app-123 --history-server http://yarn:18080
    │
    ├── HistoryServerClient + fetch_job_analysis() ──► JobAnalysis
    ├── AdviceOrchestrator.run(job) ──► rules + optional AI ──► AnalysisResult
    └── Rich console output ──► Terminal
```

---

## NATS Subjects

| Subject              | Pattern        | Publisher    | Subscriber   | Payload                                                     |
|----------------------|----------------|--------------|--------------|-------------------------------------------------------------|
| `job.fetch`          | request-reply  | Gateway      | HS Connector | Request: `{"app_id": "..."}` / Reply: `JobAnalysis`         |
| `apps.list`          | request-reply  | Gateway      | HS Connector | Request: `{"limit": N}` / Reply: `list[ApplicationSummary]` |
| `analysis.submit`    | pub-sub        | HS Connector / Storage Connector | Gateway | `JobAnalysis` (gateway creates task, forwards to analyzer)  |
| `storage.fetch.hdfs` | request-reply  | Gateway      | Storage Connector | `StorageFetchRequest` / Reply: `JobAnalysis`           |
| `storage.fetch.s3`   | request-reply  | Gateway      | Storage Connector | `StorageFetchRequest` / Reply: `JobAnalysis`           |
| `storage.fetch.gcs`  | request-reply  | Gateway      | Storage Connector | `StorageFetchRequest` / Reply: `JobAnalysis`           |
| `analysis.run`       | request-reply  | Gateway      | Analyzer     | `JobAnalysis`                                               |
| `analysis.run.agent` | request-reply  | Gateway      | Analyzer     | `JobAnalysis` (triggers agent mode — multi-turn tool_use)   |
| `analysis.result`    | pub-sub        | Analyzer     | Gateway      | `AnalysisResult`                                            |

**Message format:** Plain JSON. FastStream handles Pydantic model serialization/deserialization automatically. No envelope wrapper — models are serialized directly.

**Schema evolution:** All NATS message types are Pydantic models with `frozen=True` in the `models` package. Adding optional fields with defaults is backward-compatible. Breaking changes require coordinated deployment.

> Diagram: [05-nats-subjects.mmd](diagrams/05-nats-subjects.mmd)

---

## Configuration Management

All services use `pydantic-settings` with a consistent source priority chain:

```
init values → environment variables → .env file → YAML config → file secrets → defaults
```

### BaseServiceSettings (shared base)

Defined in `spark_advisor_models.settings`. Provides:
- YAML config source support (for K8s ConfigMap mounted as files)
- `.env` file support (for local development)
- Nested delimiter `__` (e.g., `SA_ANALYZER_NATS__URL=nats://prod:4222`)
- `extra="ignore"` (unknown env vars don't cause errors)

### Per-service env prefixes

| Service      | Env prefix      | YAML config path                              |
|--------------|-----------------|-----------------------------------------------|
| Analyzer     | `SA_ANALYZER_`  | `/etc/spark-advisor/analyzer/config.yaml`     |
| HS Connector | `SA_HS_CONNECTOR_` | `/etc/spark-advisor/hs-connector/config.yaml` |
| Gateway      | `SA_GATEWAY_`   | `/etc/spark-advisor/gateway/config.yaml`      |

### NatsSettings inheritance

```
NatsSettings (base: url only)
    ├── AnalyzerNatsSettings (+ run_subject, result_subject)
    ├── ConnectorNatsSettings (+ fetch_subject, submit_subject)
    └── GatewayNatsSettings (+ fetch_subject, run_subject, result_subject, submit_subject, timeouts)
```

### Kubernetes ConfigMap example

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: analyzer-config
data:
  SA_ANALYZER_NATS__URL: "nats://nats.nats.svc:4222"
  SA_ANALYZER_AI__ENABLED: "true"
  SA_ANALYZER_AI__MODEL: "claude-sonnet-4-6"
  SA_ANALYZER_LOG_LEVEL: "INFO"
```

---

## Health and Readiness

Each service exposes health endpoints for Kubernetes probes:

| Service           | Liveness                                | Readiness                                    |
|-------------------|-----------------------------------------|----------------------------------------------|
| Gateway           | `GET /health/live` → `{"status": "ok"}` | `GET /health/ready` → checks NATS + SQLite   |
| Analyzer          | Kubernetes exec probe (stdlib socket → NATS port) | NATS broker connected              |
| HS Connector      | Kubernetes exec probe (stdlib socket → NATS port) | NATS broker connected              |
| Storage Connector | Kubernetes exec probe (stdlib socket → NATS port) | NATS broker connected              |

Gateway readiness returns `"degraded"` if NATS is disconnected. Kubernetes will stop routing traffic until reconnection.

Analyzer, HS Connector, and Storage Connector use lightweight inline socket probes in Helm charts (~44ms latency vs ~313ms for loading the Python module).

---

## Error Handling

| Scenario                      | Behavior                                                                                                                     |
|-------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| **NATS unavailable**          | Services fail readiness check. K8s stops routing traffic. FastStream auto-reconnects.                                        |
| **HS unreachable**            | `fetch_job_from_hs()` raises `httpx.ConnectError`. Gateway task marked FAILED with error message.                            |
| **Claude API error**          | `LlmAnalysisService` catches exception, logs warning. `AdviceOrchestrator` returns rules-only result (graceful degradation). |
| **Claude API key missing**    | Analyzer starts in rules-only mode. Logs warning at startup.                                                                 |
| **NATS request timeout**      | Gateway `TaskExecutor` catches `TimeoutError`. Task marked FAILED. Configurable via `fetch_timeout` / `analyze_timeout`.     |
| **Malformed message on NATS** | FastStream Pydantic validation rejects invalid JSON. Message is not processed.                                               |
| **Large event log**           | CLI streaming parser processes line-by-line. Memory usage stays constant regardless of file size.                            |

No DLQ (dead letter queue), no circuit breaker, no rate limiter. These are intentionally excluded to keep the system simple. Graceful degradation (rules-only fallback) handles the most common failure mode (Claude API issues).

---

## Observability

All observability features are **optional** — disabled by default in docker-compose (local dev), enabled by default in Helm charts (production).

### Structured Logging (structlog)

All services use `configure_logging()` from `models/logging.py` with shared processors: `merge_contextvars`, `add_log_level`, `TimeStamper`, `add_otel_trace_context`. Foreign logs (uvicorn, faststream) are also structured via `ProcessorFormatter.foreign_pre_chain`.

- **JSON mode** (`SA_*_JSON_LOG=true`): machine-readable, includes `trace_id` and `span_id` when OTel active
- **Console mode** (default): human-readable with colors

### Distributed Tracing (OpenTelemetry + Grafana Tempo)

W3C Traceparent propagation via NATS message headers. `TracingConfig` wraps OTel's global `TracerProvider` — when disabled, `get_tracer()` returns NoOp tracer (zero overhead).

- **Gateway**: `FastAPIInstrumentor` auto-instruments HTTP, manual spans: `gateway.execute_analysis`, `gateway.nats_fetch_job`, `gateway.nats_analyze`, `gateway.handle_polling`
- **Analyzer**: span `analyzer.analyze` with `app_id` and `mode` attributes
- **HS Connector**: `HTTPXClientInstrumentor` auto-instruments httpx, manual spans: `hs.fetch_job`, `hs.list_applications`
- **Cross-service**: `nats_handler_context()` in `models/logging.py` combines `bind_nats_context()` + `get_tracer().start_as_current_span()` for NATS handlers

### Prometheus Metrics (gateway only)

Optional `/metrics` endpoint with `prometheus-fastapi-instrumentator` (auto HTTP metrics) + custom metrics:
- `sa_tasks_total{status}` — Counter of analysis tasks by status
- `sa_task_duration_seconds{mode}` — Histogram of analysis duration by mode

### Monitoring Stack

```bash
SA_GATEWAY_METRICS_ENABLED=true SA_OTEL_ENABLED=true make up   # Enable metrics + tracing
make monitoring-up                                               # Start Prometheus + Grafana + Tempo
```

Grafana dashboard with 8 panels at `localhost:3001`. Tempo datasource for trace exploration.

---

## Technology Stack

| Tool                  | Role                                                              | Package                            |
|-----------------------|-------------------------------------------------------------------|------------------------------------|
| **Python 3.12+**      | Language                                                          | all                                |
| **uv**                | Package manager, workspace                                        | all                                |
| **Pydantic v2**       | Data models, validation, `frozen=True`                            | models (+ all dependents)          |
| **pydantic-settings** | Config from env/yaml/dotenv                                       | models (base), services (concrete) |
| **NATS**              | Message broker (15MB, single binary)                              | analyzer, hs-connector, gateway    |
| **FastStream[nats]**  | Type-safe NATS workers with `TestNatsBroker`                      | analyzer, hs-connector             |
| **nats-py**           | Async NATS client (request-reply with timeouts)                   | gateway                            |
| **FastAPI**           | REST API                                                          | gateway                            |
| **uvicorn**           | ASGI server                                                       | gateway                            |
| **httpx**             | HTTP client for History Server                                    | hs-connector                       |
| **asyncio**           | Background polling loop (`asyncio.create_task` + `asyncio.sleep`) | hs-connector                       |
| **anthropic**         | Claude API SDK                                                    | analyzer                           |
| **Typer**             | CLI framework                                                     | cli                                |
| **Rich**              | Terminal output (tables, colors, panels)                          | cli                                |
| **orjson**            | Fast JSON parsing for event logs                                  | cli, models                        |
| **mcp**               | MCP server SDK (FastMCP, stdio transport)                         | mcp                                |
| **Ruff**              | Linter + formatter                                                | all                                |
| **mypy**              | Type checker (strict mode)                                        | all                                |
| **pytest**            | Testing                                                           | all                                |
| **SQLAlchemy**        | Async ORM for task persistence (SQLite + WAL)                     | gateway                            |
| **aiosqlite**         | Async SQLite driver for SQLAlchemy                                | gateway                            |
| **structlog**         | Structured logging (JSON/console, contextvars correlation)        | all services (via models)          |
| **opentelemetry**     | Distributed tracing (W3C Traceparent, Grafana Tempo)              | all services (via models)          |
| **prometheus-client** | Prometheus metrics (counters, histograms)                         | gateway (optional)                 |
| **respx**             | HTTP mocking for httpx                                            | hs-connector (tests)               |
| **React 19**          | SPA framework                                                     | frontend                           |
| **TypeScript**        | Type-safe frontend code                                           | frontend                           |
| **Vite 6**            | Build tool + dev server                                           | frontend                           |
| **nginx**             | Static file serving + reverse proxy                               | frontend (Docker/K8s)              |

---

## Kubernetes Deployment

### Helm Charts

Six Helm charts in the `charts/` directory:

```
charts/
├── spark-advisor/       # Umbrella chart — installs everything via `helm install`
│   ├── Chart.yaml       # Dependencies: analyzer, gateway, hs-connector, storage-connector, frontend, nats
│   └── values.yaml      # Global overrides (NATS URL auto-resolved from release name)
├── analyzer/            # NATS worker (rules + AI), ConfigMap with SA_ANALYZER_* env vars
├── gateway/             # REST API, Service, optional Ingress, ConfigMap with SA_GATEWAY_* env vars
├── hs-connector/        # History Server poller, ConfigMap with SA_HS_CONNECTOR_* env vars
├── storage-connector/   # HDFS/S3/GCS event log reader, ConfigMap with SA_STORAGE_* env vars, PVC for SQLite
└── frontend/            # React SPA (nginx), reverse proxy /api/ → gateway
```

**Umbrella chart dependencies:**
- `spark-advisor-analyzer` — local subchart (`file://../analyzer`)
- `spark-advisor-gateway` — local subchart (`file://../gateway`)
- `spark-advisor-hs-connector` — local subchart (`file://../hs-connector`)
- `spark-advisor-storage-connector` — local subchart (`file://../storage-connector`), disabled by default
- `spark-advisor-frontend` — local subchart (`file://../frontend`)
- `nats` — official NATS Helm chart from `https://nats-io.github.io/k8s/helm/charts/`

### Resource topology

```
namespace: spark-advisor
├── Deployment: gateway              (replicas configurable, port 8080)
├── Deployment: analyzer             (replicas configurable)
├── Deployment: hs-connector         (1 replica recommended)
├── Deployment: storage-connector    (1 replica per connector type, optional)
├── Deployment: frontend             (nginx, port 80, reverse proxy /api/ → gateway)
├── Service: gateway                 (ClusterIP → port 8080)
├── Service: frontend                (ClusterIP → port 80)
├── Ingress: gateway                 (disabled by default)
├── ConfigMap: gateway               (SA_GATEWAY_* env vars)
├── ConfigMap: analyzer              (SA_ANALYZER_* env vars)
├── ConfigMap: hs-connector          (SA_HS_CONNECTOR_* env vars)
├── ConfigMap: storage-connector     (SA_STORAGE_* env vars)
├── PVC: storage-connector-data      (SQLite polling state, named per connector type)
├── Secret: (user-managed)           (ANTHROPIC_API_KEY)
└── NATS StatefulSet                 (from NATS Helm chart dependency)
```

### ConfigMap → pydantic-settings mapping

Each ConfigMap maps `values.yaml` entries to environment variables matching the service's `env_prefix` and `env_nested_delimiter="__"`:

| Service           | ConfigMap env vars                                                              | Python config class          |
|-------------------|---------------------------------------------------------------------------------|------------------------------|
| Analyzer          | `SA_ANALYZER_NATS__URL`, `SA_ANALYZER_AI__ENABLED`, `SA_ANALYZER_THRESHOLDS__*` | `AnalyzerSettings`           |
| Gateway           | `SA_GATEWAY_SERVER__PORT`, `SA_GATEWAY_NATS__ANALYZE_TIMEOUT`, ...              | `GatewaySettings`            |
| HS-Connector      | `SA_HS_CONNECTOR_HISTORY_SERVER_URL`, `SA_HS_CONNECTOR_POLL_INTERVAL_SECONDS`, ... | `ConnectorSettings`       |
| Storage-Connector | `SA_STORAGE_CONNECTOR_TYPE`, `SA_STORAGE_HDFS__NAMENODE_URL`, ...               | `StorageConnectorSettings`   |

All defaults live in `values.yaml` (single source of truth). Deployments use `envFrom: configMapRef` — no hardcoded values in templates. A config checksum annotation triggers pod restarts on ConfigMap changes.

### NATS URL auto-resolution

When deployed via the umbrella chart, the umbrella `values.yaml` sets `config.nats.url` for all subcharts (e.g. `nats://spark-advisor-nats:4222`). ConfigMap templates use values directly — no `| default` fallbacks.

### Secrets

`ANTHROPIC_API_KEY` is mounted from a user-managed Kubernetes Secret (not stored in ConfigMap):

```bash
kubectl create secret generic anthropic-api-key --from-literal=api-key=sk-ant-...
helm install spark-advisor charts/spark-advisor \
  --set analyzer.anthropicApiKey.existingSecret=anthropic-api-key
```

### Design decisions

- **Analyzer and hs-connector have no Service** — they are NATS workers with no HTTP port
- **Gateway is the only service with Ingress** — it's the REST API entry point
- **HS-Connector should run as 1 replica** — polling deduplication via `PollingState`
- **NATS is a chart dependency** — deployed in the same release, not a separate namespace
- **`imagePullSecrets` supported** — for private container registries

> Diagram: [06-k8s-deployment.mmd](diagrams/06-k8s-deployment.mmd)

---

## CI/CD

### CI Pipeline (`.github/workflows/ci.yml`)

Runs on every push to `main` and on PRs. Three parallel jobs:

| Job           | What it does                                                                         |
|---------------|--------------------------------------------------------------------------------------|
| **check**     | Python matrix (3.12, 3.13): `uv sync` → `make lint` (ruff + mypy) → `make test`      |
| **frontend**  | npm ci → type-check → lint → build                                                   |
| **helm-lint** | `helm lint` on all subcharts (analyzer, gateway, hs-connector, storage-connector, frontend, prometheus, grafana, tempo) + umbrella chart, `helm template` to verify rendering |

### Release Pipeline (`.github/workflows/release.yml`)

Runs on push to `main`. Three jobs:

| Job                | Trigger            | What it does                                                                      |
|--------------------|--------------------|-----------------------------------------------------------------------------------|
| **release-please** | Always             | Creates release PRs, manages version bumps across 8 packages + 4 Chart.yaml files |
| **publish**        | On release created | `make check` → `uv build --package spark-advisor-cli` → `uv publish` to PyPI      |
| **docker-publish** | On release created | Matrix builds 4 Docker images → pushes to `ghcr.io/pstysz/spark-advisor-*`        |

### Docker images

Published to GitHub Container Registry on each release:

| Image                                       | Dockerfile                                       |
|---------------------------------------------|--------------------------------------------------|
| `ghcr.io/pstysz/spark-advisor-analyzer`     | `packages/spark-advisor-analyzer/Dockerfile`     |
| `ghcr.io/pstysz/spark-advisor-gateway`      | `packages/spark-advisor-gateway/Dockerfile`      |
| `ghcr.io/pstysz/spark-advisor-hs-connector` | `packages/spark-advisor-hs-connector/Dockerfile` |
| `ghcr.io/pstysz/spark-advisor-frontend`     | `packages/frontend/Dockerfile`                   |

Tags: `<version>` (e.g. `0.1.3`) + `latest`. Build context is the monorepo root (Dockerfiles use `COPY packages/...`). BuildX with GitHub Actions cache for fast builds.

### Version management

release-please bumps all versions in a single PR via `# x-release-please-version` markers:
- 8 `pyproject.toml` files (7 Python packages + frontend)
- 4 `Chart.yaml` files (`version` + `appVersion` + dependency versions in umbrella)

---

## Known Pitfalls

| Pitfall                                               | Mitigation                                                                                                                                                                        |
|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NATS message size limit** (1MB default)             | `JobAnalysis` for a job with 100+ stages could approach this. Monitor payload sizes. NATS supports configuring `max_payload`.                                                     |
| **SQLite file locking under high concurrency**        | WAL mode enabled for better concurrent read/write. Single-writer limitation acceptable for current throughput.                                                                     |
| **PollingState lost on hs-connector restart**         | Connector re-fetches recent jobs. Analyzer handles duplicates idempotently (same input → same output).                                                                            |
| **Synchronous Claude API in async worker**            | Wrapped in `asyncio.to_thread()`. Under high load, consider thread pool sizing or async anthropic client.                                                                         |
| **No message persistence**                            | NATS core (not JetStream) — messages are fire-and-forget. If analyzer is down when hs-connector publishes, message is lost. Acceptable: next poll cycle will re-discover the job. |
| **executor CPU data unavailable from History Server** | `ExecutorIdleRule` skips analysis when `total_cpu_time_ms` is `None`. Only event log source provides CPU metrics.                                                                 |

---

## What is NOT in the Architecture

These concepts existed in the previous Kafka-based architecture and have been intentionally removed:

| Removed concept                           | Why                                                                                    |
|-------------------------------------------|----------------------------------------------------------------------------------------|
| **Apache Kafka**                          | Replaced by NATS (15MB vs 500MB, no JVM/ZooKeeper, simpler ops)                        |
| **confluent-kafka**                       | Replaced by FastStream[nats] + nats-py                                                 |
| **KafkaEnvelope / MessageMetadata**       | FastStream handles Pydantic serde directly — no wrapper needed                         |
| **Source / Sink ABCs**                    | Kafka-specific abstractions. FastStream decorators replace them                        |
| **Dead Letter Queue (DLQ)**               | Graceful degradation instead — rules-only fallback on AI failure                       |
| **Circuit Breaker**                       | Not needed — NATS request timeouts + graceful degradation suffice                      |
| **Rate Limiter**                          | Not needed — Claude API has its own rate limiting; system throughput is bounded by API |
| **Jaeger**                                | Replaced by Grafana Tempo for trace storage                                            |
| **k8s-source** (Kubernetes event watcher) | Future extension — not part of current architecture                                    |
| **background_worker threading utility**   | Replaced by `asyncio` (FastStream is async-native)                                     |

---

## Future Extensions

| Extension            | Description                                                                              | Impact                                            |
|----------------------|------------------------------------------------------------------------------------------|---------------------------------------------------|
| **k8s-source**       | Watch Kubernetes events for completed Spark jobs, publish to NATS                        | New service, depends only on models               |
| **Additional rules** | New domain-specific rules as real-world use cases are discovered                         | rules package only                                |
| **NATS JetStream**   | Persistent messaging with at-least-once delivery for batch flow                          | Replace NATS core subjects with JetStream streams |

---

## Diagrams Index

| #  | File                                                                    | Type            | Description                                       |
|----|-------------------------------------------------------------------------|-----------------|---------------------------------------------------|
| 01 | [01-system-overview.mmd](diagrams/01-system-overview.mmd)               | graph TB        | Full system: services, NATS, CLI, external        |
| 02 | [02-package-dependencies.mmd](diagrams/02-package-dependencies.mmd)     | graph BT        | 8-package dependency graph                        |
| 03 | [03-models-package.mmd](diagrams/03-models-package.mmd)                 | graph TB        | Contents of models package                        |
| 04 | [04-analyzer-pipeline.mmd](diagrams/04-analyzer-pipeline.mmd)           | flowchart TB    | Analyzer processing: NATS → rules → AI → reply    |
| 05 | [05-nats-subjects.mmd](diagrams/05-nats-subjects.mmd)                   | graph LR        | NATS subjects, publishers, subscribers            |
| 06 | [06-k8s-deployment.mmd](diagrams/06-k8s-deployment.mmd)                 | graph TB        | Kubernetes resources and topology                 |
| 07 | [07-on-demand-flow.mmd](diagrams/07-on-demand-flow.mmd)                 | sequenceDiagram | On-demand: User → Gateway → HS → Analyzer → User  |
| 08 | [08-rules-hierarchy.mmd](diagrams/08-rules-hierarchy.mmd)               | classDiagram    | Rule ABC, 11 rules, StaticAnalysisService         |
| 09 | [09-batch-flow.mmd](diagrams/09-batch-flow.mmd)                         | sequenceDiagram | Batch: asyncio loop → HS → Analyzer → Gateway     |
| 10 | [10-gateway-task-lifecycle.mmd](diagrams/10-gateway-task-lifecycle.mmd) | stateDiagram-v2 | Task states: PENDING → RUNNING → COMPLETED/FAILED |
