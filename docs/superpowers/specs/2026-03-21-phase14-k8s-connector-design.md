# Phase 14: K8s Connector — Design Spec

## Goal

Add a Kubernetes connector that discovers completed SparkApplication CRDs (spark-operator v1beta2), extracts event log URIs, and delegates to storage-connector for fetching and analysis. Includes gateway API reorganization and frontend updates.

## Architecture

```
Gateway (FastAPI) ──NATS──▶ K8s Connector (FastStream) ──NATS──▶ Storage Connector
    │                              │                                     │
    │ REST API                     │ K8s API                             │ HDFS/S3/GCS
    ▼                              ▼                                     ▼
 Frontend              SparkApplication CRDs                      Event Log Files
```

K8s connector does NOT parse event logs. It discovers where they are (from CRD `spec.sparkConf["spark.eventLog.dir"]`) and delegates to storage-connector via NATS `storage.fetch.{hdfs|s3|gcs}`.

## New Package: `spark-advisor-k8s-connector`

### Components

| Component | Responsibility |
|-----------|---------------|
| `K8sClient` | Wrapper on `kubernetes-asyncio`. List/get SparkApplication CRDs. Auto-detect auth (in-cluster / kubeconfig). |
| `K8sMapper` | CRD dict → `SparkApplicationRef`. URI scheme parsing → storage type. Fallback to config defaults. |
| `K8sPoller` | Background loop: list CRDs → filter by state/age/namespace → delegate to storage-connector → publish to `analysis.submit`. |
| `K8sPollingStore` | SQLite tracking. Key: `namespace/name + completedAt`. Prevents re-processing. |
| `handlers.py` | NATS handlers: `job.fetch.k8s` (on-demand), `k8s.applications.list`. |
| `config.py` | `K8sConnectorSettings` extending `BaseConnectorSettings`. |
| `app.py` | FastStream app lifecycle (startup/shutdown). |

### Dependencies

- `kubernetes-asyncio>=30.0`
- `spark-advisor-models`
- `spark-advisor-parser` (via storage-connector, not direct)

## Data Model: `SparkApplicationRef`

Location: `spark-advisor-models/model/k8s.py`

```python
class SparkApplicationRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    # metadata
    name: str
    namespace: str
    labels: dict[str, str] = {}
    creation_timestamp: datetime | None = None

    # spec
    app_type: str | None = None            # Scala/Java/Python/R
    main_application_file: str | None = None
    spark_version: str | None = None
    spark_conf: dict[str, str] = {}

    # spec.driver (requested resources)
    driver_cores: int | None = None
    driver_memory: str | None = None
    driver_memory_overhead: str | None = None

    # spec.executor (requested resources)
    executor_cores: int | None = None
    executor_memory: str | None = None
    executor_memory_overhead: str | None = None
    executor_instances: int | None = None

    # status
    app_id: str | None = None
    state: str | None = None
    error_message: str | None = None
    execution_attempts: int | None = None
    submitted_at: datetime | None = None
    completed_at: datetime | None = None

    # computed
    event_log_dir: str | None = None
    storage_type: str | None = None
```

## NATS Subjects and Request Models

### New subjects in `defaults.py`
```python
NATS_FETCH_JOB_K8S_SUBJECT = "job.fetch.k8s"                    # already exists
NATS_K8S_APPLICATIONS_LIST_SUBJECT = "k8s.applications.list"     # new
```

### New request models in `input.py`
```python
class K8sFetchRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    namespace: str | None = None
    name: str | None = None
    app_id: str | None = None

    @model_validator(mode="after")
    def validate_identifier(self) -> Self:
        has_k8s_id = self.namespace is not None and self.name is not None
        has_app_id = self.app_id is not None
        if not has_k8s_id and not has_app_id:
            raise ValueError("Either (namespace + name) or app_id must be provided")
        return self

class ListK8sAppsRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    limit: int = Field(default=20, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    namespace: str | None = None
    state: str | None = None
    search: str | None = None
```

### Message flow

**On-demand (job.fetch.k8s):**
1. Gateway sends `K8sFetchRequest` (namespace/name or app_id)
2. K8s connector queries K8s API for the CRD
3. Extracts event_log_dir + app_id
4. Builds `StorageFetchRequest(app_id, event_log_uri, spark_conf)`
5. Sends request-reply to `storage.fetch.{type}`
6. Returns `JobAnalysis` to gateway (gateway creates task from it)

**Polling:**
1. List CRDs across configured namespaces
2. Filter: state in configured states, age < max_age_days
3. Check PollingStore (skip if namespace/name+completedAt already processed)
4. For each new: delegate to storage-connector, publish result to `analysis.submit`
5. Mark as processed in PollingStore

**List (k8s.applications.list):**
1. Gateway sends `ListK8sAppsRequest`
2. K8s connector queries K8s API with filters
3. Maps to `list[SparkApplicationRef]`
4. Returns to gateway

## Configuration: `K8sConnectorSettings`

```python
class K8sConnectorSettings(BaseConnectorSettings):
    env_prefix = "SA_K8S_CONNECTOR_"

    namespaces: list[str] = ["default"]
    label_selector: str | None = None
    application_states: list[str] = ["COMPLETED", "FAILED"]
    max_age_days: int = 7
    kubeconfig_path: str | None = None    # None = auto-detect

    default_event_log_dir: str | None = None
    default_storage_type: str = "hdfs"
```

Environment variables:
```
SA_K8S_CONNECTOR_NAMESPACES='["spark-prod","spark-staging"]'
SA_K8S_CONNECTOR_LABEL_SELECTOR=team=data-platform
SA_K8S_CONNECTOR_APPLICATION_STATES='["COMPLETED","FAILED"]'
SA_K8S_CONNECTOR_MAX_AGE_DAYS=7
SA_K8S_CONNECTOR_DEFAULT_EVENT_LOG_DIR=s3a://bucket/spark-logs/
SA_K8S_CONNECTOR_DEFAULT_STORAGE_TYPE=s3
SA_K8S_CONNECTOR_KUBECONFIG_PATH=/etc/kubeconfig/config
SA_K8S_CONNECTOR_POLLING_ENABLED=true
SA_K8S_CONNECTOR_POLL_INTERVAL_SECONDS=60
```

## Event Log Discovery

1. Check `spec.sparkConf["spark.eventLog.dir"]` in CRD
2. If missing → use `default_event_log_dir` from settings
3. If still missing → skip with warning log

URI scheme → storage type mapping:
- `hdfs://...` → `storage.fetch.hdfs`
- `s3a://...` or `s3://...` → `storage.fetch.s3`
- `gs://...` → `storage.fetch.gcs`
- No scheme → use `default_storage_type` from settings

## K8s API Interaction

- Library: `kubernetes-asyncio>=30.0`
- Auth: auto-detect (in-cluster first, then kubeconfig). Optional `kubeconfig_path` override.
- CRD API: `CustomObjectsApi.list_namespaced_custom_object()` and `get_namespaced_custom_object()`
- Group: `sparkoperator.k8s.io`, version: `v1beta2`, plural: `sparkapplications`
- Filtering: by namespace (iterate list), label_selector (K8s API native), state + age (client-side)

## Gateway API Reorganization

### Route changes
```
OLD                          NEW
POST /api/v1/analyze     →   POST /api/v1/hs/analyze
GET  /api/v1/applications →  GET  /api/v1/hs/applications
(new)                        POST /api/v1/k8s/analyze
(new)                        GET  /api/v1/k8s/applications
(new)                        GET  /api/v1/applications  (aggregator)
```

### Unchanged routes (17)
- `GET /api/v1/tasks`, `/tasks/{id}`, `/tasks/{id}/rules`, `/tasks/{id}/config`, `/tasks/stats`
- `GET /api/v1/apps/{app_id}/history`
- `GET /api/v1/stats/*` (9 endpoints)
- `WS /api/v1/ws/tasks`
- `GET /health/*` (2 endpoints)

### Router split
- `hs_routes.py` — HS-specific (analyze, applications)
- `k8s_routes.py` — K8s-specific (analyze, applications)
- `common_routes.py` — tasks, stats, apps history, aggregator, WebSocket

### Gateway config addition
```python
enabled_connectors: list[str] = ["hs"]  # ["hs"], ["k8s"], or ["hs", "k8s"]
```

### Aggregator `/api/v1/applications`
- Queries enabled connectors via NATS (concurrent requests with `asyncio.gather`)
- Per-connector timeout (5s default, configurable). If one connector times out or errors → return partial results + warning header
- Maps HS `ApplicationSummary` and K8s `SparkApplicationRef` to unified `ApplicationResponse` with `source: DataSource` field
- Supports search, filtering, pagination
- Error handling: if all connectors fail → 502 Bad Gateway

## Frontend Changes

- Update all API URLs (`/api/v1/analyze` → `/api/v1/hs/analyze`)
- Analysis form: dropdown to select source (HS / K8s)
- Application list: unified list from `/api/v1/applications` with source badge
- Minimal changes — no new pages, just adapt existing ones

## Helm Subchart: `charts/k8s-connector/`

```
charts/k8s-connector/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── deployment.yaml
│   ├── pvc.yaml
│   ├── serviceaccount.yaml
│   ├── clusterrole.yaml
│   ├── clusterrolebinding.yaml
│   └── _helpers.tpl
```

### RBAC
```yaml
rules:
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications"]
    verbs: ["get", "list", "watch"]
```

### Key values
```yaml
rbac:
  create: true
serviceAccount:
  create: true
  name: ""

kubeconfig:
  inCluster: true
  secretName: ""
  secretKey: "config"

config:
  namespaces: ["default"]
  labelSelector: ""
  applicationStates: ["COMPLETED", "FAILED"]
  maxAgeDays: 7
  defaultEventLogDir: ""
  defaultStorageType: "hdfs"
  pollingEnabled: false
  pollIntervalSeconds: 60

persistence:
  enabled: true
  size: 1Gi
```

Umbrella chart: k8s-connector as optional dependency (`condition: k8s-connector.enabled`).

## Testing

### K8s connector (unit tests with AsyncMock)
- `test_client.py` — list/get CRD, auth auto-detect, error handling
- `test_mapper.py` — CRD → SparkApplicationRef, URI parsing, fallbacks, edge cases
- `test_poller.py` — poll cycle, filtering, delegation, PollingStore integration
- `test_handlers.py` — NATS handlers with TestBroker
- `test_store.py` — PollingStore CRUD
- `test_config.py` — settings parsing

### Test factories
- `make_crd()` — generates K8s API response dict
- `make_spark_application_ref()` — generates SparkApplicationRef with defaults

### Gateway tests (update)
- New URLs in existing tests
- Aggregator tests: both sources, HS only, K8s only
- `enabled_connectors` behavior

### Frontend tests (update)
- URL changes in API mocks

## CI/CD Changes

- `ci.yml` — add `spark-advisor-k8s-connector` to test matrix
- `release-please-config.json` — add new package
- `release.yml` — Docker build + push for k8s-connector
- `Makefile` — add `test-k8s-connector` target
- `charts/spark-advisor/Chart.yaml` — add k8s-connector dependency
- `docker-compose.yaml` — NO k8s-connector (run natively with `uv run`)

## Decisions Log

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Polling, not watch | Consistent with hs/storage connectors, simpler |
| 2 | On-demand: namespace/name OR app_id | Flexible identification |
| 3 | CRD sparkConf + configurable fallback | Works regardless of where eventLog.dir is set |
| 4 | Multi-namespace with list config | Flexible, explicit RBAC scope |
| 5 | COMPLETED + FAILED (configurable) | Failed jobs have valuable metrics too |
| 6 | Max age 7 days | Prevents analyzing entire history on first start |
| 7 | SparkApplicationRef in shared models | Gateway needs it for display |
| 8 | kubernetes-asyncio | Standard async client, handles auth/TLS |
| 9 | Auto-detect auth | In-cluster first, then kubeconfig |
| 10 | API reorganization: /api/v1/{source}/... | Clean separation, only 2 endpoints split |
| 11 | Gateway as aggregator | Unified list with search/filter/pagination |
| 12 | Enabled connectors in config | Explicit, no auto-discovery latency |
| 13 | Own SQLite, key: namespace/name+completedAt | Handles re-runs of same job name |
| 14 | URI scheme + fallback config → storage type | Automatic with override |
| 15 | Reuse StorageFetchRequest | No changes to storage-connector |
| 16 | No retry (poller retries next cycle) | Simple, consistent with other connectors |
| 17 | Separate ListK8sAppsRequest / ListHsAppsRequest | Different filtering needs per source |
| 18 | Helm RBAC configurable | Standard pattern, flexibility |
| 19 | No health HTTP endpoint | Consistent with other connectors |
| 20 | No docker-compose | Kubeconfig issues in container, run natively |
| 21 | Remote kubeconfig via Secret | Minikube → remote cluster scenario |
| 22 | Frontend: minimal changes | Dropdown + source badge, no new pages |
