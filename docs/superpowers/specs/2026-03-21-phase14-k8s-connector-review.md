# Phase 14: K8s Connector Design Spec -- Review

**Verdict: ISSUES FOUND** (0 critical, 4 important, 5 minor)

## What is done well

- Follows established connector patterns (app.py lifecycle, PollingStore, ContextKey enum, config extending BaseConnectorSettings)
- Reuses `StorageFetchRequest` to delegate to storage-connector -- zero changes needed downstream
- NATS subject `job.fetch.k8s` already exists in `defaults.py` -- forward planning paid off
- Decision log is thorough and well-reasoned
- URI scheme detection + fallback is a clean layered approach
- Helm RBAC scoping is correct for read-only CRD access

---

## Important issues (should fix before implementation)

### I1. On-demand flow returns `AnalysisResult` but storage-connector returns `JobAnalysis | ErrorResponse`

Spec says (line 116): "Returns `AnalysisResult` to gateway". But `handle_storage_fetch` in storage-connector returns `JobAnalysis | ErrorResponse`. The k8s-connector would need to either:
- Return `JobAnalysis | ErrorResponse` (matching storage-connector), or
- Run analysis itself (but the spec says it delegates to storage-connector)

The k8s-connector is a **discovery** layer, not an analyzer. The on-demand flow should: (a) call storage-connector to get `JobAnalysis`, (b) then call `analysis.run` to get `AnalysisResult`, or (c) return `JobAnalysis` and let gateway handle the rest. This needs clarification.

### I2. `K8sFetchRequest` identification is ambiguous for on-demand flow

`K8sFetchRequest` has `namespace`, `name`, and `app_id` -- all optional. The spec should define:
- Which combinations are valid (namespace+name OR app_id, not both/neither)
- How app_id lookup works -- K8s API does not support querying CRDs by Spark app_id natively; it requires listing + filtering, which could be expensive
- Add a Pydantic `model_validator` to enforce exactly one identification method

### I3. Gateway API reorganization is a breaking change with no migration plan

Moving `POST /api/v1/analyze` to `POST /api/v1/hs/analyze` breaks all existing clients (CLI, frontend, external integrations). The spec should either:
- Keep old endpoints as deprecated aliases (redirect or dual-registration)
- Define a versioning strategy (v2 prefix)
- At minimum, document the breaking change and migration steps

### I4. Aggregator `/api/v1/applications` lacks timeout handling for NATS request-reply

When one connector is down (e.g., K8s cluster unreachable), the aggregator should not block indefinitely. The spec should define:
- NATS request timeout per connector (e.g., 5s)
- Behavior on partial failure: return available results + error flag per source
- This is especially important because `enabled_connectors` can include both HS and K8s

---

## Minor issues (nice to have)

### M1. `PollingStore` key: `namespace/name + completedAt` may not be unique

If a SparkApplication is deleted and recreated with the same name in the same namespace, and happens to complete at the exact same second, it would be skipped. While unlikely, using the CRD `metadata.uid` would be more robust. However, this is an acceptable tradeoff for simplicity.

### M2. Missing `NATS_K8S_APPLICATIONS_LIST_SUBJECT` in current `defaults.py`

The spec says this subject is "new" but does not show adding it to `defaults.py`. The `NATS_FETCH_JOB_K8S_SUBJECT` already exists, but `k8s.applications.list` needs to be added. Minor -- just needs explicit mention in the implementation plan.

### M3. `storage_type` field on `SparkApplicationRef` should use `ConnectorType` enum

The spec defines `storage_type: str | None` but the project already has `ConnectorType(StrEnum)` in storage-connector config. Either reuse that enum (move to shared models) or define a similar one. Avoids stringly-typed dispatch.

### M4. Config `env_prefix` inconsistency

The spec uses `SA_K8S_CONNECTOR_` but existing patterns show:
- hs-connector: `SA_HS_CONNECTOR_`
- storage-connector: `SA_STORAGE_`

The storage-connector drops the `_CONNECTOR` suffix. Both patterns exist, so this is not wrong, but worth noting for consistency discussion.

### M5. No mention of `kubernetes-asyncio` client cleanup in shutdown

The `kubernetes-asyncio` `ApiClient` needs explicit `close()` on shutdown (it holds aiohttp sessions). The spec lists `K8sClient` as a component but the shutdown section in the spec only implicitly covers it. Should be explicit in the app.py lifecycle description, matching how hs-connector closes `HistoryServerClient` and storage-connector closes `StorageConnector`.

---

## Consistency verification against existing codebase

| Aspect | hs-connector | storage-connector | K8s spec | Match? |
|--------|-------------|-------------------|----------|--------|
| `BaseConnectorSettings` | Yes | Yes | Yes | OK |
| `ContextKey` enum | Yes | Yes | Yes (implied) | OK |
| PollingStore pattern | Yes | Yes | Yes | OK |
| `app.py` lifecycle (startup/shutdown/after_startup) | Yes | Yes | Yes (implied) | OK |
| NatsRouter + handlers | Yes | Yes | Yes | OK |
| structlog + configure_logging/tracing | Yes | Yes | Not explicit | Needs mention |
| ErrorResponse return type | Yes | Yes | Not defined | See I1 |
| `__all__` in `__init__.py` | Yes | Yes | Not mentioned | Needs mention |

---

## Summary

The design is solid and well-aligned with existing patterns. The four important issues should be addressed before implementation to avoid rework. I1 (return type mismatch) and I3 (breaking API change) are the most impactful.
