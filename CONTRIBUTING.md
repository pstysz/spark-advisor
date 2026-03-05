# Contributing to spark-advisor

## Development Setup

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)

### Install

```bash
git clone https://github.com/YOUR_USERNAME/spark-advisor.git
cd spark-advisor
uv sync --all-packages
```

### Run checks

```bash
make check    # lint + mypy + tests (CI-ready)
make test     # tests only
make lint     # ruff + mypy
make format   # auto-format
```

## Project Structure

uv workspace monorepo with 7 packages:

| Package                      | Type    | Description                          |
|------------------------------|---------|--------------------------------------|
| `spark-advisor-models`       | Library | Shared Pydantic contracts and config |
| `spark-advisor-rules`        | Library | 11 deterministic rules engine        |
| `spark-advisor-analyzer`     | Service | NATS worker (rules + AI)             |
| `spark-advisor-hs-connector` | Service | History Server client + poller       |
| `spark-advisor-gateway`      | Service | REST API + NATS orchestration        |
| `spark-advisor-cli`          | App     | Typer CLI                            |
| `spark-advisor-mcp`          | App     | MCP server for Claude Desktop/Cursor |

## Code Conventions

- **Type hints** — always, mypy strict mode
- **Pydantic models** — `frozen=True` for immutability
- **Line length** — 120 characters max
- **Imports** — sorted by ruff (isort rules)
- **Tests** — pytest, fixtures in `conftest.py`, factories in `factories.py`
- **Comments** — minimal, only for non-obvious logic
- **No LangChain** — direct `anthropic` SDK usage

## Pull Request Process

1. Fork and create a feature branch from `main`
2. Make your changes
3. Run `make check` — all lint and tests must pass
4. Open a PR with a clear description of what and why

## Adding a New Rule

Use the `/new-rule` skill or follow the pattern in `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py`:

1. Create a class extending `Rule` ABC
2. Override `_check_stage()` (per-stage) or `evaluate()` (job-level)
3. Add threshold to `Thresholds` model in `packages/spark-advisor-models/src/spark_advisor_models/config.py`
4. Register in `default_rules()` factory
5. Add tests in `packages/spark-advisor-rules/tests/`

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
