# spark-advisor

AI-powered Apache Spark job analyzer and configuration advisor.

**Stop guessing Spark configs. Let data and AI tell you what's wrong.**

```
$ spark-advisor analyze application_1763665725860_53215 -s http://localhost:18080

╭──────────────────────── Spark Job Analysis ────────────────────────╮
│   App ID              application_1763665725860_53215              │
│   App Name            Spark Pi                                    │
│   Duration            0.6 min (37s)                               │
│   Stages              1                                           │
│   Total Tasks         5000                                        │
│   Shuffle Partitions  200                                         │
│   Executors           2                                           │
╰───────────────────────────────────────────────────────────────────╯

Issues Found

  🟡 WARNING: Data skew in Stage 0
    Max task duration (169ms) is 6.3x the median (27ms)
    → Enable AQE: spark.sql.adaptive.enabled=true

╭──────────────────────── AI Analysis ──────────────────────────────╮
│ This is a SparkPi computation job with 5000 tasks running on     │
│ severely under-provisioned executors (512MB with 107% memory     │
│ utilization). The primary issue is memory pressure causing        │
│ overhead.                                                         │
╰───────────────────────────────────────────────────────────────────╯

Recommendations
┏━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ # ┃ Recommendation                          ┃ Change              ┃
┡━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ 1 │ Increase executor memory                │ 512m → 2g           │
│ 2 │ Reduce task count                       │ 5000 → 800-1000     │
│ 3 │ Enable Adaptive Query Execution         │ false → true        │
└───┴─────────────────────────────────────────┴─────────────────────┘

Suggested spark-defaults.conf:
  spark.executor.memory = 2g
  spark.sql.adaptive.enabled = true
```

## How it works

```
Event Log / History Server  →  Parser  →  Rules Engine  →  AI Advisor  →  Report
                                            (free, fast)    (Claude API)
```

1. **Parser** — extracts metrics from Spark event logs or History Server REST API
2. **Rules Engine** — 6 deterministic checks: data skew, disk spill, GC pressure, partition sizing, executor idle, task failures
3. **AI Advisor** — Claude analyzes metrics + rule findings, identifies causal chains, suggests concrete config values with impact estimates

The rules engine runs for free and catches known patterns. The AI layer (optional, ~$0.02/analysis) adds contextual reasoning — it understands that skew in Stage 3 *causes* spill in Stage 4, which *causes* GC pressure, and recommends fixing the root cause instead of treating symptoms.

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (modern Python package manager)

### Install

```bash
git clone https://github.com/YOUR_USERNAME/spark-advisor.git
cd spark-advisor
uv sync
```

### Usage

```bash
# Analyze via History Server (recommended)
spark-advisor analyze <app-id> --history-server http://yarn-master:18080

# Analyze from event log file
spark-advisor analyze /path/to/event-log.json.gz

# Scan recent jobs for issues
spark-advisor scan --history-server http://yarn-master:18080 --limit 20

# Rules-only mode (no API key needed)
spark-advisor analyze <app-id> -s http://yarn:18080 --no-ai

# Save suggested config to file
spark-advisor analyze <app-id> -s http://yarn:18080 -o spark-defaults.conf
```

### AI Setup

The AI advisor requires an [Anthropic API key](https://console.anthropic.com/). Set it via environment variable:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

Or use [direnv](https://direnv.net/) for automatic loading:

```bash
echo 'export ANTHROPIC_API_KEY=sk-ant-...' > .envrc
direnv allow
```

Cost: ~$0.02 per analysis (Claude Sonnet).

## What it detects

| Issue | Detection | Severity |
|-------|-----------|----------|
| Data skew | max/median task duration > 5x | CRITICAL if >10x, WARNING if >5x |
| Disk spill | diskBytesSpilled > 0 | CRITICAL if >1GB, WARNING if >0.1GB |
| GC pressure | GC time > 20% of task time | CRITICAL if >40%, WARNING if >20% |
| Wrong partition count | partition size far from 128MB target | WARNING |
| Over-provisioned executors | CPU utilization < 40% | WARNING |
| Task failures | failed tasks > 0 | WARNING |

All thresholds are configurable via `Thresholds` model.

## Architecture

```
src/spark_advisor/
├── cli.py                         # Typer CLI: analyze, scan, version
├── spark_advisor.py               # Orchestrator: combines static + AI analysis
├── config.py                      # Thresholds and defaults
├── model/
│   ├── input.py                   # Pydantic models for Claude tool schema
│   ├── metrics.py                 # TaskMetrics, StageMetrics, ExecutorMetrics, JobAnalysis
│   ├── output.py                  # Severity, RuleResult, Recommendation, AdvisorReport
│   └── spark_config.py            # SparkConfig wrapper with typed accessors
├── analysis/
│   ├── rules.py                   # Rule ABC + 6 concrete rules
│   └── static_analysis_service.py # Runs rules, returns sorted results
├── ai/
│   ├── config.py                  # Tool definition (schema from Pydantic), system prompt
│   ├── llm_analysis_service.py    # Claude API call + response validation
│   └── prompts_builder.py         # Builds structured prompts from job data
├── api/
│   ├── anthropic_client.py        # Anthropic SDK wrapper
│   └── history_server_client.py   # Spark History Server REST client
└── util/
    ├── bytes_helper.py            # Human-readable byte formatting
    ├── console.py                 # Rich terminal output
    └── event_parser.py            # Streaming event log parser (.json, .json.gz)
```

### Key design decisions

- **Tool schema from Pydantic** — Claude's tool `input_schema` is generated via `AnalysisToolInput.model_json_schema()`. Schema and validation stay in sync — single source of truth, no hand-written JSON Schema dicts.
- **Streaming event log parser** — processes files line-by-line, never loads entire file into memory. Handles 100MB+ logs.
- **Rules engine is free** — deterministic checks run without API calls. AI is optional and additive.
- **Immutable models** — all Pydantic models use `frozen=True`. No mutable state after construction.
- **No LangChain** — direct use of `anthropic` SDK. Full control over prompts, tool definitions, and response handling.

## Development

```bash
make dev       # Install dev dependencies
make test      # Run 58 tests with coverage
make lint      # Ruff + mypy (strict)
make format    # Auto-format code
make check     # All checks (lint + test, CI-ready)
make demo      # Run against sample event log (no API key needed)
```

### Tech Stack

| Tool | Role |
|------|------|
| **Python 3.12+** | Language |
| **uv** | Package manager |
| **Typer** | CLI framework (type-hint based) |
| **Rich** | Terminal output (tables, colors, panels) |
| **Pydantic v2** | Data models, validation, JSON Schema generation |
| **httpx** | HTTP client for History Server REST API |
| **orjson** | Fast JSON parsing for event logs |
| **anthropic** | Claude SDK for AI analysis |
| **Ruff** | Linter + formatter (replaces flake8, black, isort) |
| **mypy** | Static type checker (strict mode) |
| **pytest** | Testing with coverage, fixtures, factories |

### Testing

58 tests covering rules engine, prompt building, LLM service (mocked), API client lifecycle, and event log parser. Test data is built with factory functions (`tests/factories.py`) and pytest fixtures (`tests/conftest.py`).

```bash
uv run pytest -v
```

## Roadmap

- [x] Streaming event log parser (.json, .json.gz)
- [x] History Server REST API client
- [x] Rules engine (6 rules)
- [x] AI advisor with Claude API (tool use + structured output)
- [x] Rich CLI output with suggested spark-defaults.conf
- [x] Pydantic-driven tool schema (single source of truth)
- [x] Tested against real Spark History Server
- [ ] Agent mode — LLM-driven iterative analysis with tool calling
- [ ] MCP server — integrate with Claude Desktop / Cursor
- [ ] GitHub Actions CI pipeline
- [ ] PyPI release (`pip install spark-advisor`)

## License

Apache 2.0
