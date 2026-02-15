# 🔥 spark-advisor

AI-powered Apache Spark job analyzer and configuration advisor.

**Stop guessing Spark configs. Let data and AI tell you what's wrong.**

```
$ spark-advisor analyze application_1234567890_0001 -s http://yarn:18080

╭──────────────────── Spark Job Analysis ────────────────────╮
│   App ID     application_1234567890_0001                   │
│   Duration   6.7 min                                       │
│   Stages     5                                             │
│   Tasks      12,400                                        │
╰────────────────────────────────────────────────────────────╯

  🔴 CRITICAL: Data skew in Stage 4
     Max task duration (340s) is 27.2x the median (12.5s)
     → Enable AQE: spark.sql.adaptive.enabled=true

  🔴 CRITICAL: Disk spill in Stage 4
     22.2 GB spilled to disk
     → Increase spark.executor.memory or shuffle partitions

  🤖 AI Analysis:
     Skew in Stage 4 causes cascading spill and GC pressure.
     Fixing skew alone should resolve all three issues.

     #1: spark.sql.shuffle.partitions  200 → 800
     #2: spark.sql.adaptive.enabled    false → true
     #3: spark.executor.memory         4g → 8g
```

## How it works

```
Event Log / History Server  →  Parser  →  Rules Engine  →  AI Advisor  →  Report
                                            (free, fast)    (Claude API)
```

1. **Parser** — extracts metrics from Spark event logs or History Server REST API
2. **Rules Engine** — deterministic checks (data skew, disk spill, GC pressure, partition sizing)
3. **AI Advisor** — Claude analyzes findings, identifies causal chains, suggests concrete config values

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (modern Python package manager)

### Install

```bash
git clone https://github.com/YOUR_USERNAME/spark-advisor.git
cd spark-advisor
uv sync
uv run spark-advisor --help
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

```bash
export ANTHROPIC_API_KEY=sk-ant-...
```

Cost: ~$0.02 per analysis (Claude Sonnet).

## What it detects

| Issue | Detection | Severity |
|-------|-----------|----------|
| Data skew | max/median task duration > 5x | 🔴 Critical |
| Disk spill | diskBytesSpilled > 0 | 🔴 Critical |
| GC pressure | GC time > 20% of task time | 🟡 Warning |
| Wrong partition count | partition size far from 128MB target | 🟡 Warning |
| Over-provisioned executors | CPU utilization < 40% | 🟡 Warning |

## Development

```bash
make dev       # Install dev dependencies
make test      # Run tests with coverage
make lint      # Ruff + mypy
make format    # Auto-format code
make check     # All checks (CI-ready)
make demo      # Run against sample event log
```

### Tech Stack

| Tool | Role |
|------|------|
| **uv** | Package manager (fast, modern) |
| **Typer** | CLI framework (type-hint based) |
| **Rich** | Terminal output (tables, colors) |
| **Pydantic** | Data models (validation, type safety) |
| **httpx** | HTTP client (async-capable) |
| **orjson** | JSON parsing (10x faster than stdlib) |
| **anthropic** | Claude SDK (LLM integration) |
| **Ruff** | Linter + formatter (replaces flake8, black, isort) |
| **mypy** | Static type checker (strict mode) |
| **pytest** | Testing (with coverage) |

### Project Structure

```
src/spark_advisor/
├── cli.py                  # CLI commands (Typer)
├── models.py               # Domain models (Pydantic)
├── sources/
│   ├── history_server.py   # History Server REST API client
│   └── event_log.py        # Streaming event log parser
├── analysis/
│   └── rules.py            # Deterministic rules engine
├── ai/
│   └── advisor.py          # Claude API integration
└── output/
    └── console.py          # Rich terminal formatting
```

## Roadmap

- [x] Event log parser (streaming, .json.gz)
- [x] History Server REST API client
- [x] Rules engine (5 rules)
- [x] AI advisor (Claude API)
- [x] CLI with Rich output
- [ ] Agent mode — LLM-driven iterative analysis
- [ ] MCP server — integrate with Claude Desktop / Cursor
- [ ] Databricks / EMR log support
- [ ] Batch analysis with cost estimation

## License

Apache 2.0
