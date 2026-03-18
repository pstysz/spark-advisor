[↩ spark-advisor](../../README.md)

# spark-advisor-cli

AI-powered Apache Spark job analyzer and configuration advisor. Standalone CLI tool.

**Stop guessing Spark configs. Let data and AI tell you what's wrong.**

## Install

```bash
pip install spark-advisor-cli
```

## Quick Start

```bash
# Analyze from event log file (rules-only, free)
spark-advisor analyze /path/to/event-log.json.gz --no-ai

# Analyze with AI recommendations
export ANTHROPIC_API_KEY=sk-ant-...
spark-advisor analyze /path/to/event-log.json.gz

# Analyze from History Server
spark-advisor analyze app-20250101120000-0001 -hs http://yarn:18080

# Agent mode (multi-turn AI analysis)
spark-advisor analyze /path/to/event-log.json.gz --agent
```

## Commands

### `analyze` — analyze a Spark job

```bash
# From event log file
spark-advisor analyze /path/to/event-log.json.gz

# From History Server
spark-advisor analyze app-20250101120000-0001 -hs http://yarn:18080

# With AI analysis (default if ANTHROPIC_API_KEY is set)
spark-advisor analyze /path/to/event-log.json.gz

# Without AI (rules only)
spark-advisor analyze /path/to/event-log.json.gz --no-ai

# Agent mode (multi-turn AI with tool use)
spark-advisor analyze /path/to/event-log.json.gz --agent

# Verbose mode (per-stage breakdown)
spark-advisor analyze /path/to/event-log.json.gz --verbose

# JSON output
spark-advisor analyze /path/to/event-log.json.gz --format json

# Save suggested config to file
spark-advisor analyze /path/to/event-log.json.gz -o spark-defaults.conf

# Use specific Claude model
spark-advisor analyze /path/to/event-log.json.gz --model claude-sonnet-4-6
```

| Flag               | Short | Default             | Description                                   |
|--------------------|-------|---------------------|-----------------------------------------------|
| `source`           |       | required            | App ID (with `-hs`) or path to event log file |
| `--history-server` | `-hs` | `None`              | Spark History Server URL                      |
| `--no-ai`          |       | `False`             | Disable AI analysis (rules only)              |
| `--agent`          |       | `False`             | Use agent mode (multi-turn AI with tool use)  |
| `--model`          | `-m`  | `claude-sonnet-4-6` | Claude model for AI analysis                  |
| `--output`         | `-o`  | `None`              | Write suggested config to file                |
| `--format`         | `-f`  | `text`              | Output format: `text` or `json`               |
| `--verbose`        | `-v`  | `False`             | Show per-stage breakdown                      |

### `scan` — list recent jobs from History Server

```bash
spark-advisor scan -hs http://yarn:18080 --limit 20
```

### `version`

```bash
spark-advisor version
# spark-advisor v0.1.10
```

## What it detects

11 deterministic rules: data skew, disk spill, GC pressure, shuffle partitions, executor idle, task failures, small files, broadcast join threshold, serializer choice, dynamic allocation, memory overhead.

All thresholds are configurable via `Thresholds` model.

## See also

- [Full documentation and architecture](../../README.md)
- [MCP Server setup (Claude Desktop / Cursor)](../../docs/mcp-setup.md)
- [Rules engine](../spark-advisor-rules/README.md)
- [Analyzer](../spark-advisor-analyzer/README.md)
- [Contributing](../../CONTRIBUTING.md)

## License

Apache 2.0
