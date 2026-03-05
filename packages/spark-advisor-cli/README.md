# spark-advisor

AI-powered Apache Spark job analyzer and configuration advisor.

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

# Scan recent jobs
spark-advisor scan -hs http://yarn:18080 --limit 20
```

## What it detects

11 deterministic rules: data skew, disk spill, GC pressure, shuffle partitions, executor idle, task failures, small files, broadcast join threshold, serializer choice, dynamic allocation, memory overhead.

## Links

- [Full documentation and architecture](https://github.com/pstysz/spark-advisor)
- [MCP Server setup (Claude Desktop / Cursor)](https://github.com/pstysz/spark-advisor/blob/main/docs/mcp-setup.md)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
