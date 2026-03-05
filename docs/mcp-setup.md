# MCP Server Setup

spark-advisor exposes 5 tools via the [Model Context Protocol](https://modelcontextprotocol.io/) (MCP), allowing Claude Desktop, Cursor, and other MCP clients to analyze Spark jobs directly.

## Tools

| Tool | Description |
|------|-------------|
| `analyze_spark_job` | Full analysis (rules engine + optional AI) from event log file or History Server |
| `scan_recent_jobs` | List recent Spark applications from History Server |
| `get_job_config` | Show all `spark.*` configuration properties of a job |
| `suggest_config` | Run rules engine and suggest concrete config changes |
| `explain_metric` | Explain a Spark metric (gc_time_percent, data_skew_ratio, etc.) |

## Installation

```bash
# From the workspace root
uv sync --all-packages

# Or install standalone
uv tool install spark-advisor-mcp
```

## Claude Desktop

Add to your `claude_desktop_config.json`:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "spark-advisor": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/spark-advisor/packages/spark-advisor-mcp", "python", "-m", "spark_advisor_mcp"],
      "env": {
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

If you don't set `ANTHROPIC_API_KEY`, the `analyze_spark_job` tool will run in rules-only mode (no AI recommendations).

## Cursor

Add to `.cursor/mcp.json` in your project root:

```json
{
  "mcpServers": {
    "spark-advisor": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/spark-advisor/packages/spark-advisor-mcp", "python", "-m", "spark_advisor_mcp"],
      "env": {
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

## Claude Code

Add to `.claude/settings.json` or `~/.claude.json`:

```json
{
  "mcpServers": {
    "spark-advisor": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/spark-advisor/packages/spark-advisor-mcp", "python", "-m", "spark_advisor_mcp"]
    }
  }
}
```

## Usage Examples

Once configured, you can ask your MCP client:

- *"Analyze the Spark job at /data/event-logs/app-20250101.json.gz"*
- *"What does gc_time_percent of 35% mean?"*
- *"Show me recent jobs from the History Server at http://yarn:18080"*
- *"What config changes would you suggest for app-20250101120000-0001?"*

## Verify Installation

Test that the server starts correctly:

```bash
echo '{"jsonrpc":"2.0","method":"tools/list","id":1}' | uv run python -m spark_advisor_mcp
```

You should see a JSON response listing the 5 available tools.
