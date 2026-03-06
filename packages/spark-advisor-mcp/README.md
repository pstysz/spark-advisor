# spark-advisor-mcp

MCP server exposing spark-advisor tools for Claude Desktop, Cursor, and other MCP clients. Part of the [spark-advisor](https://github.com/pstysz/spark-advisor) ecosystem.

## Install

```bash
pip install spark-advisor-mcp
```

## What it does

Provides 5 MCP tools via FastMCP (stdio transport) for AI-assisted Spark job analysis:

| Tool | Description |
|------|-------------|
| `analyze_spark_job` | Full analysis — rules engine + optional AI recommendations |
| `scan_recent_jobs` | List recent applications from History Server |
| `get_job_config` | Retrieve Spark configuration for a specific job |
| `suggest_config` | Generate optimized spark-defaults.conf |
| `explain_metric` | Explain a specific Spark metric or configuration parameter |

## Setup with Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "spark-advisor": {
      "command": "spark-advisor-mcp",
      "env": {
        "ANTHROPIC_API_KEY": "sk-ant-..."
      }
    }
  }
}
```

See [full MCP setup guide](https://github.com/pstysz/spark-advisor/blob/main/docs/mcp-setup.md) for Cursor and Claude Code integration.

## Links

- [Main project](https://github.com/pstysz/spark-advisor)
- [MCP setup guide](https://github.com/pstysz/spark-advisor/blob/main/docs/mcp-setup.md)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
