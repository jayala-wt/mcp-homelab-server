# MCP Homelab Server

A production-grade [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server built for homelab and developer tooling use cases.

Implements the full MCP stdio transport with structured audit logging, safety-gated tool execution, `dry_run` support, and a `ToolContext` metadata system for categorizing and managing tool registrations at scale.

Originally built as the backend for a self-hosted operational intelligence platform serving multiple projects. This repo contains the core framework and a subset of generic tools.

---

## Architecture

```
__main__.py               Entry point (python -m mcp_server)
mcp_server/
  server.py               MCP stdio transport, tool routing, audit logging
  config.py               Env-var driven configuration (no hardcoded values)
  errors.py               Structured error envelopes with self-healing breadcrumbs
  tool_metadata.py        ToolContext dataclass + registry for 83+ tools
  version.py              Protocol and server version constants
  core/
    db_context.py         SQLite connection management
    script_logger.py      Structured logging helpers
    script_context.py     Runtime context for tool execution
    paths.py              Path resolution utilities
  tools/
    util.py               Shared helpers (redaction, sanitization)
    lab_tools.py          Service status, logs, restart (systemd/docker)
    git_tools.py          Git operations across multiple repos
    docs_tools.py         Filesystem and document browsing
    script_tools.py       Safe script execution with allowlist
    knowledge_tools.py    FTS5 knowledge base (index, search, tiered recall)
    devloop_tools.py      Dev session logging and artifact management
    mcp_meta_tools.py     Server introspection, health, config validation
```

---

## Key Design Decisions

### Safety gates on every destructive tool

All tools that modify state require `confirm=true, dry_run=false` at call time. The default for both is safe — `dry_run=True`, `confirm=False`. A tool will not execute destructively unless both are explicitly set by the caller.

```python
# Tool will simulate, not execute
result = tool_call("lab_restart", {"dry_run": True, "confirm": False})

# Tool executes for real
result = tool_call("lab_restart", {"dry_run": False, "confirm": True})
```

### ToolContext metadata system

Every tool is registered with a `ToolContext` dataclass describing its category, safety level, databases used, external services, expected duration, and suggested server assignment (for future splitting into specialized MCP servers):

```python
@dataclass
class ToolContext:
    category: str                    # lab, git, docs, knowledge, devloop, ...
    safety_level: str                # safe | modify | destructive
    requires_confirmation: bool      # auto-set True for modify/destructive
    supports_dry_run: bool
    databases_used: List[str]
    external_services: List[str]
    suggested_server: Optional[str]  # for future multi-server splits
    tags: List[str]
```

### Structured audit log

Every tool call is written to an append-only JSONL audit log with input args, output summary, duration, and caller identity:

```json
{
  "id": "a3f91c...",
  "tool": "lab_restart",
  "args": {"dry_run": false, "confirm": true},
  "status": "success",
  "duration_ms": 312,
  "timestamp": "2025-01-15T22:04:11Z"
}
```

### Structured error envelopes

Errors return self-healing breadcrumbs — suggested next tool calls the caller can use to diagnose or recover:

```json
{
  "ok": false,
  "error_code": "SERVICE_NOT_FOUND",
  "message": "Service 'my-app' not found",
  "suggested_next_tools": [
    {"tool": "meta.discover_services", "args": {"pattern": "*"}},
    {"tool": "lab_status", "args": {}}
  ]
}
```

---

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your repo roots, service names, DB paths

# Run via stdio (for Claude Desktop / VS Code MCP)
python -m mcp_server
```

### VS Code / Claude Desktop config

```json
{
  "mcpServers": {
    "homelab": {
      "command": "python",
      "args": ["-m", "mcp_server"],
      "cwd": "/path/to/mcp-homelab-server",
      "env": {
        "MCP_REPO_ROOTS": "/path/to/your/repo",
        "WANATUX_SYSTEMD_SERVICE": "your-service"
      }
    }
  }
}
```

---

## Included Tools

| Category | Tools | Description |
|----------|-------|-------------|
| `lab` | `lab_status`, `lab_logs`, `lab_restart` | Service monitoring and control |
| `git` | `git_status`, `git_diff`, `git_log`, `git_list_repos`, + more | Git operations across allowed repos |
| `docs` | `docs_list`, `docs_read`, `docs_search` | Filesystem and document browsing |
| `scripts` | `script_run`, `script_list` | Safe script execution with allowlist |
| `knowledge` | `knowledge_search`, `knowledge_bootstrap_context`, `knowledge_status`, + more | FTS5 knowledge base with Ebbinghaus decay tiering |
| `devloop` | `devloop_log`, `devloop_search`, `devloop_latest`, `devloop_run_start` | Dev session logging across AI models |
| `meta` | `meta_health`, `meta_server_info`, `meta_validate_config`, `mcp_list_metadata` | Server introspection |

---

## Extending

Add a new tool category:

1. Create `mcp_server/tools/my_tools.py` with a `MY_TOOLS` list of tool dicts
2. Add `ToolContext` entries to `tool_metadata.py`
3. Import and register in `server.py`:
   ```python
   from .tools.my_tools import MY_TOOLS
   # ...
   self._register_tools([... MY_TOOLS])
   ```

---

## License

MIT
