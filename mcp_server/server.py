import json
import sqlite3
import sys
import time
import traceback
import uuid
from typing import Any, Dict, Optional

from mcp_homelab.core import get_script_logger, get_db_connection

from .config import load_config
from .errors import AuditLogError, error_response
from .tools.lab_tools import LAB_TOOLS
from .tools.mcp_meta_tools import MCP_META_TOOLS
from .tools.git_tools import GIT_TOOLS
from .tools.docs_tools import DOCS_TOOLS
from .tools.script_tools import SCRIPT_TOOLS
from .tools.knowledge_tools import KNOWLEDGE_TOOLS
from .tools.devloop_tools import DEVLOOP_TOOLS
from .tool_metadata import (
    TOOL_CONTEXTS,
    SERVER_CATEGORIES,
    get_tool_context,
    get_tool_summary,
    get_tool_version,
    validate_tool_contexts,
)
from .tools.util import redact_value
from .version import PROTOCOL_VERSION, SERVER_VERSION

# Setup logger for MCP server
logger = get_script_logger(__name__)

MCP_TOOL_CALL_ALLOWED_STATUSES = {"success", "error", "dry_run", "denied"}
MCP_TOOL_CALL_LEGACY_STATUSES = {"success", "error", "dry_run"}
MCP_TOOL_CALL_STATUS_ALIASES = {"permission_denied": "denied"}


class MCPServer:
    def __init__(self) -> None:
        self.config = load_config()
        self._tools = {}
        self._tool_list = []
        self._shutdown = False
        logger.info("Initializing MCP Homelab Server")
        self._register_tools(
            LAB_TOOLS + MCP_META_TOOLS + KNOWLEDGE_TOOLS +
            DEVLOOP_TOOLS + GIT_TOOLS + DOCS_TOOLS + SCRIPT_TOOLS
        )
        logger.info(f"Registered {len(self._tool_list)} MCP tools")
        
        # Validate tool contexts
        validation = validate_tool_contexts()
        if validation["valid"]:
            logger.info("✅ All tools have proper context metadata")
        else:
            logger.warning(f"⚠️  Tool context validation issues: {validation}")

    def _register_tools(self, tool_defs):
        for tool in tool_defs:
            tool_name = tool["name"]
            self._tools[tool_name] = tool["handler"]
            
            # Build tool definition with context metadata
            tool_def = {
                "name": tool_name,
                "description": tool.get("description", ""),
                "inputSchema": tool.get("inputSchema", {"type": "object"}),
            }
            
            # Add context metadata if available
            context = get_tool_context(tool_name)
            if context:
                tool_def["metadata"] = {
                    "category": context.category,
                    "subcategory": context.subcategory,
                    "safety_level": context.safety_level,
                    "requires_confirmation": context.requires_confirmation,
                    "supports_dry_run": context.supports_dry_run,
                    "suggested_server": context.suggested_server,
                    "tags": context.tags,
                    "tool_version": get_tool_version(tool_name),
                }
            
            self._tool_list.append(tool_def)
    
    def _log_tool_call(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        status: str,
        execution_time_ms: int,
        error_message: Optional[str] = None,
        reason_code: Optional[str] = None,
    ) -> None:
        """Log tool call telemetry to mcp_tool_calls (best-effort analytics)."""
        normalized_status = MCP_TOOL_CALL_STATUS_ALIASES.get(status, status)
        normalized_reason_code = reason_code
        if normalized_status == "denied" and not normalized_reason_code:
            normalized_reason_code = "CONFIRM_REQUIRED" if error_message == "confirm=true required" else "DENIED"

        if normalized_status not in MCP_TOOL_CALL_ALLOWED_STATUSES:
            logger.warning(
                "Unknown tool call status '%s' for %s; normalizing to 'error'",
                normalized_status,
                tool_name,
            )
            normalized_status = "error"
            normalized_reason_code = normalized_reason_code or "STATUS_NORMALIZED"

        legacy_status = normalized_status if normalized_status in MCP_TOOL_CALL_LEGACY_STATUSES else "error"
        args_json = json.dumps(arguments)

        try:
            with get_db_connection("knowledge") as conn:
                try:
                    conn.execute(
                        """INSERT INTO mcp_tool_calls
                           (tool_name, arguments_json, result_status, execution_time_ms, error_message, reason_code)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            tool_name,
                            args_json,
                            normalized_status,
                            execution_time_ms,
                            error_message,
                            normalized_reason_code,
                        ),
                    )
                except sqlite3.OperationalError as exc:
                    # Compatibility fallback for pre-migration schema (no reason_code column).
                    if "reason_code" not in str(exc):
                        raise
                    conn.execute(
                        """INSERT INTO mcp_tool_calls
                           (tool_name, arguments_json, result_status, execution_time_ms, error_message)
                           VALUES (?, ?, ?, ?, ?)""",
                        (tool_name, args_json, legacy_status, execution_time_ms, error_message),
                    )
                except sqlite3.IntegrityError as exc:
                    # Compatibility fallback for pre-migration CHECK constraints (no 'denied' status).
                    if "result_status" not in str(exc):
                        raise
                    conn.execute(
                        """INSERT INTO mcp_tool_calls
                           (tool_name, arguments_json, result_status, execution_time_ms, error_message)
                           VALUES (?, ?, ?, ?, ?)""",
                        (tool_name, args_json, legacy_status, execution_time_ms, error_message),
                    )
                conn.commit()
        except Exception as e:
            # Don't fail the tool call if logging fails
            logger.warning(f"Failed to log tool call for {tool_name}: {e}")

    def handle_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        method = request.get("method")
        request_id = request.get("id")

        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": PROTOCOL_VERSION,
                    "serverInfo": {
                        "name": "mcp-homelab",
                        "version": SERVER_VERSION,
                    },
                    "capabilities": {
                        "tools": {},
                    },
                },
            }

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": self._tool_list,
                },
            }

        if method == "tools/call":
            params = request.get("params", {}) or {}
            name = params.get("name")
            arguments = params.get("arguments", {}) or {}
            if name not in self._tools:
                logger.warning(f"Unknown tool requested: {name}")
                error = error_response(
                    name or "unknown",
                    f"Unknown tool: {name}",
                    error_code="INVALID_ARGS",
                    likely_causes=["Tool name not registered on this server"],
                    suggested_next_tools=[{"tool": "mcp_list_metadata", "args": {}}],
                )
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(error)}],
                    },
                }
            try:
                logger.info(f"Calling tool: {name}")
                logger.debug(f"Tool arguments: {redact_value(arguments)}")
                
                # Start timing
                start_time = time.perf_counter()
                
                tool_args = dict(arguments)
                if "run_id" not in tool_args and "_run_id" in tool_args:
                    tool_args["run_id"] = tool_args.get("_run_id")
                if "step_id" not in tool_args and "_step_id" in tool_args:
                    tool_args["step_id"] = tool_args.get("_step_id")
                tool_args.setdefault("run_id", f"run-{uuid.uuid4().hex}")
                tool_args.setdefault("step_id", f"step-{uuid.uuid4().hex}")

                context = get_tool_context(name)
                if context and context.supports_dry_run:
                    tool_args.setdefault("dry_run", True)
                if context and context.safety_level in {"modify", "destructive"}:
                    if tool_args.get("confirm") is not True:
                        dry_run_allowed = context.supports_dry_run and tool_args.get("dry_run") is True
                        if not dry_run_allowed:
                            # Log failed attempt
                            self._log_tool_call(
                                name,
                                tool_args,
                                "denied",
                                0,
                                "confirm=true required",
                                reason_code="CONFIRM_REQUIRED",
                            )
                            error = error_response(
                                name,
                                f"confirm=true required for {context.safety_level} tool execution. Add confirm=true to arguments or check tool metadata.",
                                error_code="PERMISSION_DENIED",
                                likely_causes=[
                                    f"Tool has safety_level={context.safety_level}",
                                    "Explicit confirmation required for destructive operations"
                                ],
                                # Don't pass suggested_next_tools - let error_response use context-aware defaults
                                host=self.config.wanatux_host,
                            )
                            return {
                                "jsonrpc": "2.0",
                                "id": request_id,
                                "result": {
                                    "content": [{"type": "text", "text": json.dumps(error)}],
                                },
                            }

                result = self._tools[name](self.config, tool_args)
                
                # Calculate execution time
                execution_time_ms = int((time.perf_counter() - start_time) * 1000)
                
                # Determine status and log
                if isinstance(result, dict):
                    status = "error" if result.get("ok") is False else "success"
                    error_msg = None if status == "success" else (result.get("message") or result.get("error"))
                    self._log_tool_call(name, tool_args, status, execution_time_ms, error_msg)
                    
                    if result.get("ok") is False:
                        if "error_code" not in result or "message" not in result:
                            message = result.get("message") or result.get("error") or "Tool failed"
                            normalized = error_response(
                                name,
                                message,
                                error_code=result.get("error_code", "UNKNOWN"),
                                host=self.config.wanatux_host,
                            )
                            for key, value in result.items():
                                if key not in normalized:
                                    normalized[key] = value
                            result = normalized
                    else:
                        result.setdefault("provenance", {"host": self.config.wanatux_host})
                else:
                    # Non-dict result (shouldn't happen but be safe)
                    self._log_tool_call(name, tool_args, "success", execution_time_ms, None)
                    
                logger.info(f"Tool {name} completed successfully")
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result),
                            }
                        ]
                    },
                }
            except AuditLogError as exc:
                # Log the error
                execution_time_ms = int((time.perf_counter() - start_time) * 1000)
                self._log_tool_call(name, tool_args, "error", execution_time_ms, str(exc))
                
                error = error_response(
                    name,
                    str(exc),
                    error_code="PERMISSION_DENIED",
                    likely_causes=["Audit log directory not writable"],
                    suggested_next_tools=[{"tool": "meta.health", "args": {}}, {"tool": "meta.validate_config", "args": {}}],
                    host=self.config.wanatux_host,
                )
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(error)}],
                    },
                }
            except Exception as exc:  # pragma: no cover - defensive
                error_text = f"Tool failure: {exc}"
                execution_time_ms = int((time.perf_counter() - start_time) * 1000)
                self._log_tool_call(name, tool_args, "error", execution_time_ms, error_text)
                
                logger.error(f"Tool {name} failed: {error_text}", exc_info=True)
                traceback.print_exc(file=sys.stderr)
                error = error_response(
                    name,
                    error_text,
                    error_code="UNKNOWN",
                    likely_causes=["Unexpected exception during tool execution"],
                    suggested_next_tools=[{"tool": "meta.health", "args": {}}],
                    host=self.config.wanatux_host,
                )
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(error)}],
                    },
                }

        if method in {"resources/list", "resources/read", "resources/templates/list"}:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": "Resources are not implemented on this MCP server. Use tools/list and tools/call.",
                },
            }

        if method == "shutdown":
            self._shutdown = True
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {},
            }

        if method == "exit":
            self._shutdown = True
            return None

        if request_id is None:
            return None

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": f"Unknown method: {method}",
            },
        }

    def serve_stdio(self) -> None:
        for line in sys.stdin:
            if self._shutdown:
                break
            line = line.strip()
            if not line:
                continue
            try:
                request = json.loads(line)
            except json.JSONDecodeError:
                continue
            response = self.handle_request(request)
            if response is None:
                continue
            sys.stdout.write(json.dumps(response))
            sys.stdout.write("\n")
            sys.stdout.flush()


def main() -> None:
    server = MCPServer()
    server.serve_stdio()


if __name__ == "__main__":
    main()
