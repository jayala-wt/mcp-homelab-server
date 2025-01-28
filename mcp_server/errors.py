"""Structured error helpers for MCP tools."""

from __future__ import annotations

import socket
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


ERROR_CODES = {
    "SYSTEMD_BUS_DENIED",
    "PERMISSION_DENIED",
    "PATH_MISSING",
    "SERVICE_NOT_FOUND",
    "COMMAND_NOT_FOUND",
    "INVALID_ARGS",
    "ALLOWLIST_VIOLATION",
    "TIMEOUT",
    "GOOGLE_CALENDAR_NOT_CONFIGURED",
    "UNKNOWN",
}

# Context-aware suggestions for self-healing (high ROI for operator UX)
ERROR_SUGGESTIONS = {
    "PERMISSION_DENIED": [
        {"tool": "mcp_list_metadata", "args": {}, "reason": "Check safety_level and requires_confirmation"},
        {"tool": "meta.validate_config", "args": {}, "reason": "Verify tool configuration"},
    ],
    "INVALID_ARGS": [
        {"tool": "mcp_list_metadata", "args": {}, "reason": "Review required parameters"},
        {"tool": "meta.validate_config", "args": {}, "reason": "Check argument validation rules"},
    ],
    "PATH_MISSING": [
        {"tool": "meta.discover_services", "args": {"pattern": "*"}, "reason": "Find available paths/services"},
        {"tool": "docs_list", "args": {"path": "/opt/homelab-panel", "recursive": False}, "reason": "List directory structure"},
    ],
    "SERVICE_NOT_FOUND": [
        {"tool": "meta.discover_services", "args": {"pattern": "*"}, "reason": "List available services"},
        {"tool": "lab_status", "args": {}, "reason": "Check service status"},
    ],
    "SYSTEMD_BUS_DENIED": [
        {"tool": "meta.health", "args": {}, "reason": "Check environment permissions"},
        {"tool": "meta.server_info", "args": {}, "reason": "Verify server capabilities"},
    ],
    "COMMAND_NOT_FOUND": [
        {"tool": "meta.health", "args": {}, "reason": "Check available commands"},
        {"tool": "meta.validate_config", "args": {}, "reason": "Verify system dependencies"},
    ],
    "ALLOWLIST_VIOLATION": [
        {"tool": "meta.validate_config", "args": {}, "reason": "Review allowlist configuration"},
        {"tool": "git_list_repos", "args": {"root_path": "/opt/homelab-panel"}, "reason": "See allowed repositories"},
    ],
    "TIMEOUT": [
        {"tool": "meta.health", "args": {}, "reason": "Check system health"},
        {"tool": "lab_logs", "args": {"lines": 50}, "reason": "Review recent logs"},
    ],
    "GOOGLE_CALENDAR_NOT_CONFIGURED": [
        {"tool": "calendar.status", "args": {}, "reason": "Check credential configuration"},
        {"tool": "meta.validate_config", "args": {}, "reason": "Verify environment settings"},
    ],
    "UNKNOWN": [
        {"tool": "meta.health", "args": {}, "reason": "Diagnose system state"},
        {"tool": "lab_logs", "args": {"lines": 100}, "reason": "Check error logs"},
    ],
}


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def error_response(
    tool: str,
    message: str,
    error_code: str = "UNKNOWN",
    likely_causes: Optional[List[str]] = None,
    suggested_next_tools: Optional[List[Dict[str, Any]]] = None,
    host: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a structured error envelope for tool responses.
    
    Args:
        tool: Name of the tool that errored
        message: Human-readable error message
        error_code: Error code from ERROR_CODES
        likely_causes: Optional list of diagnostic hints (auto-generated if None)
        suggested_next_tools: Optional recovery tools (context-aware if None)
        host: Optional hostname override
    
    Returns:
        Structured error dict with self-healing breadcrumbs
    """
    code = error_code if error_code in ERROR_CODES else "UNKNOWN"
    
    # Use context-aware suggestions if not explicitly provided
    if suggested_next_tools is None:
        suggested_next_tools = ERROR_SUGGESTIONS.get(code, [
            {"tool": "meta.health", "args": {}}
        ])
    
    return {
        "ok": False,
        "error_code": code,
        "message": message,
        "likely_causes": likely_causes or [],
        "suggested_next_tools": suggested_next_tools,
        "provenance": {
            "host": host or socket.gethostname(),
            "timestamp": _utc_timestamp(),
            "tool": tool,
        },
    }


@dataclass(frozen=True)
class AuditLogError(Exception):
    """Raised when audit logging fails and must be surfaced to clients."""

    message: str

    def __str__(self) -> str:
        return self.message
