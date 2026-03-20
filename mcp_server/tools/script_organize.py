"""MCP tools for organizing and moving scripts"""

import json
import os
import shutil
from typing import Any, Dict, List

from mcp_homelab.core import get_script_logger
from mcp_homelab.errors import error_response

from .util import append_audit_log, build_audit_entry, build_provenance, is_path_allowed

# Setup logger
logger = get_script_logger(__name__)


def _move_script(source_path: str, dest_path: str, allowed_roots: List[str], dry_run: bool = True) -> Dict[str, Any]:
    """Move a script file with validation."""
    if not is_path_allowed(source_path, allowed_roots):
        return {"ok": False, "error_code": "ALLOWLIST_VIOLATION", "message": f"Source path not allowed: {source_path}"}

    if not is_path_allowed(dest_path, allowed_roots):
        return {"ok": False, "error_code": "ALLOWLIST_VIOLATION", "message": f"Destination path not allowed: {dest_path}"}

    if not os.path.exists(source_path):
        return {"ok": False, "error_code": "PATH_MISSING", "message": f"Source file not found: {source_path}"}

    dest_dir = os.path.dirname(dest_path)

    if dry_run:
        return {
            "ok": True,
            "success": True,
            "dry_run": True,
            "message": f"Would move {source_path} -> {dest_path}",
            "dest_dir_exists": os.path.exists(dest_dir),
            "would_create_dir": not os.path.exists(dest_dir),
        }

    try:
        os.makedirs(dest_dir, exist_ok=True)
        shutil.move(source_path, dest_path)
        return {
            "ok": True,
            "success": True,
            "dry_run": False,
            "message": f"Moved {source_path} -> {dest_path}",
            "source": source_path,
            "destination": dest_path,
        }
    except Exception as e:
        return {"ok": False, "error_code": "UNKNOWN", "message": str(e)}


def scripts_move(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Move a script file to a new location.

    Safety: Level 2 (modifies filesystem)
    - Requires confirm=true to execute (dry_run=false is not sufficient)
    - dry_run=true by default to preview changes
    - Creates destination directories as needed
    - Logs all operations to audit trail
    """
    source = args.get("source", "")
    destination = args.get("destination", "")
    dry_run = bool(args.get("dry_run", True))
    confirm = bool(args.get("confirm", False))

    if not source or not destination:
        result = error_response(
            "scripts_move",
            "source and destination are required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing source or destination argument"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("scripts_move", args, [], json.dumps(result))
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not confirm:
        result = error_response(
            "scripts_move",
            "Move operation requires confirm=true for safety",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for file moves"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        result["dry_run"] = True
        result["planned_command"] = ["mv", source, destination]
        audit_entry = build_audit_entry("scripts_move", args, [], json.dumps(result))
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    source_path = os.path.abspath(source)
    dest_path = os.path.abspath(destination)

    if os.path.isdir(dest_path):
        dest_path = os.path.join(dest_path, os.path.basename(source_path))

    result = _move_script(source_path, dest_path, config.repo_roots, dry_run)
    if not result.get("ok"):
        error_code = result.get("error_code", "UNKNOWN")
        result = error_response(
            "scripts_move",
            result.get("message", "Move failed"),
            error_code=error_code,
            likely_causes=["Path validation failed", "Filesystem error"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {"path": os.path.dirname(source_path)}}],
            host=config.homelab_host,
        )
        result["dry_run"] = dry_run
        result["source"] = source_path
        result["destination"] = dest_path
    else:
        result["provenance"] = build_provenance(config.homelab_host, [])

    audit_entry = build_audit_entry("scripts_move", args, [], json.dumps(result), error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def scripts_bulk_organize(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Move multiple scripts in one operation with category-based organization.

    Safety: Level 2 (modifies filesystem)
    - Requires confirm=true to execute
    - dry_run=true by default
    - Creates new category folders as needed
    - Logs all operations
    """
    moves = args.get("moves", [])
    dry_run = bool(args.get("dry_run", True))
    confirm = bool(args.get("confirm", False))

    if not moves:
        result = error_response(
            "scripts_bulk_organize",
            "moves list is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing moves array"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("scripts_bulk_organize", args, [], json.dumps(result))
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not confirm:
        result = error_response(
            "scripts_bulk_organize",
            "Bulk move requires confirm=true for safety",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for bulk moves"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        result["dry_run"] = True
        result["total_moves"] = len(moves)
        audit_entry = build_audit_entry("scripts_bulk_organize", args, [], json.dumps(result))
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    results = []
    successful = 0
    failed = 0

    for move in moves:
        source = move.get("source")
        destination = move.get("destination")
        category = move.get("category", "uncategorized")

        if not source or not destination:
            results.append(
                {
                    "ok": False,
                    "success": False,
                    "error_code": "INVALID_ARGS",
                    "message": "Missing source or destination",
                    "move": move,
                    "category": category,
                }
            )
            failed += 1
            continue

        source_path = os.path.abspath(source)
        dest_path = os.path.abspath(destination)

        if os.path.isdir(dest_path):
            dest_path = os.path.join(dest_path, os.path.basename(source_path))

        move_result = _move_script(source_path, dest_path, config.repo_roots, dry_run)
        move_result["category"] = category
        move_result["source_file"] = os.path.basename(source_path)
        move_result["source"] = source_path
        move_result["destination"] = dest_path
        results.append(move_result)

        if move_result.get("ok"):
            successful += 1
        else:
            failed += 1

    summary = {
        "total": len(moves),
        "successful": successful,
        "failed": failed,
        "dry_run": dry_run,
    }

    result = {
        "ok": failed == 0,
        "success": failed == 0,
        "summary": summary,
        "results": results,
        "provenance": build_provenance(config.homelab_host, []),
    }

    audit_entry = build_audit_entry(
        "scripts_bulk_organize",
        args,
        [],
        json.dumps({"summary": summary, "results_preview": results[:5]}),
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


# MCP Tool Definitions
SCRIPT_ORGANIZE_TOOLS = [
    {
        "name": "scripts_move",
        "description": "Move a script file to a new location (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {
                    "type": "string",
                    "description": "Absolute path to the script file to move"
                },
                "destination": {
                    "type": "string",
                    "description": "Absolute path to destination (file or directory)"
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "If true (default), preview changes without executing",
                    "default": True
                },
                "confirm": {
                    "type": "boolean",
                    "description": "Must be true to actually move files (required for safety)",
                    "default": False
                }
            },
            "required": ["source", "destination"]
        },
        "handler": scripts_move
    },
    {
        "name": "scripts_bulk_organize",
        "description": "Move multiple scripts in one operation with category-based organization (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "moves": {
                    "type": "array",
                    "description": "List of move operations, each with source, destination, and optional category",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source": {"type": "string"},
                            "destination": {"type": "string"},
                            "category": {"type": "string"}
                        },
                        "required": ["source", "destination"]
                    }
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "If true (default), preview all changes without executing",
                    "default": True
                },
                "confirm": {
                    "type": "boolean",
                    "description": "Must be true to actually move files (required for safety)",
                    "default": False
                }
            },
            "required": ["moves"]
        },
        "handler": scripts_bulk_organize
    }
]
