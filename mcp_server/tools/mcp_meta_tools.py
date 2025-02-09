"""
MCP Tools for Tool Management and Onboarding

This module provides MCP tools for managing and creating new MCP tools.
"""

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from mcp_homelab.core import get_script_logger
from mcp_homelab.errors import error_response
from mcp_homelab.tool_metadata import (
    TOOL_CONTEXTS,
    SERVER_CATEGORIES,
    get_tool_summary,
    validate_tool_contexts,
)
from mcp_homelab.version import PROTOCOL_VERSION, SERVER_VERSION

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    run_command,
)

# Setup logger
logger = get_script_logger(__name__)


def _has_systemd_permission_error(command_result: Dict[str, Any]) -> bool:
    combined = f"{command_result.get('stdout', '')}\n{command_result.get('stderr', '')}"
    return (
        "Failed to connect to bus" in combined
        or "Operation not permitted" in combined
        or "Permission denied" in combined
    )


def list_tool_metadata(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    List all MCP tools with their metadata and context.
    
    Useful for understanding what tools are available, their categories,
    safety levels, and suggested server assignments.
    """
    category_filter = args.get("category")
    safety_filter = args.get("safety_level")
    server_filter = args.get("server")
    
    # Get full summary
    summary = get_tool_summary()
    validation = validate_tool_contexts()
    
    # Filter tools if requested
    tools_info = []
    for tool_name, context in TOOL_CONTEXTS.items():
        # Apply filters
        if category_filter and context.category != category_filter:
            continue
        if safety_filter and context.safety_level != safety_filter:
            continue
        if server_filter and context.suggested_server != server_filter:
            continue
        
        tools_info.append({
            "name": tool_name,
            "category": context.category,
            "subcategory": context.subcategory,
            "safety_level": context.safety_level,
            "requires_confirmation": context.requires_confirmation,
            "supports_dry_run": context.supports_dry_run,
            "databases_used": context.databases_used,
            "external_services": context.external_services,
            "suggested_server": context.suggested_server,
            "tags": context.tags,
            "notes": context.notes,
        })
    
    result = {
        "ok": True,
        "total_tools": len(tools_info),
        "tools": tools_info,
        "summary": summary,
        "validation": validation,
        "filters_applied": {
            "category": category_filter,
            "safety_level": safety_filter,
            "server": server_filter,
        },
        "provenance": build_provenance(config.wanatux_host, []),
    }
    
    audit_entry = build_audit_entry("mcp_list_metadata", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    
    logger.info(f"Listed {len(tools_info)} tools with metadata")
    return result


def show_server_plan(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Show the MCP server split plan with tool assignments.
    
    Displays which tools are assigned to which servers, helping plan
    the architecture for splitting into multiple specialized MCP servers.
    """
    servers = {}
    
    for server_name, info in SERVER_CATEGORIES.items():
        tools = info.get("tools", [])
        tool_details = []
        
        for tool_name in tools:
            if tool_name in TOOL_CONTEXTS:
                ctx = TOOL_CONTEXTS[tool_name]
                tool_details.append({
                    "name": tool_name,
                    "category": ctx.category,
                    "safety_level": ctx.safety_level,
                    "requires_confirmation": ctx.requires_confirmation,
                })
        
        servers[server_name] = {
            "description": info["description"],
            "categories": info["categories"],
            "tool_count": len(tools),
            "tools": tool_details,
            "status": "ready" if tools else "planned",
        }
    
    result = {
        "ok": True,
        "servers": servers,
        "total_servers": len(servers),
        "ready_servers": sum(1 for s in servers.values() if s["status"] == "ready"),
        "provenance": build_provenance(config.wanatux_host, []),
    }
    
    audit_entry = build_audit_entry("mcp_server_plan", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    
    logger.info(f"Retrieved server plan for {len(servers)} servers")
    return result


def generate_new_tool(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a new MCP tool with proper structure and context.
    
    Creates a new tool file with:
    - Proper imports and logger setup
    - Error handling and validation
    - Context metadata registration
    - Safety levels and dry-run support
    
    This is a dry-run only tool - it returns the generated code
    for review before creating files.
    """
    tool_name = args.get("name", "").strip()
    category = args.get("category", "").strip()
    description = args.get("description", "").strip()
    safety_level = args.get("safety_level", "safe")
    
    # Validation
    errors = []
    if not tool_name:
        errors.append("tool name is required")
    if not category:
        errors.append("category is required")
    if not description:
        errors.append("description is required")
    if safety_level not in ["safe", "modify", "destructive"]:
        errors.append(f"safety_level must be safe, modify, or destructive (got: {safety_level})")
    
    if errors:
        result = {
            "ok": False,
            "error": "; ".join(errors),
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("mcp_generate_tool", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Generate tool code
    requires_confirmation = safety_level in ["modify", "destructive"]
    supports_dry_run = safety_level in ["modify", "destructive"]
    
    tool_code = f'''"""
{description}
"""

from typing import Any, Dict
from mcp_homelab.core import get_script_logger

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
)

# Setup logger
logger = get_script_logger(__name__)


def {tool_name}_handler(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handler for {tool_name} tool.
    
    Args:
        config: MCP server configuration
        args: Tool arguments
    
    Returns:
        Result dictionary with ok/error and provenance
    """
    # Extract and validate arguments
    # TODO: Add your argument extraction here
    
    # Safety check
    {f'confirm = args.get("confirm", False)' if requires_confirmation else '# No confirmation needed (safe operation)'}
    {f'dry_run = args.get("dry_run", True)' if supports_dry_run else ''}
    
    {f'''if not confirm and not dry_run:
        result = {{
            "ok": False,
            "error": "This operation requires confirmation. Set confirm=true",
            "provenance": build_provenance(config.wanatux_host, []),
        }}
        audit_entry = build_audit_entry("{tool_name}", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result''' if requires_confirmation else ''}
    
    # TODO: Implement your tool logic here
    logger.info(f"Executing {tool_name}")
    
    result = {{
        "ok": True,
        "message": "Tool executed successfully",
        {f'"dry_run": dry_run,' if supports_dry_run else ''}
        "provenance": build_provenance(config.wanatux_host, []),
    }}
    
    audit_entry = build_audit_entry("{tool_name}", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    
    logger.info(f"{tool_name} completed successfully")
    return result


# Tool registration
{tool_name.upper()}_TOOL = [
    {{
        "name": "{tool_name}",
        "description": "{description}",
        "inputSchema": {{
            "type": "object",
            "properties": {{
                # TODO: Add your input schema here
                {f'"confirm": {{"type": "boolean", "default": False}},' if requires_confirmation else ''}
                {f'"dry_run": {{"type": "boolean", "default": True}},' if supports_dry_run else ''}
            }},
            "required": [],
        }},
        "handler": {tool_name}_handler,
    }}
]
'''

    # Generate metadata registration code
    metadata_code = f'''
# Add to mcp_homelab/tool_metadata.py TOOL_CONTEXTS dictionary:

"{tool_name}": ToolContext(
    category="{category}",
    subcategory="",  # TODO: Add subcategory
    safety_level="{safety_level}",
    requires_confirmation={requires_confirmation},
    supports_dry_run={supports_dry_run},
    expected_duration="fast",  # TODO: Adjust if needed
    suggested_server="{category}-server",
    tags=[],  # TODO: Add relevant tags
    notes="",  # TODO: Add notes if needed
),
'''
    
    result = {
        "ok": True,
        "tool_name": tool_name,
        "category": category,
        "safety_level": safety_level,
        "generated_tool_code": tool_code,
        "generated_metadata_code": metadata_code,
        "next_steps": [
            f"1. Create file: mcp_homelab/tools/{category}_tools.py (or add to existing)",
            "2. Add generated tool code to the file",
            "3. Add metadata entry to mcp_homelab/tool_metadata.py",
            "4. Import and register in mcp_homelab/server.py",
            "5. Test with: PYTHONPATH=/opt/homelab-panel python3 scripts/automation/inspect_mcp_metadata.py validate",
        ],
        "provenance": build_provenance(config.wanatux_host, []),
    }
    
    audit_entry = build_audit_entry("mcp_generate_tool", args, [], f"Generated {tool_name}")
    append_audit_log(config.audit_log_path, audit_entry)
    
    logger.info(f"Generated new tool template: {tool_name}")
    return result


def meta_server_info(config, args: Dict[str, Any]) -> Dict[str, Any]:
    summary = get_tool_summary()
    categories = sorted(summary.get("by_category", {}).keys())
    capabilities = {
        "structured_errors": True,
        "audit_logging": True,
        "output_truncation": True,
        "confirm_required": True,
        "dry_run_default": True,
    }
    result = {
        "ok": True,
        "server_version": SERVER_VERSION,
        "protocol_version": PROTOCOL_VERSION,
        "tool_count": summary.get("total_tools", 0),
        "categories": categories,
        "capabilities": capabilities,
        "provenance": build_provenance(config.wanatux_host, []),
    }
    audit_entry = build_audit_entry("meta.server_info", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def meta_health(config, args: Dict[str, Any]) -> Dict[str, Any]:
    checks: Dict[str, Any] = {}
    systemd_cmd = run_command(
        ["systemctl", "is-system-running", "--no-pager"],
        timeout_sec=config.command_timeout_sec,
        output_limit=2000,
    )
    systemd_ok = systemd_cmd.get("exit_code") == 0 and not _has_systemd_permission_error(systemd_cmd)
    systemd_error = None
    if systemd_cmd.get("error_type") == "command_not_found":
        systemd_ok = False
        systemd_error = "systemctl not found"
    elif _has_systemd_permission_error(systemd_cmd):
        systemd_ok = False
        systemd_error = "systemd access denied"
    checks["systemd"] = {
        "ok": systemd_ok,
        "error": systemd_error,
    }

    docker_socket = Path("/var/run/docker.sock")
    docker_ok = docker_socket.exists() and os.access(docker_socket, os.R_OK | os.W_OK)
    checks["docker_socket"] = {
        "ok": docker_ok,
        "path": str(docker_socket),
        "exists": docker_socket.exists(),
    }

    audit_ok = True
    audit_error = None
    audit_path = Path(config.audit_log_path)
    audit_dir = audit_path.parent
    if not audit_dir.exists():
        audit_ok = False
        audit_error = f"audit log directory missing: {audit_dir}"
    elif not os.access(audit_dir, os.W_OK):
        audit_ok = False
        audit_error = f"audit log directory not writable: {audit_dir}"
    elif audit_path.exists() and not os.access(audit_path, os.W_OK):
        audit_ok = False
        audit_error = f"audit log file not writable: {audit_path}"
    checks["audit_log"] = {
        "ok": audit_ok,
        "path": config.audit_log_path,
        "error": audit_error,
    }

    missing_roots = [root for root in config.repo_roots if not Path(root).exists()]
    checks["repo_roots"] = {
        "ok": len(missing_roots) == 0,
        "missing": missing_roots,
        "configured": config.repo_roots,
    }

    checks["python"] = {
        "ok": True,
        "version": ".".join(str(part) for part in sys.version_info[:3]),
    }

    overall_ok = all(item.get("ok") for item in checks.values())
    result = {
        "ok": overall_ok,
        "checks": checks,
        "provenance": build_provenance(config.wanatux_host, []),
    }
    audit_entry = build_audit_entry("meta.health", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def meta_validate_config(config, args: Dict[str, Any]) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []
    fix_hints: List[str] = []

    mode = (config.wanatux_mode or "auto").lower()
    if mode not in {"auto", "systemd", "compose", "script"}:
        errors.append(f"Invalid WANATUX_MODE: {mode}")
        fix_hints.append("Set WANATUX_MODE to auto|systemd|compose|script")

    if mode == "compose":
        if not config.wanatux_compose_dir:
            errors.append("WANATUX_COMPOSE_DIR is required for compose mode")
            fix_hints.append("Set WANATUX_COMPOSE_DIR to your docker-compose directory")
        elif not Path(config.wanatux_compose_dir).exists():
            errors.append(f"WANATUX_COMPOSE_DIR not found: {config.wanatux_compose_dir}")
            fix_hints.append("Ensure WANATUX_COMPOSE_DIR points to an existing path")

    if mode == "script" and not config.wanatux_restart_script:
        errors.append("WANATUX_RESTART_SCRIPT is required for script mode")
        fix_hints.append("Set WANATUX_RESTART_SCRIPT to a restart script path")

    missing_roots = [root for root in config.repo_roots if not Path(root).exists()]
    if missing_roots:
        warnings.append(f"Missing repo roots: {', '.join(missing_roots)}")
        fix_hints.append("Update MCP_REPO_ROOTS to valid directories")

    audit_dir = Path(config.audit_log_path).parent
    if not audit_dir.exists():
        warnings.append(f"Audit log directory missing: {audit_dir}")
        fix_hints.append("Create the audit log directory or update MCP_AUDIT_LOG_PATH")

    result = {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "fix_hints": fix_hints,
        "resolved": {
            "repo_roots": config.repo_roots,
            "audit_log_path": config.audit_log_path,
            "wanatux_mode": mode,
            "wanatux_systemd_service": config.wanatux_systemd_service,
            "wanatux_compose_dir": config.wanatux_compose_dir,
            "wanatux_compose_service": config.wanatux_compose_service,
            "wanatux_restart_script": config.wanatux_restart_script,
        },
        "provenance": build_provenance(config.wanatux_host, []),
    }
    audit_entry = build_audit_entry("meta.validate_config", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def meta_discover_services(config, args: Dict[str, Any]) -> Dict[str, Any]:
    pattern = args.get("pattern", "wanatux")
    if not isinstance(pattern, str) or not pattern.strip():
        result = error_response(
            "meta.discover_services",
            "pattern must be a non-empty string",
            error_code="INVALID_ARGS",
            likely_causes=["Invalid pattern argument"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("meta.discover_services", args, [], "invalid pattern", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    pattern_lower = pattern.lower()
    systemd_units: List[str] = []
    compose_services: List[str] = []
    systemd_status = {"ok": True, "error": None}
    compose_status = {"ok": True, "error": None}

    systemd_cmd = run_command(
        ["systemctl", "list-units", "--type=service", "--all", "--no-pager", "--no-legend"],
        timeout_sec=config.command_timeout_sec,
        output_limit=config.output_limit,
    )
    if systemd_cmd.get("error_type") == "command_not_found":
        systemd_status = {"ok": False, "error": "systemctl not found"}
    elif _has_systemd_permission_error(systemd_cmd):
        systemd_status = {"ok": False, "error": "systemd access denied"}
    elif systemd_cmd.get("exit_code") != 0:
        systemd_status = {"ok": False, "error": "systemctl list-units failed"}
    else:
        for line in (systemd_cmd.get("stdout") or "").splitlines():
            unit = line.split()[0] if line.split() else ""
            if unit and pattern_lower in unit.lower():
                systemd_units.append(unit)

    if config.wanatux_compose_dir and Path(config.wanatux_compose_dir).exists():
        compose_cmd = run_command(
            ["docker", "compose", "config", "--services"],
            cwd=config.wanatux_compose_dir,
            timeout_sec=config.command_timeout_sec,
            output_limit=config.output_limit,
        )
        if compose_cmd.get("error_type") == "command_not_found":
            compose_status = {"ok": False, "error": "docker compose not available"}
        elif compose_cmd.get("exit_code") != 0:
            compose_status = {"ok": False, "error": "docker compose config failed"}
        else:
            for line in (compose_cmd.get("stdout") or "").splitlines():
                if pattern_lower in line.lower():
                    compose_services.append(line.strip())
    else:
        compose_status = {"ok": False, "error": "WANATUX_COMPOSE_DIR not configured or missing"}

    result = {
        "ok": systemd_status["ok"] or compose_status["ok"],
        "pattern": pattern,
        "systemd_units": systemd_units,
        "compose_services": compose_services,
        "systemd_status": systemd_status,
        "compose_status": compose_status,
        "provenance": build_provenance(config.wanatux_host, []),
    }
    audit_entry = build_audit_entry("meta.discover_services", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


# Tool exports
MCP_META_TOOLS = [
    {
        "name": "mcp_list_metadata",
        "description": "List all MCP tools with their metadata, categories, and context information.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {"type": "string"},
                "safety_level": {"type": "string", "enum": ["safe", "modify", "destructive"]},
                "server": {"type": "string"},
            },
        },
        "handler": list_tool_metadata,
    },
    {
        "name": "mcp_generate_tool",
        "description": "Generate a new MCP tool template with proper structure and context (dry-run only).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "category": {"type": "string"},
                "description": {"type": "string"},
                "safety_level": {"type": "string", "enum": ["safe", "modify", "destructive"], "default": "safe"},
            },
            "required": ["name", "category", "description"],
        },
        "handler": generate_new_tool,
    },
    {
        "name": "meta.server_info",
        "description": "Return server version, protocol version, tool counts, and capability flags.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": meta_server_info,
    },
    {
        "name": "meta.health",
        "description": "Check systemd access, docker socket, audit log, repo roots, and Python version.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": meta_health,
    },
    {
        "name": "meta.validate_config",
        "description": "Validate config values, resolve paths, and return fix hints.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": meta_validate_config,
    },
    {
        "name": "meta.discover_services",
        "description": "List matching systemd units and docker compose services.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "default": "wanatux"},
            },
        },
        "handler": meta_discover_services,
    },
]
