import os
import signal
import socket
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp_homelab.core import get_script_logger
from mcp_homelab.errors import error_response

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    output_preview,
    run_command,
    slice_lines,
    truncate_text,
)

# Setup logger
logger = get_script_logger(__name__)

DEFAULT_PORTS = {
    "homelab-panel": 8088,
}

SYSTEMD_PERMISSION_REMEDIATION = (
    "Run MCP server with host systemd access or configure WANATUX_MODE=compose|script"
)


def _has_systemd_permission_error(command_result: Dict[str, Any]) -> bool:
    combined = f"{command_result.get('stdout', '')}\n{command_result.get('stderr', '')}"
    return (
        "Failed to connect to bus" in combined
        or "Operation not permitted" in combined
        or "Permission denied" in combined
    )


def _resolve_mode(config) -> str:
    mode = (config.wanatux_mode or "auto").lower()
    if mode != "auto":
        return mode
    if config.wanatux_systemd_service:
        return "systemd"
    if config.wanatux_compose_dir:
        return "compose"
    if config.wanatux_restart_script:
        return "script"
    return "systemd"


def _port_check(host: str, port: int, timeout_sec: int = 2) -> Dict[str, Any]:
    result = {"host": host, "port": port, "ok": False}
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            result["ok"] = True
    except OSError as exc:
        result["error"] = str(exc)
    return result


def _systemd_status(config, args: Dict[str, Any]) -> Dict[str, Any]:
    service = config.wanatux_systemd_service
    commands: List[Dict[str, Any]] = []

    active_cmd = run_command(
        ["systemctl", "is-active", service],
        timeout_sec=config.command_timeout_sec,
        output_limit=config.output_limit,
    )
    commands.append(active_cmd)
    status_cmd = run_command(
        ["systemctl", "status", service, "--no-pager"],
        timeout_sec=config.command_timeout_sec,
        output_limit=config.output_limit,
    )
    commands.append(status_cmd)

    status_text = status_cmd.get("stdout", "")
    status_text, _ = truncate_text(status_text, config.output_limit)

    port_check = None
    if service in DEFAULT_PORTS:
        port_check = _port_check(config.wanatux_host, DEFAULT_PORTS[service])

    permission_error = _has_systemd_permission_error(active_cmd) or _has_systemd_permission_error(status_cmd)
    ok = status_cmd["exit_code"] == 0 and not permission_error
    error_code = None
    result: Dict[str, Any] = {
        "ok": ok,
        "mode": "systemd",
        "service": service,
        "active": (active_cmd.get("stdout", "") or "").strip(),
        "status_summary": status_text,
        "port_check": port_check,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if permission_error:
        error_code = "SYSTEMD_BUS_DENIED"
        result.update(
            error_response(
                "lab_status",
                "systemd access blocked",
                error_code=error_code,
                likely_causes=["systemd bus access denied for the current user"],
                suggested_next_tools=[{"tool": "meta.health", "args": {}}, {"tool": "meta.validate_config", "args": {}}],
                host=config.wanatux_host,
            )
        )
        result["mode"] = "systemd"
        result["service"] = service
        result["status_summary"] = status_text
        result["port_check"] = port_check
        result["remediation"] = SYSTEMD_PERMISSION_REMEDIATION
    elif status_cmd["exit_code"] != 0:
        error_code = "SERVICE_NOT_FOUND" if "could not be found" in (status_cmd.get("stderr") or "") else "UNKNOWN"
        result.update(
            error_response(
                "lab_status",
                "systemctl status failed",
                error_code=error_code,
                likely_causes=["Service name is incorrect", "systemctl failed to execute"],
                suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
                host=config.wanatux_host,
            )
        )
        result["mode"] = "systemd"
        result["service"] = service
        result["status_summary"] = status_text
        result["port_check"] = port_check

    preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("lab_status", args, commands, preview, error_code=error_code)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def _compose_status(config, args: Dict[str, Any]) -> Dict[str, Any]:
    compose_dir = config.wanatux_compose_dir
    service = config.wanatux_compose_service

    commands: List[Dict[str, Any]] = []
    if not compose_dir:
        result = error_response(
            "lab_status",
            "WANATUX_COMPOSE_DIR is not set",
            error_code="INVALID_ARGS",
            likely_causes=["WANATUX_COMPOSE_DIR env var not configured"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.wanatux_host,
        )
        result["mode"] = "compose"
        audit_entry = build_audit_entry("lab_status", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    compose_path = Path(compose_dir)
    if not compose_path.exists():
        result = error_response(
            "lab_status",
            "compose directory does not exist",
            error_code="PATH_MISSING",
            likely_causes=["WANATUX_COMPOSE_DIR path is invalid"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.wanatux_host,
        )
        result["mode"] = "compose"
        audit_entry = build_audit_entry("lab_status", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    argv = ["docker", "compose", "ps"]
    cmd = run_command(
        argv,
        cwd=str(compose_path),
        timeout_sec=config.command_timeout_sec,
        output_limit=config.output_limit,
    )
    commands.append(cmd)

    status_text = cmd.get("stdout", "")
    status_text, _ = truncate_text(status_text, config.output_limit)

    error_code = None
    result: Dict[str, Any] = {
        "ok": cmd["exit_code"] == 0,
        "mode": "compose",
        "compose_dir": str(compose_path),
        "compose_service": service,
        "status_summary": status_text,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        error_code = "COMMAND_NOT_FOUND" if cmd.get("error_type") == "command_not_found" else "UNKNOWN"
        result.update(
            error_response(
                "lab_status",
                "docker compose ps failed",
                error_code=error_code,
                likely_causes=["Docker CLI not installed", "Compose file missing or invalid"],
                suggested_next_tools=[{"tool": "meta.health", "args": {}}],
                host=config.wanatux_host,
            )
        )
        result["mode"] = "compose"
        result["compose_dir"] = str(compose_path)
        result["compose_service"] = service
        result["status_summary"] = status_text

    preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("lab_status", args, commands, preview, error_code=error_code)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def _script_status(config, args: Dict[str, Any]) -> Dict[str, Any]:
    commands: List[Dict[str, Any]] = []
    result = error_response(
        "lab_status",
        "script mode does not provide status without a custom command",
        error_code="INVALID_ARGS",
        likely_causes=["WANATUX_MODE=script requires a custom status implementation"],
        suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
        host=config.wanatux_host,
    )
    result["mode"] = "script"
    audit_entry = build_audit_entry("lab_status", args, commands, "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def status(config, args: Dict[str, Any]) -> Dict[str, Any]:
    mode = _resolve_mode(config)
    if mode == "systemd":
        result = _systemd_status(config, args)
    elif mode == "compose":
        result = _compose_status(config, args)
    elif mode == "script":
        result = _script_status(config, args)
    else:
        result = None

    if result is not None:
        # Replace the raw multi-line systemd blob with a compact summary.
        # Small models (Qwen, etc.) loop endlessly when they get a 50-line
        # status dump they can't parse. Full text still available via lab_logs.
        active = result.get("active", "unknown")
        port_ok = (result.get("port_check") or {}).get("ok")
        result["status_summary"] = (
            f"service={result.get('service','?')} active={active} "
            f"port_ok={port_ok}"
        )
        return result
    result = error_response(
        "lab_status",
        "unsupported WANATUX_MODE",
        error_code="INVALID_ARGS",
        likely_causes=["WANATUX_MODE not in auto|systemd|compose|script"],
        suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
        host=config.wanatux_host,
    )
    result["mode"] = mode
    audit_entry = build_audit_entry("lab_status", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def logs(config, args: Dict[str, Any]) -> Dict[str, Any]:
    mode = _resolve_mode(config)
    lines = int(args.get("lines", 200))
    start_line = int(args.get("start_line", 0))
    max_lines = args.get("max_lines")
    if max_lines is not None:
        max_lines = int(max_lines)

    commands: List[Dict[str, Any]] = []
    if mode == "systemd":
        service = config.wanatux_systemd_service
        cmd = run_command(
            ["journalctl", "-u", service, "-n", str(lines), "--no-pager"],
            timeout_sec=config.command_timeout_sec,
            output_limit=config.output_limit,
        )
        commands.append(cmd)
        log_text = cmd.get("stdout", "")
        log_text, was_truncated = truncate_text(log_text, config.output_limit)
        paged_text, paged_truncated = slice_lines(log_text, start_line, max_lines)
        permission_error = _has_systemd_permission_error(cmd)
        error_code = None
        result: Dict[str, Any] = {
            "ok": cmd["exit_code"] == 0 and not permission_error,
            "mode": "systemd",
            "service": service,
            "logs": paged_text,
            "truncated": was_truncated or paged_truncated,
            "start_line": start_line,
            "max_lines": max_lines,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        if permission_error:
            error_code = "SYSTEMD_BUS_DENIED"
            result.update(
                error_response(
                    "lab_logs",
                    "systemd access blocked",
                    error_code=error_code,
                    likely_causes=["systemd bus access denied for the current user"],
                    suggested_next_tools=[{"tool": "meta.health", "args": {}}, {"tool": "meta.validate_config", "args": {}}],
                    host=config.wanatux_host,
                )
            )
            result["mode"] = "systemd"
            result["service"] = service
            result["logs"] = paged_text
            result["truncated"] = was_truncated or paged_truncated
            result["start_line"] = start_line
            result["max_lines"] = max_lines
            result["remediation"] = SYSTEMD_PERMISSION_REMEDIATION
        elif cmd["exit_code"] != 0:
            error_code = "COMMAND_NOT_FOUND" if cmd.get("error_type") == "command_not_found" else "UNKNOWN"
            result.update(
                error_response(
                    "lab_logs",
                    "journalctl failed",
                    error_code=error_code,
                    likely_causes=["journalctl failed to run or unit does not exist"],
                    suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
                    host=config.wanatux_host,
                )
            )
            result["mode"] = "systemd"
            result["service"] = service
            result["logs"] = paged_text
            result["truncated"] = was_truncated or paged_truncated
            result["start_line"] = start_line
            result["max_lines"] = max_lines

        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("lab_logs", args, commands, preview, error_code=error_code)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if mode == "compose":
        compose_dir = config.wanatux_compose_dir
        service = config.wanatux_compose_service
        if not compose_dir:
            result = error_response(
                "lab_logs",
                "WANATUX_COMPOSE_DIR is not set",
                error_code="INVALID_ARGS",
                likely_causes=["WANATUX_COMPOSE_DIR env var not configured"],
                suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
                host=config.wanatux_host,
            )
            result["mode"] = "compose"
            result["logs"] = ""
            result["truncated"] = False
            result["start_line"] = start_line
            result["max_lines"] = max_lines
            audit_entry = build_audit_entry("lab_logs", args, commands, "")
            append_audit_log(config.audit_log_path, audit_entry)
            return result

        argv = ["docker", "compose", "logs", "--tail", str(lines)]
        if service:
            argv.append(service)
        cmd = run_command(
            argv,
            cwd=compose_dir,
            timeout_sec=config.command_timeout_sec,
            output_limit=config.output_limit,
        )
        commands.append(cmd)
        log_text = cmd.get("stdout", "")
        log_text, was_truncated = truncate_text(log_text, config.output_limit)
        paged_text, paged_truncated = slice_lines(log_text, start_line, max_lines)
        error_code = None
        result: Dict[str, Any] = {
            "ok": cmd["exit_code"] == 0,
            "mode": "compose",
            "compose_dir": compose_dir,
            "compose_service": service,
            "logs": paged_text,
            "truncated": was_truncated or paged_truncated,
            "start_line": start_line,
            "max_lines": max_lines,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        if cmd["exit_code"] != 0:
            error_code = "COMMAND_NOT_FOUND" if cmd.get("error_type") == "command_not_found" else "UNKNOWN"
            result.update(
                error_response(
                    "lab_logs",
                    "docker compose logs failed",
                    error_code=error_code,
                    likely_causes=["Docker CLI not installed", "Compose file missing or invalid"],
                    suggested_next_tools=[{"tool": "meta.health", "args": {}}],
                    host=config.wanatux_host,
                )
            )
            result["mode"] = "compose"
            result["compose_dir"] = compose_dir
            result["compose_service"] = service
            result["logs"] = paged_text
            result["truncated"] = was_truncated or paged_truncated
            result["start_line"] = start_line
            result["max_lines"] = max_lines

        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("lab_logs", args, commands, preview, error_code=error_code)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    result = error_response(
        "lab_logs",
        "script mode does not provide logs without a custom command",
        error_code="INVALID_ARGS",
        likely_causes=["WANATUX_MODE=script requires a custom logs implementation"],
        suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
        host=config.wanatux_host,
    )
    result["mode"] = mode
    result["logs"] = ""
    result["truncated"] = False
    result["start_line"] = start_line
    result["max_lines"] = max_lines
    audit_entry = build_audit_entry("lab_logs", args, commands, "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def restart(config, args: Dict[str, Any]) -> Dict[str, Any]:
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))
    mode = _resolve_mode(config)

    if not confirm:
        result = error_response(
            "lab_restart",
            "confirm=true required",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for restart"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.wanatux_host,
        )
        result["mode"] = mode
        result["dry_run"] = dry_run
        audit_entry = build_audit_entry("lab_restart", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    commands: List[Dict[str, Any]] = []
    planned_command: Optional[List[str]] = None

    if mode == "systemd":
        planned_command = ["sudo", "systemctl", "restart", config.wanatux_systemd_service]
        if not dry_run:
            cmd = run_command(
                planned_command,
                timeout_sec=config.command_timeout_sec,
                output_limit=config.output_limit,
            )
            commands.append(cmd)
    elif mode == "compose":
        if not config.wanatux_compose_dir:
            result = error_response(
                "lab_restart",
                "WANATUX_COMPOSE_DIR is not set",
                error_code="INVALID_ARGS",
                likely_causes=["WANATUX_COMPOSE_DIR env var not configured"],
                suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
                host=config.wanatux_host,
            )
            result["mode"] = mode
            result["dry_run"] = dry_run
            audit_entry = build_audit_entry("lab_restart", args, commands, "")
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        planned_command = ["docker", "compose", "restart"]
        if config.wanatux_compose_service:
            planned_command.append(config.wanatux_compose_service)
        if not dry_run:
            cmd = run_command(
                planned_command,
                cwd=config.wanatux_compose_dir,
                timeout_sec=config.command_timeout_sec,
                output_limit=config.output_limit,
            )
            commands.append(cmd)
    elif mode == "script":
        if not config.wanatux_restart_script:
            result = error_response(
                "lab_restart",
                "WANATUX_RESTART_SCRIPT is not set",
                error_code="INVALID_ARGS",
                likely_causes=["WANATUX_RESTART_SCRIPT env var not configured"],
                suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
                host=config.wanatux_host,
            )
            result["mode"] = mode
            result["dry_run"] = dry_run
            audit_entry = build_audit_entry("lab_restart", args, commands, "")
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        planned_command = [config.wanatux_restart_script]
        if not dry_run:
            cmd = run_command(
                planned_command,
                timeout_sec=config.command_timeout_sec,
                output_limit=config.output_limit,
            )
            commands.append(cmd)
    else:
        result = error_response(
            "lab_restart",
            "unsupported WANATUX_MODE",
            error_code="INVALID_ARGS",
            likely_causes=["WANATUX_MODE not in auto|systemd|compose|script"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.wanatux_host,
        )
        result["mode"] = mode
        result["dry_run"] = dry_run
        audit_entry = build_audit_entry("lab_restart", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    status_result = status(config, {})
    logs_result = logs(config, {"lines": 100})

    output_ok = True
    error = None
    remediation = None
    if commands:
        if mode == "systemd" and _has_systemd_permission_error(commands[-1]):
            output_ok = False
            error = "systemd access blocked"
            remediation = SYSTEMD_PERMISSION_REMEDIATION
        elif commands[-1]["exit_code"] != 0:
            output_ok = False
            error = "restart command failed"

    result = {
        "ok": output_ok,
        "mode": mode,
        "dry_run": dry_run,
        "planned_command": planned_command,
        "status": status_result,
        "logs": logs_result,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if error:
        error_code = "SYSTEMD_BUS_DENIED" if "systemd access blocked" in error else "UNKNOWN"
        result.update(
            error_response(
                "lab_restart",
                error,
                error_code=error_code,
                likely_causes=["systemd bus access denied", "restart command failed"],
                suggested_next_tools=[{"tool": "meta.health", "args": {}}, {"tool": "meta.validate_config", "args": {}}],
                host=config.wanatux_host,
            )
        )
        result["mode"] = mode
        result["dry_run"] = dry_run
        result["planned_command"] = planned_command
        result["status"] = status_result
        result["logs"] = logs_result
    if remediation:
        result["remediation"] = remediation

    preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("lab_restart", args, commands, preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def _find_mcp_server_pid() -> Optional[int]:
    """Find the PID of the running MCP server process (python3 -m mcp_homelab.server)."""
    my_pid = os.getpid()
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        if pid == my_pid:
            continue
        try:
            cmdline_path = Path(f"/proc/{pid}/cmdline")
            cmdline = cmdline_path.read_bytes().decode("utf-8", errors="replace")
            if "mcp_homelab" in cmdline and ("server" in cmdline or "mcp_homelab.server" in cmdline):
                return pid
        except (OSError, PermissionError):
            continue
    return None


def mcp_server_restart(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Restart the MCP server process by sending SIGTERM.

    The MCP server (python3 -m mcp_homelab.server) is a separate process
    from the homelab-panel systemd service. It is spawned by VS Code and
    must be killed for code changes in mcp_homelab/ to take effect.
    VS Code will automatically respawn it on the next MCP tool call.

    NOTE: This tool will terminate its own process. The response may not
    be delivered. The next MCP tool call will trigger a fresh server.
    """
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))

    pid = _find_mcp_server_pid()
    # If we couldn't find another MCP process, we ARE the MCP process
    if pid is None:
        pid = os.getpid()

    result = {
        "ok": True,
        "mcp_server_pid": pid,
        "is_self": pid == os.getpid(),
        "dry_run": dry_run,
        "action": "SIGTERM",
        "note": (
            "The MCP server is a SEPARATE process from homelab-panel. "
            "Restarting homelab-panel (systemd) does NOT reload MCP code. "
            "This tool sends SIGTERM to the MCP server process; VS Code "
            "will respawn it on the next tool call with fresh code."
        ),
        "provenance": build_provenance(config.wanatux_host, []),
    }

    if not confirm:
        result["ok"] = False
        result["message"] = "confirm=true required to restart MCP server"
        audit_entry = build_audit_entry("mcp_server_restart", args, [], "needs confirm")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if dry_run:
        result["message"] = f"Would send SIGTERM to MCP server PID {pid}"
        audit_entry = build_audit_entry("mcp_server_restart", args, [], "dry_run")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    # Write audit BEFORE killing ourselves
    result["message"] = f"Sending SIGTERM to MCP server PID {pid}"
    audit_entry = build_audit_entry("mcp_server_restart", args, [], f"kill {pid}")
    append_audit_log(config.audit_log_path, audit_entry)

    logger.info("Sending SIGTERM to MCP server PID %d (self=%s)", pid, pid == os.getpid())
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        result["ok"] = False
        result["message"] = f"Failed to kill PID {pid}: {exc}"
        logger.error("Failed to kill MCP server PID %d: %s", pid, exc)

    return result


LAB_TOOLS = [
    {
        "name": "lab_status",
        "description": "Check Wanatux Lab service status.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": status,
    },
    {
        "name": "lab_logs",
        "description": "Fetch recent Wanatux Lab logs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "lines": {"type": "integer", "default": 200},
                "start_line": {"type": "integer", "default": 0},
                "max_lines": {"type": "integer"},
            },
        },
        "handler": logs,
    },
    {
        "name": "lab_restart",
        "description": "Restart Wanatux Lab service (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
        },
        "handler": restart,
    },
    {
        "name": "mcp_server_restart",
        "description": (
            "Restart the MCP server process itself (NOT the homelab-panel service). "
            "The MCP server is a separate VS Code-spawned process. Code changes in "
            "mcp_homelab/ require this restart to take effect. VS Code will auto-respawn "
            "the server on the next tool call."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
        },
        "handler": mcp_server_restart,
    },
]
