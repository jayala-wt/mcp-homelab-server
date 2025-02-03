import hashlib
import json
import os
import re
import shlex
import socket
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mcp_homelab.config import load_config
from mcp_homelab.core import get_script_logger
from mcp_homelab.errors import AuditLogError
from mcp_homelab.tool_metadata import get_tool_version
from mcp_homelab.version import SERVER_VERSION

# Setup logger for MCP utilities
logger = get_script_logger(__name__)

DEFAULT_OUTPUT_LIMIT = 20000
_CONFIG_FINGERPRINT: Optional[str] = None

SENSITIVE_KEY_PATTERN = re.compile(r"(token|secret|password|passwd|api[_-]?key|authorization)", re.IGNORECASE)
REDACTION_PATTERNS = [
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), "[REDACTED_EMAIL]"),
    (re.compile(r"\b\d{3}-\d{2}-\d{4}\b"), "[REDACTED_SSN]"),
    (re.compile(r"\b\d{9}\b"), "[REDACTED_SSN]"),
    (re.compile(r"(?i)(api[_-]?key|token|secret|password|passwd)\s*[:=]\s*([^\s,;]+)"), r"\1=[REDACTED]"),
    (re.compile(r"(?i)bearer\s+[A-Za-z0-9._\\-]+=*"), "Bearer [REDACTED]"),
]


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def truncate_text(text: str, limit: int) -> Tuple[str, bool]:
    if text is None:
        return "", False
    if len(text) <= limit:
        return text, False
    return text[:limit] + "\n...[truncated]", True


def slice_lines(text: str, start_line: int, max_lines: Optional[int]) -> Tuple[str, bool]:
    lines = text.splitlines()
    if start_line < 0:
        start_line = 0
    if max_lines is None:
        end_line = len(lines)
    else:
        end_line = min(len(lines), start_line + max_lines)
    sliced = lines[start_line:end_line]
    truncated = end_line < len(lines)
    return "\n".join(sliced), truncated


def format_command(argv: List[str]) -> str:
    return " ".join(shlex.quote(part) for part in argv)


def run_command(
    argv: List[str],
    cwd: Optional[str] = None,
    timeout_sec: int = 20,
    output_limit: int = DEFAULT_OUTPUT_LIMIT,
) -> Dict[str, Any]:
    start = time.monotonic()
    stdout = ""
    stderr = ""
    exit_code: Optional[int] = None
    error_type: Optional[str] = None
    try:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        exit_code = completed.returncode
    except FileNotFoundError as exc:
        stdout = ""
        stderr = str(exc)
        exit_code = 127
        error_type = "command_not_found"
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        exit_code = -1
        stderr = (stderr + "\n" if stderr else "") + f"Timeout after {timeout_sec}s"
        error_type = "timeout"
    duration_ms = int((time.monotonic() - start) * 1000)
    stdout, stdout_truncated = truncate_text(stdout, output_limit)
    stderr, stderr_truncated = truncate_text(stderr, output_limit)
    return {
        "argv": argv,
        "cwd": cwd,
        "stdout": stdout,
        "stderr": stderr,
        "exit_code": exit_code,
        "duration_ms": duration_ms,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
        "error_type": error_type,
    }


def output_preview(command_results: List[Dict[str, Any]], limit: int) -> str:
    if not command_results:
        return ""
    last = command_results[-1]
    combined = last.get("stdout", "")
    stderr = last.get("stderr", "")
    if stderr:
        combined = combined + "\n[stderr]\n" + stderr
    preview, _ = truncate_text(combined, limit)
    return preview


def _config_fingerprint() -> str:
    global _CONFIG_FINGERPRINT
    if _CONFIG_FINGERPRINT is not None:
        return _CONFIG_FINGERPRINT
    try:
        config = load_config()
        config_payload = {
            "repo_roots": config.repo_roots,
            "audit_log_path": config.audit_log_path,
            "wanatux_mode": config.wanatux_mode,
            "wanatux_systemd_service": config.wanatux_systemd_service,
            "wanatux_compose_dir": config.wanatux_compose_dir,
            "wanatux_compose_service": config.wanatux_compose_service,
            "wanatux_restart_script": config.wanatux_restart_script,
            "wanatux_host": config.wanatux_host,
            "command_timeout_sec": config.command_timeout_sec,
            "output_limit": config.output_limit,
            "audit_preview_limit": config.audit_preview_limit,
            "max_repo_depth": config.max_repo_depth,
        }
        payload = json.dumps(config_payload, sort_keys=True, default=str)
        _CONFIG_FINGERPRINT = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    except Exception:
        _CONFIG_FINGERPRINT = "unknown"
    return _CONFIG_FINGERPRINT


def redact_text(text: str) -> str:
    if not text:
        return text
    redacted = text
    for pattern, replacement in REDACTION_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def redact_value(value: Any) -> Any:
    if isinstance(value, dict):
        redacted_dict: Dict[str, Any] = {}
        for key, item in value.items():
            if SENSITIVE_KEY_PATTERN.search(str(key)):
                redacted_dict[key] = "[REDACTED]"
                continue
            redacted_dict[key] = redact_value(item)
        return redacted_dict
    if isinstance(value, list):
        return [redact_value(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def is_path_allowed(path: str, allowed_roots: List[str]) -> bool:
    try:
        resolved_path = str(Path(path).resolve())
    except OSError:
        resolved_path = str(Path(path))
    for root in allowed_roots:
        try:
            resolved_root = str(Path(root).resolve())
        except OSError:
            resolved_root = str(Path(root))
        try:
            common = os.path.commonpath([resolved_path, resolved_root])
        except ValueError:
            continue
        if common == resolved_root:
            return True
    return False


def _is_preview_truncated(text: str, command_results: List[Dict[str, Any]]) -> bool:
    if text and "[truncated]" in text:
        return True
    for result in command_results:
        if not isinstance(result, dict):
            continue
        if result.get("stdout_truncated") or result.get("stderr_truncated"):
            return True
    return False


def _extract_run_step_ids(args: Dict[str, Any]) -> Tuple[str, str]:
    run_id = args.get("run_id") or args.get("_run_id")
    step_id = args.get("step_id") or args.get("_step_id")
    if not run_id:
        run_id = f"run-{uuid.uuid4().hex}"
    if not step_id:
        step_id = f"step-{uuid.uuid4().hex}"
    return run_id, step_id


def ensure_audit_log_path(path: str) -> None:
    """Ensure audit log directory exists (uses LOGS_DIR from core)."""
    path_obj = Path(path)
    try:
        path_obj.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise AuditLogError(f"Audit log directory not writable: {path_obj.parent}") from exc
    if not os.access(path_obj.parent, os.W_OK):
        raise AuditLogError(f"Audit log directory not writable: {path_obj.parent}")
    if path_obj.exists() and not os.access(path_obj, os.W_OK):
        raise AuditLogError(f"Audit log file not writable: {path_obj}")
    logger.debug(f"Ensured audit log path: {path}")


def append_audit_log(path: str, entry: Dict[str, Any]) -> None:
    """Append audit entry to log file."""
    ensure_audit_log_path(path)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True))
        handle.write("\n")
    logger.debug(f"Appended audit log entry for tool: {entry.get('tool', 'unknown')}")


def build_provenance(host: str, commands: List[Dict[str, Any]]) -> Dict[str, Any]:
    formatted = []
    for cmd in commands:
        if isinstance(cmd, dict):
            argv = cmd.get("argv") or []
            formatted.append(format_command(argv))
        else:
            formatted.append(str(cmd))
    return {
        "host": host,
        "timestamp": utc_timestamp(),
        "commands": formatted,
    }


def build_audit_entry(
    tool_name: str,
    args: Dict[str, Any],
    command_results: List[Dict[str, Any]],
    output_preview_text: str,
    error_code: Optional[str] = None,
) -> Dict[str, Any]:
    last_result = next((item for item in reversed(command_results) if isinstance(item, dict)), None)
    exit_code = last_result.get("exit_code") if isinstance(last_result, dict) else None
    total_duration_ms = sum(
        result.get("duration_ms", 0) or 0
        for result in command_results
        if isinstance(result, dict)
    )
    run_id, step_id = _extract_run_step_ids(args)
    output_preview_redacted = redact_text(output_preview_text or "")
    truncated = _is_preview_truncated(output_preview_redacted, command_results)
    commands = []
    for result in command_results:
        if not isinstance(result, dict):
            continue
        commands.append(
            {
                "argv": result.get("argv"),
                "cwd": result.get("cwd"),
                "exit_code": result.get("exit_code"),
                "duration_ms": result.get("duration_ms"),
            }
        )
    config = load_config()
    output_hash = hashlib.sha256(output_preview_redacted.encode("utf-8")).hexdigest()
    return {
        "timestamp": utc_timestamp(),
        "run_id": run_id,
        "step_id": step_id,
        "tool": tool_name,
        "args_redacted": redact_value(args),
        "commands": commands,
        "cwd": commands[-1]["cwd"] if commands else None,
        "exit_code": exit_code,
        "duration_ms": total_duration_ms,
        "output_preview": output_preview_redacted,
        "output_hash": output_hash,
        "truncated": truncated,
        "error_code": error_code,
        "server_version": SERVER_VERSION,
        "tool_version": get_tool_version(tool_name),
        "config_hash": _config_fingerprint(),
        "host": config.wanatux_host or socket.gethostname(),
    }
