import os
from dataclasses import dataclass
from pathlib import Path
from typing import List


@dataclass
class Config:
    repo_roots: List[str]
    audit_log_path: str
    wanatux_mode: str
    wanatux_systemd_service: str
    wanatux_compose_dir: str
    wanatux_compose_service: str
    wanatux_restart_script: str
    wanatux_host: str
    command_timeout_sec: int = 20
    output_limit: int = 20000
    audit_preview_limit: int = 1000
    max_repo_depth: int = 4


def _split_csv(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _resolve_path(path: str) -> str:
    return str(Path(path).expanduser().resolve())


def _resolve_if_relative(path: str, base_dir: Path) -> str:
    path_obj = Path(path).expanduser()
    if path_obj.is_absolute():
        return str(path_obj)
    return str((base_dir / path_obj).resolve())


def _resolve_roots(roots: List[str]) -> List[str]:
    resolved = []
    for root in roots:
        try:
            resolved.append(_resolve_path(root))
        except OSError:
            resolved.append(str(Path(root).expanduser()))
    return resolved


def load_config() -> Config:
    base_dir = Path(__file__).resolve().parent.parent
    default_repo_root = str(base_dir)

    env_roots = os.environ.get("MCP_REPO_ROOTS", "").strip()
    repo_roots = _split_csv(env_roots) if env_roots else [default_repo_root]

    audit_log_path = os.environ.get("MCP_AUDIT_LOG_PATH", "./logs/mcp_audit.jsonl")
    audit_log_path = _resolve_if_relative(audit_log_path, base_dir)

    wanatux_mode = os.environ.get("WANATUX_MODE", "auto")
    wanatux_systemd_service = os.environ.get("WANATUX_SYSTEMD_SERVICE", "").strip()
    if not wanatux_systemd_service:
        wanatux_systemd_service = "homelab-panel"

    wanatux_compose_dir = os.environ.get("WANATUX_COMPOSE_DIR", "").strip()
    if wanatux_compose_dir:
        wanatux_compose_dir = _resolve_path(wanatux_compose_dir)

    wanatux_compose_service = os.environ.get("WANATUX_COMPOSE_SERVICE", "").strip()

    wanatux_restart_script = os.environ.get("WANATUX_RESTART_SCRIPT", "").strip()
    if wanatux_restart_script:
        wanatux_restart_script = _resolve_path(wanatux_restart_script)

    wanatux_host = os.environ.get("WANATUX_HOST", "localhost")

    return Config(
        repo_roots=_resolve_roots(repo_roots),
        audit_log_path=audit_log_path,
        wanatux_mode=wanatux_mode,
        wanatux_systemd_service=wanatux_systemd_service,
        wanatux_compose_dir=wanatux_compose_dir,
        wanatux_compose_service=wanatux_compose_service,
        wanatux_restart_script=wanatux_restart_script,
        wanatux_host=wanatux_host,
    )
