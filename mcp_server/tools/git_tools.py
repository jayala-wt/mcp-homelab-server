import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from mcp_homelab.core import get_script_logger

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    is_path_allowed,
    output_preview,
    run_command,
    slice_lines,
    truncate_text,
)

# Setup logger
logger = get_script_logger(__name__)


def _validate_repo(config, repo_path: str) -> Tuple[bool, str, List[Dict[str, Any]]]:
    if not is_path_allowed(repo_path, config.repo_roots):
        return False, "repo_path not within allowlisted roots", []
    repo_dir = Path(repo_path)
    if not repo_dir.exists() or not repo_dir.is_dir():
        return False, "repo_path does not exist or is not a directory", []
    if (repo_dir / ".git").exists():
        return True, "", []
    check_cmd = run_command(
        ["git", "-C", repo_path, "rev-parse", "--git-dir"],
        timeout_sec=config.command_timeout_sec,
        output_limit=config.output_limit,
    )
    if check_cmd["exit_code"] == 0:
        return True, "", [check_cmd]
    return False, "repo_path is not a git repository", [check_cmd]


def list_repos(config, args: Dict[str, Any]) -> Dict[str, Any]:
    root_path = args.get("root_path", "")
    if not root_path:
        result = {
            "ok": False,
            "error": "root_path is required",
            "repos": [],
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_list_repos", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not is_path_allowed(root_path, config.repo_roots):
        result = {
            "ok": False,
            "error": "root_path not within allowlisted roots",
            "repos": [],
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_list_repos", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    root = Path(root_path)
    if not root.exists() or not root.is_dir():
        result = {
            "ok": False,
            "error": "root_path does not exist or is not a directory",
            "repos": [],
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_list_repos", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    repos: List[str] = []
    max_depth = config.max_repo_depth
    for dirpath, dirnames, _ in os.walk(root):
        current = Path(dirpath)
        try:
            depth = len(current.relative_to(root).parts)
        except ValueError:
            depth = max_depth
        if depth >= max_depth:
            dirnames[:] = []
        if ".git" in dirnames or (current / ".git").is_file():
            repos.append(str(current))
            dirnames[:] = []
            continue
        if ".git" in dirnames:
            dirnames.remove(".git")

    result = {
        "ok": True,
        "repos": sorted(repos),
        "provenance": build_provenance(config.wanatux_host, []),
    }
    audit_entry = build_audit_entry("git_list_repos", args, [], "")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def status(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")
    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "branch": "",
            "dirty_files": [],
            "ahead": 0,
            "behind": 0,
            "staged_files": [],
            "unstaged_files": [],
            "untracked_files": [],
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_status", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    cmd = run_command(
        ["git", "-C", repo_path, "status", "--porcelain=v1", "-b"],
        timeout_sec=config.command_timeout_sec,
        output_limit=config.output_limit,
    )
    commands.append(cmd)

    branch = ""
    ahead = 0
    behind = 0
    staged_files: List[str] = []
    unstaged_files: List[str] = []
    untracked_files: List[str] = []

    lines = (cmd.get("stdout") or "").splitlines()
    if lines and lines[0].startswith("## "):
        header = lines[0][3:]
        if "..." in header:
            branch = header.split("...", 1)[0]
        else:
            branch = header
        if "[" in header and "]" in header:
            bracket = header[header.index("[") + 1 : header.index("]")]
            for part in bracket.split(","):
                part = part.strip()
                if part.startswith("ahead "):
                    try:
                        ahead = int(part.split(" ", 1)[1])
                    except ValueError:
                        ahead = 0
                if part.startswith("behind "):
                    try:
                        behind = int(part.split(" ", 1)[1])
                    except ValueError:
                        behind = 0

    for line in lines[1:]:
        if not line:
            continue
        if line.startswith("?? "):
            untracked_files.append(line[3:])
            continue
        if len(line) < 3:
            continue
        status_flags = line[:2]
        path = line[3:]
        if status_flags[0] != " ":
            staged_files.append(path)
        if status_flags[1] != " ":
            unstaged_files.append(path)

    dirty_files = sorted(set(staged_files + unstaged_files + untracked_files))

    result = {
        "ok": cmd["exit_code"] == 0,
        "branch": branch,
        "dirty_files": dirty_files,
        "ahead": ahead,
        "behind": behind,
        "staged_files": staged_files,
        "unstaged_files": unstaged_files,
        "untracked_files": untracked_files,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git status failed"

    preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_status", args, commands, preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def diff(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")
    staged = bool(args.get("staged", False))
    paths = args.get("paths", []) or []
    start_line = int(args.get("start_line", 0))
    max_lines = args.get("max_lines")
    if max_lines is not None:
        max_lines = int(max_lines)

    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "diff": "",
            "truncated": False,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_diff", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    argv = ["git", "-C", repo_path, "diff"]
    if staged:
        argv.append("--staged")
    if paths:
        argv.append("--")
        argv.extend(paths)

    cmd = run_command(argv, timeout_sec=config.command_timeout_sec, output_limit=config.output_limit)
    commands.append(cmd)

    diff_text = cmd.get("stdout", "")
    diff_text, was_truncated = truncate_text(diff_text, config.output_limit)
    paged_text, paged_truncated = slice_lines(diff_text, start_line, max_lines)

    result = {
        "ok": cmd["exit_code"] == 0,
        "diff": paged_text,
        "truncated": was_truncated or paged_truncated,
        "start_line": start_line,
        "max_lines": max_lines,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git diff failed"

    preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_diff", args, commands, preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def log(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")
    limit = int(args.get("limit", 20))

    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "commits": [],
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_log", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    argv = [
        "git",
        "-C",
        repo_path,
        "log",
        "-n",
        str(limit),
        "--date=iso",
        "--pretty=format:%H%x1f%an%x1f%ad%x1f%s",
    ]
    cmd = run_command(argv, timeout_sec=config.command_timeout_sec, output_limit=config.output_limit)
    commands.append(cmd)

    commits: List[Dict[str, str]] = []
    for line in (cmd.get("stdout") or "").splitlines():
        parts = line.split("\x1f")
        if len(parts) != 4:
            continue
        commits.append(
            {
                "hash": parts[0],
                "author": parts[1],
                "date": parts[2],
                "subject": parts[3],
            }
        )

    result = {
        "ok": cmd["exit_code"] == 0,
        "commits": commits,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git log failed"

    preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_log", args, commands, preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def fetch(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")

    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "output_preview": "",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_fetch", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    cmd = run_command(
        ["git", "-C", repo_path, "fetch", "--prune"],
        timeout_sec=config.command_timeout_sec,
        output_limit=config.output_limit,
    )
    commands.append(cmd)

    preview_text = output_preview(commands, config.output_limit)
    result = {
        "ok": cmd["exit_code"] == 0,
        "output_preview": preview_text,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git fetch failed"

    audit_preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_fetch", args, commands, audit_preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def checkout(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")
    branch = args.get("branch", "")
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))

    if not confirm:
        result = {
            "ok": False,
            "error": "confirm=true required",
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_checkout", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_checkout", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not branch:
        result = {
            "ok": False,
            "error": "branch is required",
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_checkout", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    argv = ["git", "-C", repo_path, "checkout"]
    if dry_run:
        argv.append("--dry-run")
    argv.append(branch)

    cmd = run_command(argv, timeout_sec=config.command_timeout_sec, output_limit=config.output_limit)
    commands.append(cmd)

    preview_text = output_preview(commands, config.output_limit)
    result = {
        "ok": cmd["exit_code"] == 0,
        "output_preview": preview_text,
        "dry_run": dry_run,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git checkout failed"

    audit_preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_checkout", args, commands, audit_preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def pull_ff_only(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))

    if not confirm:
        result = {
            "ok": False,
            "error": "confirm=true required",
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_pull_ff_only", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_pull_ff_only", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    argv = ["git", "-C", repo_path, "pull", "--ff-only"]
    if dry_run:
        argv.append("--dry-run")

    cmd = run_command(argv, timeout_sec=config.command_timeout_sec, output_limit=config.output_limit)
    commands.append(cmd)

    preview_text = output_preview(commands, config.output_limit)
    result = {
        "ok": cmd["exit_code"] == 0,
        "output_preview": preview_text,
        "dry_run": dry_run,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git pull failed"

    audit_preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_pull_ff_only", args, commands, audit_preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def commit(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")
    message = args.get("message", "")
    paths = args.get("paths", []) or []
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))

    if not confirm:
        result = {
            "ok": False,
            "error": "confirm=true required",
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_commit", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not message:
        result = {
            "ok": False,
            "error": "message is required",
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_commit", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_commit", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    argv = ["git", "-C", repo_path, "commit", "-m", message]
    if dry_run:
        argv.append("--dry-run")
    if paths:
        argv.append("--")
        argv.extend(paths)

    cmd = run_command(argv, timeout_sec=config.command_timeout_sec, output_limit=config.output_limit)
    commands.append(cmd)

    preview_text = output_preview(commands, config.output_limit)
    result = {
        "ok": cmd["exit_code"] == 0,
        "output_preview": preview_text,
        "dry_run": dry_run,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git commit failed"

    audit_preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_commit", args, commands, audit_preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def push(config, args: Dict[str, Any]) -> Dict[str, Any]:
    repo_path = args.get("repo_path", "")
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))

    if not confirm:
        result = {
            "ok": False,
            "error": "confirm=true required",
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("git_push", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    is_valid, error, commands = _validate_repo(config, repo_path)
    if not is_valid:
        result = {
            "ok": False,
            "error": error,
            "output_preview": "",
            "dry_run": dry_run,
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        preview = output_preview(commands, config.audit_preview_limit)
        audit_entry = build_audit_entry("git_push", args, commands, preview)
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    argv = ["git", "-C", repo_path, "push"]
    if dry_run:
        argv.append("--dry-run")

    cmd = run_command(argv, timeout_sec=config.command_timeout_sec, output_limit=config.output_limit)
    commands.append(cmd)

    preview_text = output_preview(commands, config.output_limit)
    result = {
        "ok": cmd["exit_code"] == 0,
        "output_preview": preview_text,
        "dry_run": dry_run,
        "provenance": build_provenance(config.wanatux_host, commands),
    }
    if cmd["exit_code"] != 0:
        result["error"] = cmd.get("stderr") or "git push failed"

    audit_preview = output_preview(commands, config.audit_preview_limit)
    audit_entry = build_audit_entry("git_push", args, commands, audit_preview)
    append_audit_log(config.audit_log_path, audit_entry)
    return result


GIT_TOOLS = [
    {
        "name": "git_list_repos",
        "description": "List git repositories under an allowlisted root.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "root_path": {"type": "string"},
            },
            "required": ["root_path"],
        },
        "handler": list_repos,
    },
    {
        "name": "git_status",
        "description": "Get repository status summary.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
            },
            "required": ["repo_path"],
        },
        "handler": status,
    },
    {
        "name": "git_diff",
        "description": "Show git diff for a repository.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
                "staged": {"type": "boolean", "default": False},
                "paths": {"type": "array", "items": {"type": "string"}},
                "start_line": {"type": "integer", "default": 0},
                "max_lines": {"type": "integer"},
            },
            "required": ["repo_path"],
        },
        "handler": diff,
    },
    {
        "name": "git_log",
        "description": "List recent commits.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
            },
            "required": ["repo_path"],
        },
        "handler": log,
    },
    {
        "name": "git_fetch",
        "description": "Fetch remote updates for a repository.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
            },
            "required": ["repo_path"],
        },
        "handler": fetch,
    },
    {
        "name": "git_checkout",
        "description": "Checkout a branch (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
                "branch": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
            "required": ["repo_path", "branch"],
        },
        "handler": checkout,
    },
    {
        "name": "git_pull_ff_only",
        "description": "Pull with --ff-only (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
            "required": ["repo_path"],
        },
        "handler": pull_ff_only,
    },
    {
        "name": "git_commit",
        "description": "Commit changes (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
                "message": {"type": "string"},
                "paths": {"type": "array", "items": {"type": "string"}},
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
            "required": ["repo_path", "message"],
        },
        "handler": commit,
    },
    {
        "name": "git_push",
        "description": "Push commits (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
            "required": ["repo_path"],
        },
        "handler": push,
    },
]
