import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mcp_homelab.core import get_script_logger
from mcp_homelab.errors import error_response

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    is_path_allowed,
    run_command,
    truncate_text,
)

# Setup logger
logger = get_script_logger(__name__)


def _is_script_file(path: Path) -> bool:
    """Check if file is a script based on extension."""
    script_extensions = {'.py', '.sh', '.sql', '.bash'}
    return path.suffix.lower() in script_extensions


def _extract_script_metadata(file_path: Path) -> Dict[str, Any]:
    """Extract metadata from a script file."""
    metadata = {
        "purpose": None,
        "description": None,
        "shebang": None,
        "dependencies": [],
        "is_executable": False,
        "line_count": 0,
    }
    
    try:
        # Check if executable
        metadata["is_executable"] = os.access(file_path, os.X_OK)
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            metadata["line_count"] = len(lines)
            
            # Extract shebang
            if lines and lines[0].startswith('#!'):
                metadata["shebang"] = lines[0].strip()
            
            # Look for purpose/description in first 20 lines
            for i, line in enumerate(lines[:20]):
                line = line.strip()
                
                # Python docstring
                if '"""' in line or "'''" in line:
                    desc_lines = []
                    for j in range(i, min(i+10, len(lines))):
                        desc_lines.append(lines[j].strip())
                        if j > i and ('"""' in lines[j] or "'''" in lines[j]):
                            break
                    metadata["description"] = ' '.join(desc_lines).replace('"""', '').replace("'''", '').strip()
                    break
                
                # Shell/Python comments
                if line.startswith('#') and not line.startswith('#!'):
                    comment = line[1:].strip()
                    if any(x in comment.lower() for x in ['purpose:', 'description:']):
                        metadata["purpose"] = comment.split(':', 1)[1].strip() if ':' in comment else comment
                        break
            
            # Detect dependencies
            content = ''.join(lines)
            
            # Python imports
            if file_path.suffix == '.py':
                imports = re.findall(r'^(?:from|import)\s+(\w+)', content, re.MULTILINE)
                metadata["dependencies"] = list(set([imp for imp in imports if imp not in ['os', 'sys', 're', 'json']]))
            
            # Shell source/require
            elif file_path.suffix in ['.sh', '.bash']:
                sources = re.findall(r'(?:source|\.)\s+([^\s]+)', content)
                metadata["dependencies"] = list(set(sources))
    
    except Exception as e:
        metadata["error"] = str(e)
    
    return metadata


def list_scripts(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """List scripts in a directory with metadata."""
    path = args.get("path", "")
    recursive = args.get("recursive", True)
    category = args.get("category", None)  # financial, sparkle, deployment, etc.
    
    if not path:
        path = "/opt/homelab-panel/scripts"
    
    path_obj = Path(path).resolve()
    
    if not is_path_allowed(path_obj, config.repo_roots):
        result = error_response(
            "scripts_list",
            f"Path not allowed: {path_obj}",
            error_code="ALLOWLIST_VIOLATION",
            likely_causes=["Path outside MCP_REPO_ROOTS allowlist"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("scripts_list", args, [], truncate_text(str(result), config.audit_preview_limit), error_code="ALLOWLIST_VIOLATION")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    scripts = []
    
    try:
        # If category specified, look in that subdirectory
        if category:
            path_obj = path_obj / category
            if not path_obj.exists():
                result = error_response(
                    "scripts_list",
                    f"Category not found: {category}",
                    error_code="PATH_MISSING",
                    likely_causes=["Category directory does not exist"],
                    suggested_next_tools=[{"tool": "scripts_list", "args": {"path": str(path_obj.parent)}}],
                    host=config.wanatux_host,
                )
                audit_entry = build_audit_entry("scripts_list", args, [], truncate_text(str(result), config.audit_preview_limit), error_code="PATH_MISSING")
                append_audit_log(config.audit_log_path, audit_entry)
                return result
        
        if path_obj.is_file():
            # Single file
            if _is_script_file(path_obj):
                stat = path_obj.stat()
                scripts.append({
                    "path": str(path_obj),
                    "name": path_obj.name,
                    "category": path_obj.parent.name if path_obj.parent.name != "scripts" else "root",
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "executable": os.access(path_obj, os.X_OK),
                })
        else:
            # Directory
            pattern = "**/*" if recursive else "*"
            for file_path in path_obj.glob(pattern):
                if file_path.is_file() and file_path.name != '__pycache__':
                    if _is_script_file(file_path):
                        stat = file_path.stat()
                        # Determine category from parent directory
                        rel_path = file_path.relative_to(Path("/opt/homelab-panel/scripts"))
                        cat = str(rel_path.parent) if rel_path.parent != Path('.') else "root"
                        
                        scripts.append({
                            "path": str(file_path),
                            "name": file_path.name,
                            "category": cat,
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                            "executable": os.access(file_path, os.X_OK),
                        })
        
        scripts.sort(key=lambda x: (x["category"], x["name"]))
        
        result = {
            "ok": True,
            "path": str(path_obj),
            "count": len(scripts),
            "scripts": scripts,
            "categories": list(set(s["category"] for s in scripts)),
            "provenance": build_provenance(config.wanatux_host, []),
        }
        
    except Exception as e:
        result = error_response(
            "scripts_list",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while listing scripts"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.wanatux_host,
        )
        result["scripts"] = []
    
    audit_entry = build_audit_entry("scripts_list", args, [], truncate_text(str(result), config.audit_preview_limit), error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def analyze(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze a script for metadata, purpose, and dependencies."""
    file_path = args.get("file_path", "")
    
    if not file_path:
        result = error_response(
            "scripts_analyze",
            "file_path is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing file_path argument"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("scripts_analyze", args, [], truncate_text(str(result), config.audit_preview_limit), error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    path_obj = Path(file_path).resolve()
    
    if not is_path_allowed(path_obj, config.repo_roots):
        result = error_response(
            "scripts_analyze",
            f"Path not allowed: {path_obj}",
            error_code="ALLOWLIST_VIOLATION",
            likely_causes=["Path outside MCP_REPO_ROOTS allowlist"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("scripts_analyze", args, [], truncate_text(str(result), config.audit_preview_limit), error_code="ALLOWLIST_VIOLATION")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    if not path_obj.exists():
        result = error_response(
            "scripts_analyze",
            f"Script not found: {path_obj}",
            error_code="PATH_MISSING",
            likely_causes=["File does not exist"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {"path": str(path_obj.parent)}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("scripts_analyze", args, [], truncate_text(str(result), config.audit_preview_limit), error_code="PATH_MISSING")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        stat = path_obj.stat()
        metadata = _extract_script_metadata(path_obj)
        
        # Determine category
        rel_path = path_obj.relative_to(Path("/opt/homelab-panel/scripts"))
        category = str(rel_path.parent) if rel_path.parent != Path('.') else "root"
        
        result = {
            "ok": True,
            "file_path": str(path_obj),
            "file_name": path_obj.name,
            "file_size": stat.st_size,
            "modified_date": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "category": category,
            "metadata": metadata,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        
    except Exception as e:
        result = error_response(
            "scripts_analyze",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while analyzing script"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.wanatux_host,
        )
    
    audit_entry = build_audit_entry("scripts_analyze", args, [], truncate_text(str(result), config.audit_preview_limit), error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def execute(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a script with safety checks and logging."""
    script_path = args.get("script_path", "")
    script_args = args.get("args", [])
    confirm = args.get("confirm", False)
    dry_run = args.get("dry_run", True)
    timeout = args.get("timeout", 300)  # 5 minutes default
    
    commands = []
    
    if not script_path:
        result = error_response(
            "scripts_execute",
            "script_path is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing script_path argument"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("scripts_execute", args, commands, truncate_text(str(result), config.audit_preview_limit), error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    path_obj = Path(script_path).resolve()
    
    if not is_path_allowed(path_obj, config.repo_roots):
        result = error_response(
            "scripts_execute",
            f"Path not allowed: {path_obj}",
            error_code="ALLOWLIST_VIOLATION",
            likely_causes=["Path outside MCP_REPO_ROOTS allowlist"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("scripts_execute", args, commands, truncate_text(str(result), config.audit_preview_limit), error_code="ALLOWLIST_VIOLATION")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    if not path_obj.exists():
        result = error_response(
            "scripts_execute",
            f"Script not found: {path_obj}",
            error_code="PATH_MISSING",
            likely_causes=["File does not exist"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {"path": str(path_obj.parent)}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("scripts_execute", args, commands, truncate_text(str(result), config.audit_preview_limit), error_code="PATH_MISSING")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Require confirmation for execution
    if not confirm:
        result = error_response(
            "scripts_execute",
            "Script execution requires confirm=true",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for script execution"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.wanatux_host,
        )
        result["dry_run"] = True
        result["planned_command"] = [str(path_obj)] + script_args
        audit_entry = build_audit_entry("scripts_execute", args, commands, truncate_text(str(result), config.audit_preview_limit), error_code="PERMISSION_DENIED")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    planned_command = [str(path_obj)] + script_args
    try:
        if dry_run:
            result = {
                "ok": True,
                "dry_run": True,
                "message": f"DRY RUN: Would execute {path_obj.name}",
                "planned_command": planned_command,
                "provenance": build_provenance(config.wanatux_host, commands),
            }
        else:
            cmd_result = run_command(
                planned_command,
                cwd=str(path_obj.parent),
                timeout_sec=timeout,
                output_limit=config.output_limit,
            )
            commands.append(cmd_result)
            duration_seconds = round((cmd_result.get("duration_ms", 0) or 0) / 1000, 3)
            exit_code = cmd_result.get("exit_code")
            if exit_code == 0:
                result = {
                    "ok": True,
                    "dry_run": False,
                    "script": str(path_obj),
                    "exit_code": exit_code,
                    "duration_seconds": duration_seconds,
                    "stdout": cmd_result.get("stdout", ""),
                    "stderr": cmd_result.get("stderr", ""),
                    "provenance": build_provenance(config.wanatux_host, commands),
                }
            else:
                error_type = cmd_result.get("error_type")
                error_code = "UNKNOWN"
                message = "Script execution failed"
                if error_type == "timeout":
                    error_code = "TIMEOUT"
                    message = f"Script execution timed out after {timeout} seconds"
                elif error_type == "command_not_found":
                    error_code = "COMMAND_NOT_FOUND"
                    message = "Script command not found"
                result = error_response(
                    "scripts_execute",
                    message,
                    error_code=error_code,
                    likely_causes=["Script returned non-zero exit code"],
                    suggested_next_tools=[{"tool": "scripts_analyze", "args": {"file_path": str(path_obj)}}],
                    host=config.wanatux_host,
                )
                result["dry_run"] = False
                result["script"] = str(path_obj)
                result["exit_code"] = exit_code
                result["duration_seconds"] = duration_seconds
                result["stdout"] = cmd_result.get("stdout", "")
                result["stderr_preview"] = cmd_result.get("stderr", "")
                result["planned_command"] = planned_command
    except Exception as e:
        result = error_response(
            "scripts_execute",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while executing script"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.wanatux_host,
        )

    audit_entry = build_audit_entry(
        "scripts_execute",
        args,
        commands,
        truncate_text(str(result), config.audit_preview_limit),
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


SCRIPT_TOOLS = [
    {
        "name": "scripts_list",
        "description": "List scripts in a directory with metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "recursive": {"type": "boolean", "default": True},
                "category": {"type": "string"},  # financial, sparkle, deployment, etc.
            },
            "required": [],
        },
        "handler": list_scripts,
    },
    {
        "name": "scripts_analyze",
        "description": "Analyze a script for metadata, purpose, and dependencies.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
            },
            "required": ["file_path"],
        },
        "handler": analyze,
    },
    {
        "name": "scripts_execute",
        "description": "Execute a script with safety checks and logging (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "script_path": {"type": "string"},
                "args": {"type": "array", "items": {"type": "string"}, "default": []},
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
                "timeout": {"type": "integer", "default": 300},
            },
            "required": ["script_path"],
        },
        "handler": execute,
    },
]
