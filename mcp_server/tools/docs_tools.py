import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from mcp_homelab.core import get_script_logger

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    is_path_allowed,
    truncate_text,
)

# Setup logger
logger = get_script_logger(__name__)


def _is_documentation_file(path: Path) -> bool:
    """Check if file is a documentation file based on extension."""
    doc_extensions = {'.md', '.txt', '.rst', '.adoc'}
    return path.suffix.lower() in doc_extensions


def _extract_metadata(file_path: Path) -> Dict[str, Any]:
    """Extract metadata from a documentation file."""
    metadata = {
        "title": None,
        "last_updated": None,
        "word_count": 0,
        "line_count": 0,
        "headings": [],
        "has_todos": False,
        "code_blocks": 0,
    }
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            lines = content.split('\n')
            
            metadata["line_count"] = len(lines)
            metadata["word_count"] = len(content.split())
            
            # Extract title (first # heading)
            for line in lines:
                if line.startswith('# '):
                    metadata["title"] = line[2:].strip()
                    break
            
            # Extract all headings
            for line in lines:
                if line.startswith('#'):
                    metadata["headings"].append(line.strip())
            
            # Find "Last Updated" date
            last_updated_match = re.search(
                r'\*\*Last Updated:\*\*\s*([A-Za-z]+\s+\d{1,2},?\s+\d{4})',
                content
            )
            if last_updated_match:
                metadata["last_updated"] = last_updated_match.group(1)
            
            # Check for TODOs
            metadata["has_todos"] = bool(re.search(r'\b(TODO|FIXME|XXX)\b', content, re.IGNORECASE))
            
            # Count code blocks
            metadata["code_blocks"] = len(re.findall(r'```', content)) // 2
            
    except Exception as e:
        metadata["error"] = str(e)
    
    return metadata


def _extract_links(file_path: Path) -> List[str]:
    """Extract file links from a markdown document."""
    links = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Match markdown links: [text](path)
            md_links = re.findall(r'\[([^\]]+)\]\(([^\)]+)\)', content)
            for _, link in md_links:
                # Skip URLs
                if not link.startswith(('http://', 'https://', 'mailto:', '#')):
                    links.append(link)
            
            # Match relative file references
            file_refs = re.findall(r'`([^`]+\.(md|txt|py|sh|json))`', content)
            for ref, _ in file_refs:
                if '/' in ref or '\\' in ref:
                    links.append(ref)
    
    except Exception:
        pass
    
    return list(set(links))


def list_docs(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """List documentation files in a directory with metadata."""
    path = args.get("path", "")
    recursive = args.get("recursive", True)
    extension_filter = args.get("extension_filter", [".md", ".txt"])
    
    if not path:
        result = {
            "ok": False,
            "error": "path is required",
            "files": [],
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_list", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    if not is_path_allowed(path, config.repo_roots):
        result = {
            "ok": False,
            "error": "path not within allowlisted roots",
            "files": [],
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_list", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    path_obj = Path(path)
    if not path_obj.exists():
        result = {
            "ok": False,
            "error": "path does not exist",
            "files": [],
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_list", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    files = []
    
    try:
        if path_obj.is_file():
            # Single file
            if _is_documentation_file(path_obj):
                stat = path_obj.stat()
                files.append({
                    "path": str(path_obj),
                    "name": path_obj.name,
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                })
        else:
            # Directory
            pattern = "**/*" if recursive else "*"
            for file_path in path_obj.glob(pattern):
                if file_path.is_file():
                    if extension_filter and file_path.suffix not in extension_filter:
                        continue
                    if _is_documentation_file(file_path):
                        stat = file_path.stat()
                        files.append({
                            "path": str(file_path),
                            "name": file_path.name,
                            "size": stat.st_size,
                            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                        })
        
        files.sort(key=lambda x: x["path"])
        
        result = {
            "ok": True,
            "path": str(path_obj),
            "count": len(files),
            "files": files,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        
    except Exception as e:
        result = {
            "ok": False,
            "error": str(e),
            "files": [],
            "provenance": build_provenance(config.wanatux_host, []),
        }
    
    audit_entry = build_audit_entry("docs_list", args, [], truncate_text(str(result), config.audit_preview_limit))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def analyze(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze a documentation file for metadata, structure, and issues."""
    file_path = args.get("file_path", "")
    
    if not file_path:
        result = {
            "ok": False,
            "error": "file_path is required",
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_analyze", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    if not is_path_allowed(file_path, config.repo_roots):
        result = {
            "ok": False,
            "error": "file_path not within allowlisted roots",
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_analyze", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    path_obj = Path(file_path)
    if not path_obj.exists() or not path_obj.is_file():
        result = {
            "ok": False,
            "error": "file_path does not exist or is not a file",
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_analyze", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        stat = path_obj.stat()
        metadata = _extract_metadata(path_obj)
        links = _extract_links(path_obj)
        
        # Calculate days since last modification
        modified_date = datetime.fromtimestamp(stat.st_mtime)
        days_since_modified = (datetime.now() - modified_date).days
        
        result = {
            "ok": True,
            "file_path": str(path_obj),
            "file_name": path_obj.name,
            "file_size": stat.st_size,
            "modified_date": modified_date.isoformat(),
            "days_since_modified": days_since_modified,
            "metadata": metadata,
            "links_found": len(links),
            "links": links[:20],  # Limit to first 20
            "provenance": build_provenance(config.wanatux_host, []),
        }
        
    except Exception as e:
        result = {
            "ok": False,
            "error": str(e),
            "provenance": build_provenance(config.wanatux_host, []),
        }
    
    audit_entry = build_audit_entry("docs_analyze", args, [], truncate_text(str(result), config.audit_preview_limit))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def validate_links(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Validate that file links in a document exist."""
    file_path = args.get("file_path", "")
    
    if not file_path:
        result = {
            "ok": False,
            "error": "file_path is required",
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_validate_links", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    if not is_path_allowed(file_path, config.repo_roots):
        result = {
            "ok": False,
            "error": "file_path not within allowlisted roots",
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_validate_links", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    path_obj = Path(file_path)
    if not path_obj.exists() or not path_obj.is_file():
        result = {
            "ok": False,
            "error": "file_path does not exist or is not a file",
            "provenance": build_provenance(config.wanatux_host, []),
        }
        audit_entry = build_audit_entry("docs_validate_links", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        links = _extract_links(path_obj)
        base_dir = path_obj.parent
        
        valid_links = []
        broken_links = []
        
        for link in links:
            # Resolve relative to the document's directory
            link_path = base_dir / link
            if link_path.exists():
                valid_links.append(link)
            else:
                broken_links.append(link)
        
        result = {
            "ok": True,
            "file_path": str(path_obj),
            "total_links": len(links),
            "valid_links": valid_links,
            "broken_links": broken_links,
            "has_broken_links": len(broken_links) > 0,
            "provenance": build_provenance(config.wanatux_host, []),
        }
        
    except Exception as e:
        result = {
            "ok": False,
            "error": str(e),
            "provenance": build_provenance(config.wanatux_host, []),
        }
    
    audit_entry = build_audit_entry("docs_validate_links", args, [], truncate_text(str(result), config.audit_preview_limit))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def move(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Move or rename a documentation file with safety checks."""
    source = args.get("source", "")
    destination = args.get("destination", "")
    confirm = args.get("confirm", False)
    dry_run = args.get("dry_run", True)
    
    commands = []
    
    if not source or not destination:
        result = {
            "ok": False,
            "error": "source and destination are required",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_move", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Validate paths
    source_path = Path(source).resolve()
    dest_path = Path(destination).resolve()
    
    if not is_path_allowed(source_path, config.repo_roots):
        result = {
            "ok": False,
            "error": f"Source path not allowed: {source_path}",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_move", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    if not is_path_allowed(dest_path, config.repo_roots):
        result = {
            "ok": False,
            "error": f"Destination path not allowed: {dest_path}",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_move", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Check if source exists
    if not source_path.exists():
        result = {
            "ok": False,
            "error": f"Source file does not exist: {source_path}",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_move", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Check if destination already exists
    if dest_path.exists():
        result = {
            "ok": False,
            "error": f"Destination already exists: {dest_path}",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_move", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Require confirmation for actual move
    if not confirm:
        result = {
            "ok": False,
            "error": "Operation requires confirm=true to proceed",
            "dry_run": True,
            "would_execute": f"mv {source_path} {dest_path}",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_move", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        if dry_run:
            result = {
                "ok": True,
                "dry_run": True,
                "message": f"DRY RUN: Would move {source_path} → {dest_path}",
                "source": str(source_path),
                "destination": str(dest_path),
                "provenance": build_provenance(config.wanatux_host, commands),
            }
        else:
            # Create destination directory if needed
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Move the file
            shutil.move(str(source_path), str(dest_path))
            commands.append(f"mv {source_path} {dest_path}")
            
            result = {
                "ok": True,
                "dry_run": False,
                "message": f"Successfully moved {source_path.name} → {dest_path}",
                "source": str(source_path),
                "destination": str(dest_path),
                "provenance": build_provenance(config.wanatux_host, commands),
            }
    except Exception as e:
        result = {
            "ok": False,
            "error": str(e),
            "provenance": build_provenance(config.wanatux_host, commands),
        }
    
    audit_entry = build_audit_entry("docs_move", args, commands, truncate_text(str(result), config.audit_preview_limit))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def merge(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple documentation files into one."""
    source_files = args.get("source_files", [])
    target = args.get("target", "")
    separator = args.get("separator", "\n\n---\n\n")
    confirm = args.get("confirm", False)
    dry_run = args.get("dry_run", True)
    
    commands = []
    
    if not source_files or not target:
        result = {
            "ok": False,
            "error": "source_files and target are required",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_merge", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Validate target path
    target_path = Path(target).resolve()
    if not is_path_allowed(target_path, config.repo_roots):
        result = {
            "ok": False,
            "error": f"Target path not allowed: {target_path}",
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_merge", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    # Validate all source files exist and are allowed
    validated_sources = []
    for src in source_files:
        src_path = Path(src).resolve()
        if not is_path_allowed(src_path, config.repo_roots):
            result = {
                "ok": False,
                "error": f"Source path not allowed: {src_path}",
                "provenance": build_provenance(config.wanatux_host, commands),
            }
            audit_entry = build_audit_entry("docs_merge", args, commands, truncate_text(str(result), config.audit_preview_limit))
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        
        if not src_path.exists():
            result = {
                "ok": False,
                "error": f"Source file does not exist: {src_path}",
                "provenance": build_provenance(config.wanatux_host, commands),
            }
            audit_entry = build_audit_entry("docs_merge", args, commands, truncate_text(str(result), config.audit_preview_limit))
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        
        validated_sources.append(src_path)
    
    # Require confirmation
    if not confirm:
        result = {
            "ok": False,
            "error": "Operation requires confirm=true to proceed",
            "dry_run": True,
            "would_merge": [str(s) for s in validated_sources],
            "into": str(target_path),
            "provenance": build_provenance(config.wanatux_host, commands),
        }
        audit_entry = build_audit_entry("docs_merge", args, commands, truncate_text(str(result), config.audit_preview_limit))
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        # Read all source files
        merged_content = []
        for src_path in validated_sources:
            with open(src_path, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                merged_content.append(content)
        
        # Join with separator
        final_content = separator.join(merged_content)
        
        if dry_run:
            preview = final_content[:500] + "..." if len(final_content) > 500 else final_content
            result = {
                "ok": True,
                "dry_run": True,
                "message": f"DRY RUN: Would merge {len(validated_sources)} files into {target_path}",
                "source_files": [str(s) for s in validated_sources],
                "target": str(target_path),
                "total_size": len(final_content),
                "preview": preview,
                "provenance": build_provenance(config.wanatux_host, commands),
            }
        else:
            # Create target directory if needed
            target_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write merged content
            with open(target_path, 'w', encoding='utf-8') as f:
                f.write(final_content)
            
            commands.append(f"merge {len(validated_sources)} files → {target_path}")
            
            result = {
                "ok": True,
                "dry_run": False,
                "message": f"Successfully merged {len(validated_sources)} files into {target_path.name}",
                "source_files": [str(s) for s in validated_sources],
                "target": str(target_path),
                "total_size": len(final_content),
                "provenance": build_provenance(config.wanatux_host, commands),
            }
    except Exception as e:
        result = {
            "ok": False,
            "error": str(e),
            "provenance": build_provenance(config.wanatux_host, commands),
        }
    
    audit_entry = build_audit_entry("docs_merge", args, commands, truncate_text(str(result), config.audit_preview_limit))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


DOCS_TOOLS = [
    {
        "name": "docs_list",
        "description": "List documentation files in a directory with metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "recursive": {"type": "boolean", "default": True},
                "extension_filter": {
                    "type": "array",
                    "items": {"type": "string"},
                    "default": [".md", ".txt"],
                },
            },
            "required": ["path"],
        },
        "handler": list_docs,
    },
    {
        "name": "docs_analyze",
        "description": "Analyze a documentation file for metadata, structure, and potential issues.",
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
        "name": "docs_validate_links",
        "description": "Validate that file links in a documentation file exist.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": {"type": "string"},
            },
            "required": ["file_path"],
        },
        "handler": validate_links,
    },
    {
        "name": "docs_move",
        "description": "Move or rename a documentation file (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "string"},
                "destination": {"type": "string"},
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
            "required": ["source", "destination"],
        },
        "handler": move,
    },
    {
        "name": "docs_merge",
        "description": "Merge multiple documentation files into one (confirm required).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source_files": {"type": "array", "items": {"type": "string"}},
                "target": {"type": "string"},
                "separator": {"type": "string", "default": "\n\n---\n\n"},
                "confirm": {"type": "boolean", "default": False},
                "dry_run": {"type": "boolean", "default": True},
            },
            "required": ["source_files", "target"],
        },
        "handler": merge,
    },
]
