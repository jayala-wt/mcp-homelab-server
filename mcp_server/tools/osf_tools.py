"""
OSF (Open Science Framework) API tools for research publication management.
"""
import json
import os
from pathlib import Path
from typing import Any, Dict, List

from mcp_homelab.core import get_script_logger

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    run_command,
)

# Setup logger
logger = get_script_logger(__name__)

# Import authenticated tools
try:
    from .osf_tools_auth import (
        osf_list_files,
        osf_download_file,
        osf_upload_file,
        osf_create_component,
        osf_update_project
    )
except ImportError:
    logger.warning("osf_tools_auth not available, some tools will be disabled")
    osf_list_files = None
    osf_download_file = None
    osf_upload_file = None
    osf_create_component = None
    osf_update_project = None


def _load_osf_config():
    """Load OSF configuration including API token."""
    config_path = Path("/opt/homelab-panel/config/osf_config.json")
    if not config_path.exists():
        return None
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load OSF config: {e}")
        return None


def _get_auth_header():
    """Get authorization header with personal access token."""
    osf_config = _load_osf_config()
    if not osf_config or 'personal_access_token' not in osf_config:
        return None
    
    token = osf_config['personal_access_token']
    return f"Bearer {token}"


def osf_get_project(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get OSF project details by project ID.
    
    Args:
        config: MCP server configuration
        args: Tool arguments containing project_id
        
    Returns:
        Project details including title, description, contributors, etc.
    """
    project_id = args.get("project_id", "").strip()
    
    if not project_id:
        result = {
            "ok": False,
            "error": "project_id is required",
            "provenance": build_provenance(config.homelab_host, []),
        }
        audit_entry = build_audit_entry("osf_get_project", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    logger.info(f"Fetching OSF project: {project_id}")
    
    # Call OSF API using run_command for consistency
    cmd_result = run_command(
        ["curl", "-s", f"https://api.osf.io/v2/nodes/{project_id}/"],
        timeout_sec=10,
        output_limit=config.output_limit,
    )
    
    commands = [cmd_result]
    
    if cmd_result["exit_code"] != 0:
        result = {
            "ok": False,
            "error": f"OSF API request failed: {cmd_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_get_project", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        data = json.loads(cmd_result["stdout"])
        
        if "errors" in data:
            error_detail = data["errors"][0].get("detail", "Unknown OSF API error")
            result = {
                "ok": False,
                "error": error_detail,
                "provenance": build_provenance(config.homelab_host, commands),
            }
            audit_entry = build_audit_entry("osf_get_project", args, commands, "")
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        
        attrs = data.get("data", {}).get("attributes", {})
        
        result = {
            "ok": True,
            "project_id": project_id,
            "title": attrs.get("title", ""),
            "description": attrs.get("description", ""),
            "date_created": attrs.get("date_created", ""),
            "date_modified": attrs.get("date_modified", ""),
            "category": attrs.get("category", ""),
            "public": attrs.get("public", False),
            "tags": attrs.get("tags", []),
            "api_url": f"https://api.osf.io/v2/nodes/{project_id}/",
            "web_url": f"https://osf.io/{project_id}/",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        
        audit_entry = build_audit_entry("osf_get_project", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        logger.info(f"Successfully fetched OSF project: {attrs.get('title', project_id)}")
        return result
        
    except json.JSONDecodeError as e:
        result = {
            "ok": False,
            "error": f"Failed to parse OSF API response: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_get_project", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    except Exception as e:
        result = {
            "ok": False,
            "error": f"Unexpected error: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_get_project", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


def osf_list_components(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    List all components (children) of an OSF project.
    
    Args:
        config: MCP server configuration
        args: Tool arguments containing project_id
        
    Returns:
        List of child components with their IDs, titles, and descriptions
    """
    project_id = args.get("project_id", "").strip()
    
    if not project_id:
        result = {
            "ok": False,
            "error": "project_id is required",
            "provenance": build_provenance(config.homelab_host, []),
        }
        audit_entry = build_audit_entry("osf_list_components", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    logger.info(f"Listing components for OSF project: {project_id}")
    
    # Call OSF API for children
    cmd_result = run_command(
        ["curl", "-s", f"https://api.osf.io/v2/nodes/{project_id}/children/"],
        timeout_sec=10,
        output_limit=config.output_limit,
    )
    
    commands = [cmd_result]
    
    if cmd_result["exit_code"] != 0:
        result = {
            "ok": False,
            "error": f"OSF API request failed: {cmd_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_list_components", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        data = json.loads(cmd_result["stdout"])
        
        if "errors" in data:
            error_detail = data["errors"][0].get("detail", "Unknown OSF API error")
            result = {
                "ok": False,
                "error": error_detail,
                "provenance": build_provenance(config.homelab_host, commands),
            }
            audit_entry = build_audit_entry("osf_list_components", args, commands, "")
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        
        components = []
        for item in data.get("data", []):
            attrs = item.get("attributes", {})
            component_id = item.get("id", "")
            
            components.append({
                "id": component_id,
                "title": attrs.get("title", ""),
                "description": attrs.get("description", ""),
                "category": attrs.get("category", ""),
                "date_created": attrs.get("date_created", ""),
                "date_modified": attrs.get("date_modified", ""),
                "web_url": f"https://osf.io/{component_id}/"
            })
        
        result = {
            "ok": True,
            "project_id": project_id,
            "component_count": len(components),
            "components": components,
            "provenance": build_provenance(config.homelab_host, commands),
        }
        
        audit_entry = build_audit_entry("osf_list_components", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        logger.info(f"Found {len(components)} components for project {project_id}")
        return result
        
    except json.JSONDecodeError as e:
        result = {
            "ok": False,
            "error": f"Failed to parse OSF API response: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_list_components", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    except Exception as e:
        result = {
            "ok": False,
            "error": f"Unexpected error: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_list_components", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


def osf_get_abstract(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get just the abstract/description for an OSF project.
    Lightweight version of osf_get_project for quick abstract retrieval.
    
    Args:
        config: MCP server configuration
        args: Tool arguments containing project_id
        
    Returns:
        Title and description/abstract
    """
    project_id = args.get("project_id", "").strip()
    
    if not project_id:
        result = {
            "ok": False,
            "error": "project_id is required",
            "provenance": build_provenance(config.homelab_host, []),
        }
        audit_entry = build_audit_entry("osf_get_abstract", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    logger.info(f"Fetching abstract for OSF project: {project_id}")
    
    # Call OSF API
    cmd_result = run_command(
        ["curl", "-s", f"https://api.osf.io/v2/nodes/{project_id}/"],
        timeout_sec=10,
        output_limit=config.output_limit,
    )
    
    commands = [cmd_result]
    
    if cmd_result["exit_code"] != 0:
        result = {
            "ok": False,
            "error": f"OSF API request failed: {cmd_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_get_abstract", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        data = json.loads(cmd_result["stdout"])
        
        if "errors" in data:
            error_detail = data["errors"][0].get("detail", "Unknown OSF API error")
            result = {
                "ok": False,
                "error": error_detail,
                "provenance": build_provenance(config.homelab_host, commands),
            }
            audit_entry = build_audit_entry("osf_get_abstract", args, commands, "")
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        
        attrs = data.get("data", {}).get("attributes", {})
        
        result = {
            "ok": True,
            "project_id": project_id,
            "title": attrs.get("title", ""),
            "abstract": attrs.get("description", ""),
            "web_url": f"https://osf.io/{project_id}/",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        
        audit_entry = build_audit_entry("osf_get_abstract", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        logger.info(f"Successfully fetched abstract for: {attrs.get('title', project_id)}")
        return result
        
    except json.JSONDecodeError as e:
        result = {
            "ok": False,
            "error": f"Failed to parse OSF API response: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_get_abstract", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    except Exception as e:
        result = {
            "ok": False,
            "error": f"Unexpected error: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_get_abstract", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


# Tool definitions for MCP server
OSF_TOOLS = [
    {
        "name": "osf_get_project",
        "description": "Get OSF project details by project ID. Returns title, description, contributors, dates, and URLs.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {
                    "type": "string",
                    "description": "OSF project ID (e.g., 'bvy3q' from https://osf.io/bvy3q/)"
                }
            },
            "required": ["project_id"]
        },
        "handler": osf_get_project
    },
    {
        "name": "osf_list_components",
        "description": "List all child components of an OSF project. Returns array of components with IDs, titles, and descriptions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {
                    "type": "string",
                    "description": "Parent OSF project ID"
                }
            },
            "required": ["project_id"]
        },
        "handler": osf_list_components
    },
    {
        "name": "osf_get_abstract",
        "description": "Get abstract/description for an OSF project. Lightweight version of osf_get_project for quick abstract retrieval.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {
                    "type": "string",
                    "description": "OSF project ID"
                }
            },
            "required": ["project_id"]
        },
        "handler": osf_get_abstract
    }
]

# Add authenticated tools if available
if osf_list_files:
    OSF_TOOLS.append({
        "name": "osf_list_files",
        "description": "List all files in an OSF project or component. Returns files with names, sizes, download URLs. Requires authentication.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {
                    "type": "string",
                    "description": "OSF project or component ID"
                },
                "provider": {
                    "type": "string",
                    "description": "Storage provider (default: osfstorage)",
                    "default": "osfstorage"
                }
            },
            "required": ["project_id"]
        },
        "handler": osf_list_files
    })

if osf_download_file:
    OSF_TOOLS.append({
        "name": "osf_download_file",
        "description": "Download files from OSF projects to local filesystem using WaterButler API. Supports downloading individual files with progress tracking.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {
                    "type": "string",
                    "description": "OSF project or component ID"
                },
                "file_id": {
                    "type": "string",
                    "description": "File ID from osf_list_files output"
                },
                "output_path": {
                    "type": "string",
                    "description": "Local path to save downloaded file"
                },
                "provider": {
                    "type": "string",
                    "description": "Storage provider (default: osfstorage)",
                    "default": "osfstorage"
                }
            },
            "required": ["project_id", "file_id", "output_path"]
        },
        "handler": osf_download_file
    })

if osf_upload_file:
    OSF_TOOLS.append({
        "name": "osf_upload_file",
        "description": "Upload files to OSF projects with dry-run safety. Creates new files or updates existing files (new version). Requires confirm flag for execution.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {
                    "type": "string",
                    "description": "OSF project or component ID"
                },
                "file_path": {
                    "type": "string",
                    "description": "Local file path to upload"
                },
                "osf_path": {
                    "type": "string",
                    "description": "Target path in OSF (default: root, use folder/file.txt for subdirs)",
                    "default": ""
                },
                "provider": {
                    "type": "string",
                    "description": "Storage provider (default: osfstorage)",
                    "default": "osfstorage"
                },
                "confirm": {
                    "type": "boolean",
                    "description": "Required to execute upload (default: false)",
                    "default": False
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Preview without uploading (default: true)",
                    "default": True
                }
            },
            "required": ["project_id", "file_path"]
        },
        "handler": osf_upload_file
    })

if osf_create_component:
    OSF_TOOLS.append({
        "name": "osf_create_component",
        "description": "Create child components/sub-projects under parent OSF project. Supports dry-run preview showing component structure before creation.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "parent_id": {
                    "type": "string",
                    "description": "Parent project or component ID"
                },
                "title": {
                    "type": "string",
                    "description": "Component title"
                },
                "category": {
                    "type": "string",
                    "description": "Component category (project, data, analysis, etc.)",
                    "default": "project"
                },
                "description": {
                    "type": "string",
                    "description": "Optional component description",
                    "default": ""
                },
                "confirm": {
                    "type": "boolean",
                    "description": "Required to execute creation (default: false)",
                    "default": False
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Preview without creating (default: true)",
                    "default": True
                }
            },
            "required": ["parent_id", "title"]
        },
        "handler": osf_create_component
    })

if osf_update_project:
    OSF_TOOLS.append({
        "name": "osf_update_project",
        "description": "Update OSF project metadata (title, description, tags, public status). Dry-run shows current vs new values before applying changes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "project_id": {
                    "type": "string",
                    "description": "Project or component ID to update"
                },
                "title": {
                    "type": "string",
                    "description": "New project title (optional)"
                },
                "description": {
                    "type": "string",
                    "description": "New project description (optional)"
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of tags (optional)"
                },
                "public": {
                    "type": "boolean",
                    "description": "Make project public (optional)"
                },
                "confirm": {
                    "type": "boolean",
                    "description": "Required to execute update (default: false)",
                    "default": False
                },
                "dry_run": {
                    "type": "boolean",
                    "description": "Preview without updating (default: true)",
                    "default": True
                }
            },
            "required": ["project_id"]
        },
        "handler": osf_update_project
    })
