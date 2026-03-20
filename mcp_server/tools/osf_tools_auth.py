"""
OSF (Open Science Framework) Authenticated Tools - File Operations & Project Management

These tools require a personal access token stored in config/osf_config.json
"""
import json
import os
from pathlib import Path
from typing import Any, Dict

from mcp_homelab.core import get_script_logger
from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    run_command,
)

logger = get_script_logger(__name__)


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


def osf_list_files(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    List all files in an OSF project or component.
    
    Args:
        config: MCP server configuration
        args: Tool arguments containing project_id and optional provider
        
    Returns:
        List of files with names, sizes, URLs, and download links
    """
    project_id = args.get("project_id", "").strip()
    provider = args.get("provider", "osfstorage")
    
    if not project_id:
        result = {
            "ok": False,
            "error": "project_id is required",
            "provenance": build_provenance(config.homelab_host, []),
        }
        audit_entry = build_audit_entry("osf_list_files", args, [], "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    logger.info(f"Listing files for OSF project: {project_id} (provider: {provider})")
    
    cmd_result = run_command(
        ["curl", "-s", f"https://api.osf.io/v2/nodes/{project_id}/files/{provider}/"],
        timeout_sec=15,
        output_limit=config.output_limit,
    )
    
    commands = [cmd_result]
    
    if cmd_result["exit_code"] != 0:
        result = {
            "ok": False,
            "error": f"OSF API request failed: {cmd_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_list_files", args, commands, "")
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
            audit_entry = build_audit_entry("osf_list_files", args, commands, "")
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        
        files = []
        for item in data.get("data", []):
            attrs = item.get("attributes", {})
            links = item.get("links", {})
            
            files.append({
                "id": item.get("id", ""),
                "name": attrs.get("name", ""),
                "kind": attrs.get("kind", ""),  # file or folder
                "size": attrs.get("size"),
                "modified": attrs.get("date_modified", ""),
                "download_url": links.get("download", ""),
                "web_url": links.get("html", ""),
                "path": attrs.get("materialized_path", "")
            })
        
        result = {
            "ok": True,
            "project_id": project_id,
            "provider": provider,
            "file_count": len(files),
            "files": files,
            "provenance": build_provenance(config.homelab_host, commands),
        }
        
        audit_entry = build_audit_entry("osf_list_files", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        logger.info(f"Found {len(files)} files in project {project_id}")
        return result
        
    except json.JSONDecodeError as e:
        result = {
            "ok": False,
            "error": f"Failed to parse OSF API response: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_list_files", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    except Exception as e:
        result = {
            "ok": False,
            "error": f"Unexpected error: {e}",
            "provenance": build_provenance(config.homelab_host, commands),
        }
        audit_entry = build_audit_entry("osf_list_files", args, commands, "")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


def osf_download_file(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Download files from OSF projects to local filesystem using WaterButler API.
    
    Args:
        config: MCP server configuration
        args: Tool arguments
            - project_id: OSF project/component ID
            - file_id: File ID from osf_list_files (or file path)
            - output_path: Local path to save file
            - provider: Storage provider (default: osfstorage)
    
    Returns:
        Result dictionary with download status and file info
    """
    project_id = args.get("project_id")
    file_id = args.get("file_id")
    output_path = args.get("output_path")
    provider = args.get("provider", "osfstorage")
    
    if not all([project_id, file_id, output_path]):
        return {
            "ok": False,
            "error": "Missing required parameters: project_id, file_id, output_path",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    auth_header = _get_auth_header()
    if not auth_header:
        return {
            "ok": False,
            "error": "OSF authentication not configured. Add personal_access_token to config/osf_config.json",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    # First, get file metadata to get download URL
    api_url = f"https://api.osf.io/v2/files/{file_id}/"
    cmd_result = run_command(
        ["curl", "-s", "-H", f"Authorization: {auth_header}", api_url],
        timeout_sec=10,
        output_limit=config.output_limit
    )
    
    commands = [cmd_result]
    
    if cmd_result["exit_code"] != 0:
        return {
            "ok": False,
            "error": f"Failed to get file metadata: {cmd_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    try:
        file_data = json.loads(cmd_result["stdout"])
        download_url = file_data["data"]["links"]["download"]
        file_name = file_data["data"]["attributes"]["name"]
        file_size = file_data["data"]["attributes"]["size"]
    except (KeyError, json.JSONDecodeError) as e:
        return {
            "ok": False,
            "error": f"Failed to parse file metadata: {e}",
            "raw_response": cmd_result["stdout"],
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    # Download the file
    download_result = run_command(
        ["curl", "-L", "-H", f"Authorization: {auth_header}", download_url, "-o", output_path],
        timeout_sec=60,
        output_limit=config.output_limit
    )
    
    commands.append(download_result)
    
    if download_result["exit_code"] != 0:
        return {
            "ok": False,
            "error": f"Failed to download file: {download_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    # Verify file was created
    if not os.path.exists(output_path):
        return {
            "ok": False,
            "error": "File download completed but file not found at output path",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    actual_size = os.path.getsize(output_path)
    audit_entry = build_audit_entry("osf_download_file", args, commands, "")
    append_audit_log(config.audit_log_path, audit_entry)
    
    return {
        "ok": True,
        "message": f"Downloaded {file_name} ({file_size} bytes)",
        "file_name": file_name,
        "file_size": file_size,
        "actual_size": actual_size,
        "output_path": output_path,
        "provenance": build_provenance(config.homelab_host, [cmd, download_cmd])
    }


def osf_upload_file(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Upload files to OSF projects with dry-run safety.
    Creates new files or updates existing files (new version).
    
    Args:
        config: MCP server configuration
        args: Tool arguments
            - project_id: OSF project/component ID
            - file_path: Local file path to upload
            - osf_path: Target path in OSF (default: root, use folder/file.txt for subdirs)
            - provider: Storage provider (default: osfstorage)
            - confirm: Requires confirmation (default: False)
            - dry_run: Preview without uploading (default: True)
    
    Returns:
        Result dictionary with upload status
    """
    project_id = args.get("project_id")
    file_path = args.get("file_path")
    osf_path = args.get("osf_path", "")
    provider = args.get("provider", "osfstorage")
    confirm = args.get("confirm", False)
    dry_run = args.get("dry_run", True)
    
    if not all([project_id, file_path]):
        return {
            "ok": False,
            "error": "Missing required parameters: project_id, file_path",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    if not confirm and not dry_run:
        return {
            "ok": False,
            "error": "This operation requires confirmation. Set confirm=true",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    auth_header = _get_auth_header()
    if not auth_header:
        return {
            "ok": False,
            "error": "OSF authentication not configured. Add personal_access_token to config/osf_config.json",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    # Verify local file exists
    if not os.path.exists(file_path):
        return {
            "ok": False,
            "error": f"Local file not found: {file_path}",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    
    # Construct upload URL - WaterButler format
    # New file (no osf_path): PUT to root with ?kind=file&name=filename
    # Existing file (osf_path is a file ID): PUT to /{file_id}/?kind=file
    if osf_path:
        upload_url = f"https://files.osf.io/v1/resources/{project_id}/providers/{provider}/{osf_path.lstrip('/')}?kind=file"
        display_path = osf_path
    else:
        upload_url = f"https://files.osf.io/v1/resources/{project_id}/providers/{provider}/?kind=file&name={file_name}"
        display_path = f"/{file_name} (new)"

    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "message": "DRY RUN - Would upload file to OSF",
            "preview": {
                "file_name": file_name,
                "file_size": file_size,
                "local_path": file_path,
                "project_id": project_id,
                "osf_path": display_path,
                "provider": provider,
                "upload_url": upload_url
            },
            "next_step": "Run with confirm=true and dry_run=false to execute upload",
            "provenance": build_provenance(config.homelab_host, [])
        }

    # Execute upload
    upload_result = run_command(
        ["curl", "-X", "PUT", "-H", f"Authorization: {auth_header}",
         "--data-binary", f"@{file_path}", upload_url],
        timeout_sec=120,
        output_limit=config.output_limit
    )
    
    commands = [upload_result]
    
    if upload_result["exit_code"] != 0:
        return {
            "ok": False,
            "error": f"Failed to upload file: {upload_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    try:
        response = json.loads(upload_result["stdout"])
        uploaded_file_id = response["data"]["id"]
        uploaded_name = response["data"]["attributes"]["name"]
    except (KeyError, json.JSONDecodeError) as e:
        return {
            "ok": False,
            "error": f"Upload may have succeeded but failed to parse response: {e}",
            "raw_response": upload_result["stdout"],
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    audit_entry = build_audit_entry("osf_upload_file", args, commands, "")
    append_audit_log(config.audit_log_path, audit_entry)
    
    return {
        "ok": True,
        "message": f"Uploaded {uploaded_name} ({file_size} bytes)",
        "file_id": uploaded_file_id,
        "file_name": uploaded_name,
        "file_size": file_size,
        "project_id": project_id,
        "osf_path": base_path,
        "provenance": build_provenance(config.homelab_host, [upload_cmd])
    }


def osf_create_component(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create child components/sub-projects under parent OSF project.
    
    Args:
        config: MCP server configuration
        args: Tool arguments
            - parent_id: Parent project/component ID
            - title: Component title
            - category: Component category (project, data, etc.)
            - description: Optional description
            - confirm: Requires confirmation (default: False)
            - dry_run: Preview without creating (default: True)
    
    Returns:
        Result dictionary with component info
    """
    parent_id = args.get("parent_id")
    title = args.get("title")
    category = args.get("category", "project")
    description = args.get("description", "")
    confirm = args.get("confirm", False)
    dry_run = args.get("dry_run", True)
    
    if not all([parent_id, title]):
        return {
            "ok": False,
            "error": "Missing required parameters: parent_id, title",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    if not confirm and not dry_run:
        return {
            "ok": False,
            "error": "This operation requires confirmation. Set confirm=true",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    auth_header = _get_auth_header()
    if not auth_header:
        return {
            "ok": False,
            "error": "OSF authentication not configured. Add personal_access_token to config/osf_config.json",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    # Construct request payload
    payload = {
        "data": {
            "type": "nodes",
            "attributes": {
                "title": title,
                "category": category,
                "description": description
            }
        }
    }
    
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "message": "DRY RUN - Would create component",
            "preview": {
                "parent_id": parent_id,
                "title": title,
                "category": category,
                "description": description,
                "payload": payload
            },
            "next_step": "Run with confirm=true and dry_run=false to execute creation",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    # Execute creation
    api_url = f"https://api.osf.io/v2/nodes/{parent_id}/children/"
    payload_json = json.dumps(payload)
    create_result = run_command(
        ["curl", "-X", "POST", "-H", f"Authorization: {auth_header}", 
         "-H", "Content-Type: application/json", "-d", payload_json, api_url],
        timeout_sec=10,
        output_limit=config.output_limit
    )
    
    commands = [create_result]
    
    if create_result["exit_code"] != 0:
        return {
            "ok": False,
            "error": f"Failed to create component: {create_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    try:
        response = json.loads(create_result["stdout"])
        component_id = response["data"]["id"]
        component_title = response["data"]["attributes"]["title"]
        component_url = response["data"]["links"]["html"]
    except (KeyError, json.JSONDecodeError) as e:
        return {
            "ok": False,
            "error": f"Component may have been created but failed to parse response: {e}",
            "raw_response": create_result["stdout"],
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    audit_entry = build_audit_entry("osf_create_component", args, commands, "")
    append_audit_log(config.audit_log_path, audit_entry)
    
    return {
        "ok": True,
        "message": f"Created component: {component_title}",
        "component_id": component_id,
        "title": component_title,
        "category": category,
        "url": component_url,
        "provenance": build_provenance(config.homelab_host, commands)
    }


def osf_update_project(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Update OSF project metadata (title, description, tags, public status).
    
    Args:
        config: MCP server configuration
        args: Tool arguments
            - project_id: Project/component ID
            - title: New title (optional)
            - description: New description (optional)
            - tags: List of tags (optional)
            - public: Make public (optional, boolean)
            - confirm: Requires confirmation (default: False)
            - dry_run: Preview without updating (default: True)
    
    Returns:
        Result dictionary with update status
    """
    project_id = args.get("project_id")
    title = args.get("title")
    description = args.get("description")
    tags = args.get("tags")
    public = args.get("public")
    confirm = args.get("confirm", False)
    dry_run = args.get("dry_run", True)
    
    if not project_id:
        return {
            "ok": False,
            "error": "Missing required parameter: project_id",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    if not any([title, description, tags is not None, public is not None]):
        return {
            "ok": False,
            "error": "No update parameters provided. Specify title, description, tags, or public",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    if not confirm and not dry_run:
        return {
            "ok": False,
            "error": "This operation requires confirmation. Set confirm=true",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    auth_header = _get_auth_header()
    if not auth_header:
        return {
            "ok": False,
            "error": "OSF authentication not configured. Add personal_access_token to config/osf_config.json",
            "provenance": build_provenance(config.homelab_host, [])
        }
    
    # Get current project data for comparison
    api_url = f"https://api.osf.io/v2/nodes/{project_id}/"
    get_result = run_command(
        ["curl", "-s", "-H", f"Authorization: {auth_header}", api_url],
        timeout_sec=10,
        output_limit=config.output_limit
    )
    
    commands = [get_result]
    
    if get_result["exit_code"] != 0:
        return {
            "ok": False,
            "error": f"Failed to get current project data: {get_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    try:
        current_data = json.loads(get_result["stdout"])
        current_attrs = current_data["data"]["attributes"]
    except (KeyError, json.JSONDecodeError) as e:
        return {
            "ok": False,
            "error": f"Failed to parse current project data: {e}",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    # Build update payload
    attributes = {}
    if title is not None:
        attributes["title"] = title
    if description is not None:
        attributes["description"] = description
    if tags is not None:
        attributes["tags"] = tags
    if public is not None:
        attributes["public"] = public
    
    payload = {
        "data": {
            "type": "nodes",
            "id": project_id,
            "attributes": attributes
        }
    }
    
    # Show current vs new values
    changes = {}
    if title is not None:
        changes["title"] = {"current": current_attrs.get("title"), "new": title}
    if description is not None:
        changes["description"] = {"current": current_attrs.get("description"), "new": description}
    if tags is not None:
        changes["tags"] = {"current": current_attrs.get("tags"), "new": tags}
    if public is not None:
        changes["public"] = {"current": current_attrs.get("public"), "new": public}
    
    if dry_run:
        return {
            "ok": True,
            "dry_run": True,
            "message": "DRY RUN - Would update project metadata",
            "preview": {
                "project_id": project_id,
                "changes": changes,
                "payload": payload
            },
            "next_step": "Run with confirm=true and dry_run=false to execute update",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    # Execute update
    payload_json = json.dumps(payload)
    update_result = run_command(
        ["curl", "-X", "PATCH", "-H", f"Authorization: {auth_header}", 
         "-H", "Content-Type: application/json", "-d", payload_json, api_url],
        timeout_sec=10,
        output_limit=config.output_limit
    )
    
    commands.append(update_result)
    
    if update_result["exit_code"] != 0:
        return {
            "ok": False,
            "error": f"Failed to update project: {update_result.get('stderr', 'Unknown error')}",
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    try:
        response = json.loads(update_result["stdout"])
        updated_title = response["data"]["attributes"]["title"]
    except (KeyError, json.JSONDecodeError) as e:
        return {
            "ok": False,
            "error": f"Update may have succeeded but failed to parse response: {e}",
            "raw_response": update_result["stdout"],
            "provenance": build_provenance(config.homelab_host, commands)
        }
    
    audit_entry = build_audit_entry("osf_update_project", args, commands, "")
    append_audit_log(config.audit_log_path, audit_entry)
    
    return {
        "ok": True,
        "message": f"Updated project: {updated_title}",
        "project_id": project_id,
        "changes": changes,
        "provenance": build_provenance(config.homelab_host, commands)
    }


# Tool definitions for authenticated OSF operations
OSF_AUTH_TOOLS = [
    {
        "name": "osf_list_files",
        "description": "List all files in an OSF project or component. Returns files with names, sizes, download URLs.",
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
    },
    {
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
    },
    {
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
    },
    {
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
    },
    {
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
    }
]
