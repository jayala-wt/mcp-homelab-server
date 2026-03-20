"""
Maintenance MCP Tools

Provides Plex and Home Assistant maintenance capabilities.
Wraps the refactored maintenance scripts as MCP tools.
"""

from typing import Any, Dict
from pathlib import Path
import sys

# Add base dir for imports
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from mcp_homelab.core import get_script_logger, script_execution_context
from mcp_homelab.errors import error_response

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
)

# Setup logger
logger = get_script_logger(__name__)


def maintenance_refresh_playlists(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Refresh all Plex NextUp playlists.
    
    Refreshes main library playlists (Anime-NextUp, etc) and genre-based playlists.
    Creates language-aware anime playlists (Anime-JA-NextUp, Anime-EN-NextUp).
    """
    libraries = args.get("libraries", [])  # Empty = all libraries
    include_genres = args.get("include_genres", True)
    include_languages = args.get("include_languages", True)
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))

    if not confirm:
        result = error_response(
            "maintenance_refresh_playlists",
            "confirm=true required",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for playlist refresh"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        result["dry_run"] = True
        audit_entry = build_audit_entry("maintenance_refresh_playlists", args, [], "confirm missing", error_code="PERMISSION_DENIED")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        from scripts.maintenance.refresh_all_playlists_REFACTORED import refresh_all_playlists
        
        with script_execution_context(__name__, "maintenance_refresh_playlists", 
                                    {"libraries": libraries, "dry_run": dry_run}) as ctx:
            logger.info(f"Starting playlist refresh (libraries={libraries or 'all'}, dry_run={dry_run})")
            
            results = refresh_all_playlists(
                libraries=libraries,
                include_genres=include_genres,
                include_languages=include_languages,
                dry_run=dry_run
            )
            
            result = {
                "ok": True,
                "playlists_refreshed": results.get("refreshed", 0),
                "playlists_created": results.get("created", 0),
                "playlists_updated": results.get("updated", 0),
                "dry_run": dry_run,
                "provenance": build_provenance(config.homelab_host, []),
            }
            
            logger.info(f"Playlist refresh complete - refreshed: {results.get('refreshed', 0)}, created: {results.get('created', 0)}")
            
            audit_entry = build_audit_entry(
                "maintenance_refresh_playlists",
                args,
                [],
                f"Refreshed {results.get('refreshed', 0)} playlists",
            )
            append_audit_log(config.audit_log_path, audit_entry)

            return result
            
    except Exception as e:
        logger.error(f"Playlist refresh error: {e}")
        result = error_response(
            "maintenance_refresh_playlists",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while refreshing playlists"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("maintenance_refresh_playlists", args, [], "playlist refresh failed", error_code="UNKNOWN")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


def maintenance_cleanup_duplicates(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove duplicate episodes from Plex libraries.
    
    Automatically removes lowest quality duplicate episodes.
    Keeps highest quality/largest file size or newest version.
    """
    library = args.get("library")  # None = all libraries
    confirm = bool(args.get("confirm", False))
    dry_run = args.get("dry_run", True)  # Default to dry_run for safety

    if not confirm:
        result = error_response(
            "maintenance_cleanup_duplicates",
            "confirm=true required",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for cleanup"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        result["dry_run"] = True
        audit_entry = build_audit_entry("maintenance_cleanup_duplicates", args, [], "confirm missing", error_code="PERMISSION_DENIED")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        from scripts.maintenance.cleanup_duplicates_REFACTORED import cleanup_duplicates
        from homelab_portal.media.plex_connector import PlexConnector
        
        with script_execution_context(__name__, "maintenance_cleanup_duplicates", 
                                    {"library": library, "dry_run": dry_run}) as ctx:
            logger.info(f"Starting duplicate cleanup (library={library or 'all'}, dry_run={dry_run})")
            
            plex = PlexConnector()
            server = plex.get_server()
            
            results = cleanup_duplicates(server, lib_name=library, dry_run=dry_run)
            
            result = {
                "ok": True,
                "duplicates_found": results.get("found", 0),
                "duplicates_removed": results.get("removed", 0),
                "space_freed_mb": results.get("space_freed_mb", 0),
                "dry_run": dry_run,
                "provenance": build_provenance(config.homelab_host, []),
            }
            
            logger.info(f"Duplicate cleanup complete - found: {results.get('found', 0)}, removed: {results.get('removed', 0)}")
            
            audit_entry = build_audit_entry(
                "maintenance_cleanup_duplicates",
                args,
                [],
                f"Removed {results.get('removed', 0)} duplicate episodes",
            )
            append_audit_log(config.audit_log_path, audit_entry)

            return result
            
    except Exception as e:
        logger.error(f"Duplicate cleanup error: {e}")
        result = error_response(
            "maintenance_cleanup_duplicates",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while cleaning duplicates"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("maintenance_cleanup_duplicates", args, [], "cleanup duplicates failed", error_code="UNKNOWN")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


def maintenance_auto_adjust_refresh(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Auto-adjust Plex playlist refresh interval based on watch patterns.
    
    Analyzes watch frequency and recommends optimal refresh schedule.
    Can automatically update cron if confirmed.
    """
    apply_changes = args.get("apply_changes", False)
    confirm = bool(args.get("confirm", False))

    if not confirm:
        result = error_response(
            "maintenance_auto_adjust_refresh",
            "confirm=true required",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for auto-adjust"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("maintenance_auto_adjust_refresh", args, [], "confirm missing", error_code="PERMISSION_DENIED")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        from scripts.maintenance.auto_adjust_refresh_REFACTORED import (
            get_watch_stats,
            recommend_interval,
            get_current_cron,
            update_cron
        )
        
        with script_execution_context(__name__, "maintenance_auto_adjust_refresh", 
                                    {"apply_changes": apply_changes}) as ctx:
            logger.info(f"Analyzing watch patterns (apply_changes={apply_changes})")
            
            # Get watch statistics
            stats = get_watch_stats()
            if not stats or stats.get('total_watches', 0) == 0:
                result = error_response(
                    "maintenance_auto_adjust_refresh",
                    "No watch data available - run analyze_watch_patterns.py first",
                    error_code="UNKNOWN",
                    likely_causes=["Watch stats missing or empty"],
                    suggested_next_tools=[{"tool": "meta.health", "args": {}}],
                    host=config.homelab_host,
                )
                audit_entry = build_audit_entry("maintenance_auto_adjust_refresh", args, [], "no watch data", error_code="UNKNOWN")
                append_audit_log(config.audit_log_path, audit_entry)
                return result
            
            # Analyze and recommend
            total_watches = stats.get('total_watches', 0)
            days_analyzed = stats.get('days_analyzed', 30)
            avg_per_day = total_watches / days_analyzed if days_analyzed > 0 else 0
            
            recommended_schedule, interval_name, description = recommend_interval(avg_per_day)
            current_cron = get_current_cron()
            
            result = {
                "ok": True,
                "watch_stats": {
                    "total_watches": total_watches,
                    "days_analyzed": days_analyzed,
                    "avg_per_day": round(avg_per_day, 2)
                },
                "current_schedule": current_cron,
                "recommended_schedule": recommended_schedule,
                "recommended_description": description,
                "changes_applied": False,
                "provenance": build_provenance(config.homelab_host, []),
            }
            
            # Apply changes if requested
            if apply_changes:
                success = update_cron(recommended_schedule)
                result["changes_applied"] = success
                
                if success:
                    logger.info(f"Updated cron schedule to: {recommended_schedule}")
                    audit_entry = build_audit_entry("maintenance_auto_adjust_refresh", args, [], 
                                                  f"Updated refresh interval to {description}")
                    append_audit_log(config.audit_log_path, audit_entry)
                else:
                    result = error_response(
                        "maintenance_auto_adjust_refresh",
                        "Failed to update cron",
                        error_code="UNKNOWN",
                        likely_causes=["Cron update failed or insufficient permissions"],
                        suggested_next_tools=[{"tool": "meta.health", "args": {}}],
                        host=config.homelab_host,
                    )
                    result["changes_applied"] = False
                    audit_entry = build_audit_entry(
                        "maintenance_auto_adjust_refresh",
                        args,
                        [],
                        "cron update failed",
                        error_code="UNKNOWN",
                    )
                    append_audit_log(config.audit_log_path, audit_entry)
            else:
                audit_entry = build_audit_entry(
                    "maintenance_auto_adjust_refresh",
                    args,
                    [],
                    "auto adjust preview",
                )
                append_audit_log(config.audit_log_path, audit_entry)
            
            return result
            
    except Exception as e:
        logger.error(f"Auto-adjust error: {e}")
        result = error_response(
            "maintenance_auto_adjust_refresh",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while adjusting refresh schedule"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("maintenance_auto_adjust_refresh", args, [], "auto adjust failed", error_code="UNKNOWN")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


def maintenance_ha_devices(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Manage Home Assistant devices in local database.
    
    Query devices by area or domain, get capabilities, list areas.
    """
    action = args.get("action", "list_areas")  # list_areas, get_devices, get_by_domain
    area_id = args.get("area_id")
    domain = args.get("domain")
    
    try:
        from scripts.maintenance.ha_device_manager_REFACTORED import HADeviceManager
        
        with script_execution_context(__name__, "maintenance_ha_devices", 
                                    {"action": action, "area_id": area_id, "domain": domain}) as ctx:
            logger.info(f"Home Assistant device query (action={action})")
            
            manager = HADeviceManager()
            
            if action == "list_areas":
                areas = manager.get_areas()
                result = {
                    "ok": True,
                    "action": action,
                    "areas": areas,
                    "total_areas": len(areas),
                    "provenance": build_provenance(config.homelab_host, []),
                }
                
            elif action == "get_devices":
                devices = manager.get_devices_by_area(area_id)
                result = {
                    "ok": True,
                    "action": action,
                    "area_id": area_id,
                    "devices": devices,
                    "total_devices": len(devices),
                    "provenance": build_provenance(config.homelab_host, []),
                }
                
            elif action == "get_by_domain":
                if not domain:
                    return error_response(
                        "maintenance_ha_devices",
                        "domain parameter required for get_by_domain action",
                        error_code="INVALID_ARGS",
                        likely_causes=["Missing domain argument"],
                        suggested_next_tools=[{"tool": "maintenance_ha_devices", "args": {"action": "list_areas"}}],
                        host=config.homelab_host,
                    )
                devices = manager.get_devices_by_domain(domain)
                result = {
                    "ok": True,
                    "action": action,
                    "domain": domain,
                    "devices": devices,
                    "total_devices": len(devices),
                    "provenance": build_provenance(config.homelab_host, []),
                }
            else:
                return error_response(
                    "maintenance_ha_devices",
                    f"Unknown action: {action}. Valid actions: list_areas, get_devices, get_by_domain",
                    error_code="INVALID_ARGS",
                    likely_causes=["Action not in allowed set"],
                    suggested_next_tools=[{"tool": "maintenance_ha_devices", "args": {"action": "list_areas"}}],
                    host=config.homelab_host,
                )
            
            logger.info(f"HA device query complete - action: {action}")
            return result
            
    except Exception as e:
        logger.error(f"HA device query error: {e}")
        return error_response(
            "maintenance_ha_devices",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while querying HA devices"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )


# Tool exports
MAINTENANCE_TOOLS = [
    {
        "name": "maintenance_refresh_playlists",
        "description": "Refresh all Plex NextUp playlists including genre-based and language-aware playlists.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "libraries": {"type": "array", "items": {"type": "string"}, "description": "Specific libraries to refresh (default: all)"},
                "include_genres": {"type": "boolean", "default": True, "description": "Include genre-based playlists"},
                "include_languages": {"type": "boolean", "default": True, "description": "Include language-aware playlists"},
                "confirm": {"type": "boolean", "default": False, "description": "Required to run the refresh"},
                "dry_run": {"type": "boolean", "default": True, "description": "Preview without making changes"},
            },
        },
        "handler": maintenance_refresh_playlists,
    },
    {
        "name": "maintenance_cleanup_duplicates",
        "description": "Remove duplicate episodes from Plex libraries, keeping highest quality versions.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "library": {"type": "string", "description": "Specific library to clean (default: all)"},
                "confirm": {"type": "boolean", "default": False, "description": "Required to proceed with cleanup"},
                "dry_run": {"type": "boolean", "default": True, "description": "Preview duplicates without removing (RECOMMENDED)"},
            },
        },
        "handler": maintenance_cleanup_duplicates,
    },
    {
        "name": "maintenance_auto_adjust_refresh",
        "description": "Auto-adjust Plex playlist refresh interval based on watch patterns.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "apply_changes": {"type": "boolean", "default": False, "description": "Apply recommended schedule to cron (default: preview only)"},
                "confirm": {"type": "boolean", "default": False, "description": "Required to apply or preview refresh changes"},
            },
        },
        "handler": maintenance_auto_adjust_refresh,
    },
    {
        "name": "maintenance_ha_devices",
        "description": "Query Home Assistant devices from local database by area or domain.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list_areas", "get_devices", "get_by_domain"],
                    "default": "list_areas",
                    "description": "Action to perform"
                },
                "area_id": {"type": "string", "description": "Area ID for get_devices action"},
                "domain": {"type": "string", "description": "Domain for get_by_domain action (e.g., 'light', 'switch')"},
            },
        },
        "handler": maintenance_ha_devices,
    },
]
