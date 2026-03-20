"""
Automation MCP Tools

Provides todo management and digest capabilities.
Wraps the refactored automation scripts as MCP tools.
"""

from typing import Any, Dict
from pathlib import Path
import sys

# Add base dir for imports
BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from mcp_homelab.core import get_script_logger, get_db_connection, script_execution_context
from mcp_homelab.errors import error_response

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
)

# Setup logger
logger = get_script_logger(__name__)


def automation_quick_wins_digest(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate and send daily quick wins digest email.
    
    Analyzes todos and financial context to create an actionable morning digest
    with LLM-powered insights using Ollama Qwen 2.5.
    
    Sends email via msmtp to configured address.
    """
    confirm = bool(args.get("confirm", False))
    dry_run = args.get("dry_run", True)
    email_to = args.get("email_to", "user@example.com")

    if not confirm:
        result = error_response(
            "automation_quick_wins_digest",
            "confirm=true required",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for digest generation"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("automation_quick_wins_digest", args, [], "confirm missing", error_code="PERMISSION_DENIED")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        from scripts.automation.daily_quick_wins_digest_REFACTORED import generate_digest, send_email
        
        with script_execution_context(__name__, "automation_quick_wins_digest", 
                                    {"dry_run": dry_run, "email_to": email_to}) as ctx:
            logger.info(f"Generating quick wins digest (email_to={email_to}, dry_run={dry_run})")
            
            # Generate digest content
            digest = generate_digest()
            
            if not digest:
                return {
                    "ok": False,
                    "error": "Failed to generate digest content",
                    "provenance": build_provenance(config.homelab_host, []),
                }
            
            # Send email if not dry run
            if not dry_run:
                success = send_email(digest, email_to)
                if not success:
                    return {
                        "ok": False,
                        "error": "Failed to send digest email",
                        "provenance": build_provenance(config.homelab_host, []),
                    }
            
            result = {
                "ok": True,
                "digest_generated": True,
                "email_sent": not dry_run,
                "email_to": email_to,
                "dry_run": dry_run,
                "provenance": build_provenance(config.homelab_host, []),
            }
            
            logger.info(f"Quick wins digest {'generated' if dry_run else 'sent'}")
            
            action = "Sent" if not dry_run else "Generated"
            audit_entry = build_audit_entry(
                "automation_quick_wins_digest",
                args,
                [],
                f"{action} quick wins digest to {email_to}",
            )
            append_audit_log(config.audit_log_path, audit_entry)
            
            return result
            
    except Exception as e:
        logger.error(f"Quick wins digest error: {e}")
        result = error_response(
            "automation_quick_wins_digest",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while generating digest"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("automation_quick_wins_digest", args, [], "digest failed", error_code="UNKNOWN")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


def automation_reprioritize_todos(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reprioritize todos based on age, patterns, and workload.
    
    Analyzes pending todos and adjusts priorities based on:
    - Age of todo
    - Completion patterns
    - Workload distribution
    - Due dates
    
    Can use LLM for intelligent prioritization decisions.
    """
    confirm = bool(args.get("confirm", False))
    dry_run = args.get("dry_run", True)
    use_llm = args.get("use_llm", False)
    model = args.get("model", "qwen2.5:7b")

    if not confirm:
        result = error_response(
            "automation_reprioritize_todos",
            "confirm=true required",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for reprioritization"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("automation_reprioritize_todos", args, [], "confirm missing", error_code="PERMISSION_DENIED")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        from scripts.automation.reprioritize_todos_nightly_REFACTORED import TodoReprioritizer
        
        with script_execution_context(__name__, "automation_reprioritize_todos", 
                                    {"dry_run": dry_run, "use_llm": use_llm, "model": model}) as ctx:
            logger.info(f"Starting todo reprioritization (dry_run={dry_run}, use_llm={use_llm})")
            
            reprioritizer = TodoReprioritizer(dry_run=dry_run)
            results = reprioritizer.reprioritize(use_llm=use_llm, model=model)
            
            result = {
                "ok": True,
                "todos_analyzed": results.get("analyzed", 0),
                "todos_updated": results.get("updated", 0),
                "updates": results.get("updates", []),
                "dry_run": dry_run,
                "use_llm": use_llm,
                "provenance": build_provenance(config.homelab_host, []),
            }
            
            logger.info(f"Reprioritization complete - analyzed: {results.get('analyzed', 0)}, updated: {results.get('updated', 0)}")
            
            audit_entry = build_audit_entry(
                "automation_reprioritize_todos",
                args,
                [],
                f"Reprioritized {results.get('updated', 0)} todos",
            )
            append_audit_log(config.audit_log_path, audit_entry)
            
            return result
            
    except Exception as e:
        logger.error(f"Reprioritization error: {e}")
        result = error_response(
            "automation_reprioritize_todos",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while reprioritizing todos"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("automation_reprioritize_todos", args, [], "reprioritize failed", error_code="UNKNOWN")
        append_audit_log(config.audit_log_path, audit_entry)
        return result


# Tool exports
AUTOMATION_TOOLS = [
    {
        "name": "automation_quick_wins_digest",
        "description": "Generate and send daily quick wins digest email with LLM-powered insights.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "email_to": {"type": "string", "default": "user@example.com", "description": "Email recipient"},
                "confirm": {"type": "boolean", "default": False, "description": "Required to generate or send digest"},
                "dry_run": {"type": "boolean", "default": True, "description": "Generate digest without sending email"},
            },
        },
        "handler": automation_quick_wins_digest,
    },
]
