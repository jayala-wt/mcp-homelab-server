"""
Tool metadata and categorization for MCP servers.

This module provides structured metadata for all MCP tools, enabling:
- Tool categorization for splitting into multiple specialized MCP servers
- Context information for each tool (databases used, file access, safety level)
- Dependency tracking between tools
- Resource requirements and access patterns

Future use: Split into separate MCP servers (git-server, docs-server, etc.)
"""

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class ToolContext:
    """Context information for an MCP tool."""
    
    # Tool categorization
    category: str  # git, lab, docs, scripts, email, financial, database
    subcategory: Optional[str] = None  # e.g., "analysis", "execution", "management"
    
    # Resource access
    databases_used: List[str] = field(default_factory=list)  # e.g., ["email_intelligence", "financial"]
    file_paths_accessed: List[str] = field(default_factory=list)  # Directories/files accessed
    external_services: List[str] = field(default_factory=list)  # e.g., ["systemd", "docker"]
    
    # Safety and permissions
    safety_level: str = "safe"  # safe, modify, destructive
    requires_confirmation: bool = False
    supports_dry_run: bool = False

    # Versioning
    version: str = "1.0.0"
    
    # Execution characteristics
    expected_duration: str = "fast"  # fast (<1s), medium (<30s), slow (>30s)
    can_run_parallel: bool = True
    idempotent: bool = True
    
    # Dependencies
    depends_on_tools: List[str] = field(default_factory=list)
    depends_on_config: List[str] = field(default_factory=list)  # e.g., ["repo_roots", "homelab_mode"]
    
    # Server assignment (for future splitting)
    suggested_server: Optional[str] = None  # "git-server", "docs-server", "homelab-server"
    
    # Additional metadata
    tags: List[str] = field(default_factory=list)
    notes: Optional[str] = None

    def __post_init__(self) -> None:
        if self.safety_level in {"modify", "destructive"} and not self.requires_confirmation:
            self.requires_confirmation = True


# Tool context registry
TOOL_CONTEXTS: Dict[str, ToolContext] = {
    # LAB TOOLS (3 tools)
    "lab_status": ToolContext(
        category="lab",
        subcategory="monitoring",
        external_services=["systemd", "docker"],
        safety_level="safe",
        expected_duration="fast",
        depends_on_config=["homelab_mode", "homelab_systemd_service"],
        suggested_server="homelab-server",
        tags=["read-only", "monitoring"],
    ),
    "lab_logs": ToolContext(
        category="lab",
        subcategory="monitoring",
        external_services=["systemd", "docker"],
        safety_level="safe",
        expected_duration="fast",
        depends_on_config=["homelab_mode", "homelab_systemd_service"],
        suggested_server="homelab-server",
        tags=["read-only", "logs"],
    ),
    "lab_restart": ToolContext(
        category="lab",
        subcategory="control",
        external_services=["systemd", "docker"],
        safety_level="modify",  # was "destructive" — gated from agent callable set; confirm+dry_run is sufficient
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="medium",
        can_run_parallel=False,
        idempotent=True,
        depends_on_config=["homelab_mode", "homelab_systemd_service"],
        suggested_server="homelab-server",
        tags=["write", "restart", "service-control"],
        notes="Uses sudo; restarts Flask/waitress only, NOT the MCP server process. Was 'destructive' but downgraded to 'modify' because destructive tools are hidden from VS Code agent.",
    ),
    "mcp_server_restart": ToolContext(
        category="lab",
        subcategory="control",
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="fast",
        can_run_parallel=False,
        idempotent=True,
        suggested_server="homelab-server",
        tags=["write", "restart", "mcp", "service-control"],
        notes=(
            "Restarts the MCP server process (python3 -m mcp_homelab.server), "
            "which is SEPARATE from homelab-panel systemd. Required after code "
            "changes in mcp_homelab/. Sends SIGTERM; VS Code respawns on next call."
        ),
    ),
    
    # MCP META TOOLS (7 tools)
    "mcp_list_metadata": ToolContext(
        category="mcp",
        subcategory="discovery",
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "metadata", "introspection"],
        notes="Query tool metadata and context information",
    ),
    "mcp_generate_tool": ToolContext(
        category="mcp",
        subcategory="generation",
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "code-generation", "onboarding"],
        notes="Generate new MCP tool templates (dry-run only, returns code)",
    ),
    "meta.server_info": ToolContext(
        category="mcp",
        subcategory="meta",
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "server", "introspection"],
        notes="Returns server version, protocol, and capability flags",
    ),
    "meta.health": ToolContext(
        category="mcp",
        subcategory="meta",
        external_services=["systemd", "docker"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "health", "diagnostics"],
        notes="Checks systemd access, docker socket, audit log, repo roots, and Python version",
    ),
    "meta.validate_config": ToolContext(
        category="mcp",
        subcategory="meta",
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "config", "validation"],
        notes="Validates config values and resolves paths with fix hints",
    ),
    "meta.discover_services": ToolContext(
        category="mcp",
        subcategory="meta",
        external_services=["systemd", "docker"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "discovery", "services"],
        notes="Lists matching systemd units and docker compose services",
    ),
    "meta.tool_lifecycle_report": ToolContext(
        category="mcp",
        subcategory="lifecycle",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "lifecycle", "analytics", "sigil"],
        notes="Phase 1 read-only lifecycle report: heat scoring, temperature, recommendations per tool",
    ),

    # EMAIL TOOLS
    "email_scan_gmail": ToolContext(
        category="email",
        subcategory="ingestion",
        databases_used=["email_intelligence"],
        external_services=["gmail-api"],
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="slow",
        suggested_server="email-server",
        tags=["gmail", "import", "email"],
        notes="Fetches Gmail messages into raw_emails (default no keyword prefilter; supports keyword/custom Gmail query overrides). Requires valid OAuth token in config/oauth_token.json",
    ),
    
    # FINANCIAL TOOLS (1 tool)
    "financial_analyze_expenses": ToolContext(
        category="financial",
        subcategory="analysis",
        databases_used=["financial"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="financial-server",
        tags=["analysis", "expenses", "reporting"],
        notes="Analyzes business expenses by category",
    ),
    
    # EMAIL TOOLS - Additional
    "email_analyze_topics": ToolContext(
        category="email",
        subcategory="analysis",
        databases_used=["email_intelligence"],
        safety_level="safe",
        expected_duration="slow",
        suggested_server="email-server",
        tags=["ai", "topic-modeling", "lda", "nmf"],
        notes="Discovers topics in non-financial emails using LDA and NMF",
    ),
    "email_sender_baseline": ToolContext(
        category="email",
        subcategory="analysis",
        databases_used=["email_intelligence"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="email-server",
        tags=["analysis", "sender", "cleanup", "triage"],
        notes="Ranks recent high-volume senders and bulk/spam-like candidates using heuristics",
    ),
    "email_investigate_sender_family": ToolContext(
        category="email",
        subcategory="analysis",
        databases_used=["email_intelligence"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="email-server",
        tags=["analysis", "family", "debugging", "newspaper", "triage"],
        notes="Generic sender-family investigation/debug tool that returns central taxonomy routing and family-specific operational insights (currently supports citi, covetrus, linkedin, michaels, samsclub, stocktwits, and wayfair presets)",
    ),
    "email_apply_routing_rules": ToolContext(
        category="email",
        subcategory="triage",
        databases_used=["email_intelligence"],
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="fast",
        suggested_server="email-server",
        tags=["routing", "classification", "newspaper", "cleanup", "rules"],
        notes="Applies and persists centralized internal newspaper/garbage routing classifications for supported sender families",
    ),
    "email_newspaper_generate": ToolContext(
        category="email",
        subcategory="analysis",
        databases_used=["email_intelligence"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="email-server",
        tags=["newspaper", "digest", "newsroom", "reporting", "triage"],
        notes="Generates the internal newspaper/newsroom view from stored classifications only without recomputing heuristics",
    ),
    "email_sender_decision_set": ToolContext(
        category="email",
        subcategory="triage",
        databases_used=["email_intelligence"],
        safety_level="modify",
        requires_confirmation=True,
        expected_duration="fast",
        suggested_server="email-server",
        tags=["sender", "decisions", "cleanup", "triage"],
        notes="Stores internal sender-level cleanup decisions (garbage/review/keep) without changing Gmail",
    ),
    "email_global_triage_accelerator": ToolContext(
        category="email",
        subcategory="triage",
        databases_used=["email_intelligence"],
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="medium",
        suggested_server="email-server",
        tags=["triage", "global-rules", "sender-purity", "bulk", "analysis"],
        notes="Global triage accelerator: conservative cross-sender rules, sender purity analysis, mixed/new family queues; optional write mode for bulk classification and pure-sender decisions",
    ),
    
    # AUTOMATION TOOLS (1 tool)
    "automation_quick_wins_digest": ToolContext(
        category="automation",
        subcategory="digest",
        databases_used=["todos", "financial"],
        external_services=["ollama", "msmtp"],
        safety_level="modify",
        requires_confirmation=False,
        supports_dry_run=True,
        expected_duration="slow",
        suggested_server="automation-server",
        tags=["ai", "email", "digest", "todos"],
        notes="Generates and sends daily quick wins digest with LLM insights",
    ),
    
    # MAINTENANCE TOOLS (4 tools)
    "maintenance_refresh_playlists": ToolContext(
        category="maintenance",
        subcategory="plex",
        external_services=["plex"],
        safety_level="modify",
        requires_confirmation=False,
        supports_dry_run=True,
        expected_duration="slow",
        suggested_server="homelab-server",
        tags=["plex", "playlists", "media"],
        notes="Refreshes all Plex NextUp playlists (main, genre, language-aware)",
    ),
    "maintenance_cleanup_duplicates": ToolContext(
        category="maintenance",
        subcategory="plex",
        external_services=["plex"],
        safety_level="modify",  # was "destructive" — has confirm+dry_run gating; destructive hides from agent
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="slow",
        suggested_server="homelab-server",
        tags=["plex", "cleanup", "media"],
        notes="Removes duplicate episodes from Plex libraries (keeps highest quality)",
    ),
    "maintenance_auto_adjust_refresh": ToolContext(
        category="maintenance",
        subcategory="optimization",
        external_services=["cron"],
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=False,
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["plex", "cron", "optimization"],
        notes="Auto-adjusts Plex playlist refresh interval based on watch patterns",
    ),
    "maintenance_ha_devices": ToolContext(
        category="maintenance",
        subcategory="home-assistant",
        databases_used=["home_assistant"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["home-assistant", "devices", "iot"],
        notes="Queries Home Assistant devices by area or domain",
    ),
    
    # OSF TOOLS (8 tools)
    "osf_get_project": ToolContext(
        category="research",
        subcategory="api",
        external_services=["osf.io"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="research-server",
        tags=["read-only", "api", "osf"],
    ),
    "osf_list_components": ToolContext(
        category="research",
        subcategory="api",
        external_services=["osf.io"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="research-server",
        tags=["read-only", "api", "osf"],
    ),
    "osf_get_abstract": ToolContext(
        category="research",
        subcategory="api",
        external_services=["osf.io"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="research-server",
        tags=["read-only", "api", "osf"],
    ),
    "osf_list_files": ToolContext(
        category="research",
        subcategory="file-operations",
        external_services=["osf.io"],
        file_paths_accessed=["config/osf_config.json"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="research-server",
        depends_on_config=["osf_config.json"],
        tags=["read-only", "api", "osf", "authenticated"],
        notes="Requires personal access token in config/osf_config.json",
    ),
    "osf_download_file": ToolContext(
        category="research",
        subcategory="file-operations",
        external_services=["osf.io"],
        file_paths_accessed=["config/osf_config.json"],
        safety_level="safe",
        expected_duration="medium",
        suggested_server="research-server",
        depends_on_config=["osf_config.json"],
        tags=["read-only", "download", "api", "osf", "authenticated"],
        notes="Downloads files from OSF to local filesystem using WaterButler API",
    ),
    "osf_upload_file": ToolContext(
        category="research",
        subcategory="file-operations",
        external_services=["osf.io"],
        file_paths_accessed=["config/osf_config.json"],
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="medium",
        suggested_server="research-server",
        depends_on_config=["osf_config.json"],
        tags=["write", "upload", "api", "osf", "authenticated", "guarded"],
        notes="Uploads files to OSF projects with dry-run safety",
    ),
    "osf_create_component": ToolContext(
        category="research",
        subcategory="project-management",
        external_services=["osf.io"],
        file_paths_accessed=["config/osf_config.json"],
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="fast",
        suggested_server="research-server",
        depends_on_config=["osf_config.json"],
        tags=["write", "create", "api", "osf", "authenticated", "guarded"],
        notes="Creates child components under parent OSF projects",
    ),
    "osf_update_project": ToolContext(
        category="research",
        subcategory="project-management",
        external_services=["osf.io"],
        file_paths_accessed=["config/osf_config.json"],
        safety_level="modify",
        requires_confirmation=True,
        supports_dry_run=True,
        expected_duration="fast",
        suggested_server="research-server",
        depends_on_config=["osf_config.json"],
        tags=["write", "update", "api", "osf", "authenticated", "guarded"],
        notes="Updates OSF project metadata (title, description, tags, public status)",
    ),
    
    # WORDPRESS TOOLS (4 tools)
    "wordpress_create_post": ToolContext(
        category="publishing",
        subcategory="content",
        external_services=["example-wordpress.org"],
        safety_level="modify",
        supports_dry_run=True,
        expected_duration="fast",
        suggested_server="publishing-server",
        tags=["write", "api", "wordpress", "content-management"],
        notes="Creates or updates WordPress posts via REST API (confirm required)",
    ),
    "wordpress_get_post": ToolContext(
        category="publishing",
        subcategory="content",
        external_services=["example-wordpress.org"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="publishing-server",
        tags=["read-only", "api", "wordpress"],
    ),
    "wordpress_list_posts": ToolContext(
        category="publishing",
        subcategory="content",
        external_services=["example-wordpress.org"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="publishing-server",
        tags=["read-only", "api", "wordpress"],
    ),

    # TODO TOOLS (5 tools)
    "todo.session_status": ToolContext(
        category="todo",
        subcategory="session",
        databases_used=["todos"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "recommendations"],
    ),
    "todo.capture": ToolContext(
        category="todo",
        subcategory="capture",
        databases_used=["todos"],
        safety_level="modify",
        supports_dry_run=True,
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["write", "dedupe", "capture"],
        notes="Deterministic capture with dedupe and ISO due dates",
    ),
    "todo.list": ToolContext(
        category="todo",
        subcategory="query",
        databases_used=["todos"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "list"],
    ),
    "todo.complete": ToolContext(
        category="todo",
        subcategory="completion",
        databases_used=["todos"],
        safety_level="modify",
        supports_dry_run=True,
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["write", "completion"],
    ),
    "todo.update": ToolContext(
        category="todo",
        subcategory="update",
        databases_used=["todos"],
        safety_level="modify",
        supports_dry_run=True,
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["write", "update", "reschedule"],
        notes="Update todo fields: title, description, category, priority, due_date, when_bucket, context, status",
    ),
    "todo.review_next": ToolContext(
        category="todo",
        subcategory="review",
        databases_used=["todos"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "recommendations"],
    ),

    # KNOWLEDGE TOOLS (5 tools)
    "knowledge.status": ToolContext(
        category="database",
        subcategory="status",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "knowledge"],
    ),
    "knowledge.search": ToolContext(
        category="database",
        subcategory="search",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "fts"],
    ),
    "knowledge.bootstrap_context": ToolContext(
        category="database",
        subcategory="bootstrap",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "bootstrap"],
    ),
    "knowledge.context_mark": ToolContext(
        category="database",
        subcategory="tracking",
        databases_used=["knowledge"],
        safety_level="modify",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["write", "context", "sigil", "confidence"],
    ),
    "knowledge.reindex": ToolContext(
        category="database",
        subcategory="maintenance",
        databases_used=["knowledge"],
        safety_level="modify",
        supports_dry_run=True,
        expected_duration="medium",
        can_run_parallel=False,
        suggested_server="database-server",
        tags=["write", "indexing"],
    ),
    "knowledge.ocr_queue": ToolContext(
        category="database",
        subcategory="ocr",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "ocr", "queue"],
        notes="List or manage documents pending OCR processing.",
    ),

    # DEVLOOP TOOLS (5 tools)
    # devloop writes are append-only notebook entries — no confirm required
    "devloop.run_start": ToolContext(
        category="database",
        subcategory="devloop",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["write", "devloop"],
    ),
    "devloop.add_artifact": ToolContext(
        category="database",
        subcategory="devloop",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["write", "devloop"],
    ),
    "devloop.log": ToolContext(
        category="database",
        subcategory="devloop",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["write", "devloop"],
        notes="Single-call devloop logger (creates run if needed, adds artifact).",
    ),
    "devloop.latest": ToolContext(
        category="database",
        subcategory="devloop",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "devloop"],
    ),
    "devloop.search": ToolContext(
        category="database",
        subcategory="devloop",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "devloop"],
    ),

    # MEMORY TOOLS (2 tools) — Unified Memory Recall
    "memory.recall": ToolContext(
        category="database",
        subcategory="memory",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "memory", "devloop", "knowledge"],
        notes="Unified search across devloop + knowledge.db with scoring/ranking.",
    ),
    "memory.confirm": ToolContext(
        category="database",
        subcategory="memory",
        databases_used=["knowledge"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["write", "memory", "reinforcement"],
        notes="Reinforce recalled results: access_count++, warm→hot promotion.",
    ),

    # DECISION CAPTURE TOOLS (3 tools) – Epistemic Trace System
    "decision_capture.log": ToolContext(
        category="database",
        subcategory="epistemic-trace",
        databases_used=["decision_captures"],
        safety_level="modify",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["write", "epistemic-trace", "decision-capture"],
        notes="Log an epistemic activation event (miss/win/ambiguous). Phase 1 of Epistemic Trace.",
    ),
    "decision_capture.list": ToolContext(
        category="database",
        subcategory="epistemic-trace",
        databases_used=["decision_captures"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "epistemic-trace", "decision-capture"],
        notes="List recent decision captures with filtering.",
    ),
    "decision_capture.metrics": ToolContext(
        category="database",
        subcategory="epistemic-trace",
        databases_used=["decision_captures"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="database-server",
        tags=["read-only", "epistemic-trace", "decision-capture", "metrics"],
        notes="TAR/TMR/RR epistemic activation metrics.",
    ),

    # JOB TOOLS (4 tools)
    "job_tailor": ToolContext(
        category="job-pipeline",
        subcategory="tailoring",
        databases_used=["jobs_pipeline"],
        file_paths_accessed=["personal/resumes/"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "job-search", "resume", "cover-letter"],
        notes="Returns job + profile + resume context for the calling model to generate tailored docs",
    ),
    "job_list_top": ToolContext(
        category="job-pipeline",
        subcategory="query",
        databases_used=["jobs_pipeline"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "job-search", "browse"],
        notes="Browse top-scoring jobs from the pipeline DB",
    ),
    "job_update_status": ToolContext(
        category="job-pipeline",
        subcategory="management",
        databases_used=["jobs_pipeline"],
        safety_level="modify",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["write", "job-search", "status"],
        notes="Update job status in the pipeline (shortlisted, applying, applied, interview, etc.)",
    ),
    "job_save_output": ToolContext(
        category="job-pipeline",
        subcategory="output",
        databases_used=["jobs_pipeline"],
        safety_level="modify",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["write", "job-search", "resume", "cover-letter"],
        notes="Persist generated resume/cover letter to DB so it appears in the job detail UI",
    ),

    # CALENDAR TOOLS (3 tools)
    "calendar.status": ToolContext(
        category="calendar",
        subcategory="status",
        external_services=["google-calendar"],
        safety_level="safe",
        expected_duration="fast",
        suggested_server="homelab-server",
        tags=["read-only", "calendar"],
    ),
    "calendar.create_event_from_todo": ToolContext(
        category="calendar",
        subcategory="sync",
        databases_used=["todos"],
        external_services=["google-calendar"],
        safety_level="modify",
        supports_dry_run=True,
        expected_duration="medium",
        suggested_server="homelab-server",
        tags=["write", "calendar", "sync"],
        notes="Creates a Google Calendar event for a due-dated todo",
    ),
    "calendar.sync_due_dates": ToolContext(
        category="calendar",
        subcategory="sync",
        databases_used=["todos"],
        external_services=["google-calendar"],
        safety_level="modify",
        supports_dry_run=True,
        expected_duration="medium",
        suggested_server="homelab-server",
        tags=["write", "calendar", "sync"],
        notes="Bulk creates Google Calendar events for due-dated todos",
    ),
}


# Category groupings for server splitting
SERVER_CATEGORIES = {
    "homelab-server": {
        "categories": ["lab", "mcp", "maintenance"],
        "description": "Homelab service monitoring, control, MCP meta-tools, and Plex/HA maintenance",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category in ["lab", "mcp", "maintenance"]],
    },
    # Future servers
    "email-server": {
        "categories": ["email"],
        "description": "Email intelligence and analysis",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category == "email"],
    },
    "financial-server": {
        "categories": ["financial"],
        "description": "Financial data analysis and reporting",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category == "financial"],
    },
    "automation-server": {
        "categories": ["automation"],
        "description": "Automated task management and digest generation",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category == "automation"],
    },
    "todo-server": {
        "categories": ["todo", "calendar"],
        "description": "Home-only todo capture, review, and calendar sync",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category in ["todo", "calendar"]],
    },
    "research-server": {
        "categories": ["research"],
        "description": "OSF research publication management and API access",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category == "research"],
    },
    "publishing-server": {
        "categories": ["publishing"],
        "description": "WordPress content management and blog publishing",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category == "publishing"],
    },
    "database-server": {
        "categories": ["database"],
        "description": "Database operations and maintenance",
        "tools": [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category == "database"],
    },
}


def get_tool_context(tool_name: str) -> Optional[ToolContext]:
    """Get context for a specific tool."""
    return TOOL_CONTEXTS.get(tool_name)


def get_tool_version(tool_name: str) -> str:
    """Get version string for a specific tool."""
    context = get_tool_context(tool_name)
    if context:
        return context.version
    return "unknown"


def get_tools_by_category(category: str) -> List[str]:
    """Get all tool names in a category."""
    return [name for name, ctx in TOOL_CONTEXTS.items() if ctx.category == category]


def get_tools_by_server(server_name: str) -> List[str]:
    """Get all tools assigned to a specific server."""
    return SERVER_CATEGORIES.get(server_name, {}).get("tools", [])


def get_tool_summary() -> Dict[str, Any]:
    """Get summary of all tools and their contexts."""
    summary = {
        "total_tools": len(TOOL_CONTEXTS),
        "by_category": {},
        "by_safety_level": {},
        "by_server": {},
    }
    
    for name, ctx in TOOL_CONTEXTS.items():
        # By category
        if ctx.category not in summary["by_category"]:
            summary["by_category"][ctx.category] = []
        summary["by_category"][ctx.category].append(name)
        
        # By safety level
        if ctx.safety_level not in summary["by_safety_level"]:
            summary["by_safety_level"][ctx.safety_level] = []
        summary["by_safety_level"][ctx.safety_level].append(name)
        
        # By server
        if ctx.suggested_server:
            if ctx.suggested_server not in summary["by_server"]:
                summary["by_server"][ctx.suggested_server] = []
            summary["by_server"][ctx.suggested_server].append(name)
    
    return summary


def validate_tool_contexts() -> Dict[str, Any]:
    """Validate that all registered tools have contexts."""
    from .tools.lab_tools import LAB_TOOLS
    from .tools.mcp_meta_tools import MCP_META_TOOLS
    from .tools.email_tools import EMAIL_TOOLS
    from .tools.email_actions import EMAIL_ACTION_TOOLS
    from .tools.financial_tools import FINANCIAL_TOOLS
    from .tools.automation_tools import AUTOMATION_TOOLS
    from .tools.maintenance_tools import MAINTENANCE_TOOLS
    from .tools.osf_tools import OSF_TOOLS
    from .tools.wordpress_tools import WORDPRESS_TOOLS
    from .tools.todo_tools import TODO_TOOLS, CALENDAR_TOOLS
    from .tools.knowledge_tools import KNOWLEDGE_TOOLS
    from .tools.devloop_tools import DEVLOOP_TOOLS
    from .tools.decision_capture_tools import DECISION_CAPTURE_TOOLS
    from .tools.memory_tools import MEMORY_TOOLS
    from .tools.job_tools import JOB_TOOLS

    all_registered_tools = (
        LAB_TOOLS + MCP_META_TOOLS + EMAIL_TOOLS + FINANCIAL_TOOLS +
        AUTOMATION_TOOLS + MAINTENANCE_TOOLS + OSF_TOOLS + WORDPRESS_TOOLS +
        TODO_TOOLS + CALENDAR_TOOLS + KNOWLEDGE_TOOLS + DEVLOOP_TOOLS +
        DECISION_CAPTURE_TOOLS + EMAIL_ACTION_TOOLS + MEMORY_TOOLS +
        JOB_TOOLS
    )
    
    registered_names = {tool["name"] for tool in all_registered_tools}
    context_names = set(TOOL_CONTEXTS.keys())
    
    missing_contexts = registered_names - context_names
    extra_contexts = context_names - registered_names
    
    return {
        "valid": len(missing_contexts) == 0 and len(extra_contexts) == 0,
        "registered_tools": len(registered_names),
        "contextualized_tools": len(context_names),
        "missing_contexts": list(missing_contexts),
        "extra_contexts": list(extra_contexts),
    }
