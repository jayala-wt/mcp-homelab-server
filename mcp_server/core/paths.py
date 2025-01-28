"""Central registry for all paths used across scripts and MCP tools."""

from pathlib import Path

# Base directory
BASE_DIR = Path("/opt/homelab-panel")

# Data directories
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "mcp_homelab" / "logs"
CONFIG_DIR = BASE_DIR / "config"
SCRIPTS_DIR = BASE_DIR / "scripts"
TEMP_DIR = BASE_DIR / "temp_upload"

# Ensure critical directories exist
for directory in [DATA_DIR, LOGS_DIR, CONFIG_DIR, TEMP_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Database paths
DB_EMAIL_INTELLIGENCE = DATA_DIR / "email_intelligence.db"
DB_FINANCIAL = DATA_DIR / "financial.db"
DB_TODOS = DATA_DIR / "todos.db"
DB_HOME_ASSISTANT = DATA_DIR / "home_assistant.db"
DB_FINANCIAL_DOCS = DATA_DIR / "financial_docs.db"
DB_KNOWLEDGE = DATA_DIR / "knowledge.db"

# Config file paths
CONFIG_GMAIL = CONFIG_DIR / "gmail_credentials.json"
CONFIG_GMAIL_TOKEN = CONFIG_DIR / "gmail_token.json"
CONFIG_PLEX = CONFIG_DIR / "plex_config.json"
CONFIG_LINKEDIN = CONFIG_DIR / "linkedin_api.json"
CONFIG_LINKEDIN_TOKEN = CONFIG_DIR / "linkedin_token.json"

# Log file paths
LOG_AUDIT = LOGS_DIR / "mcp_audit.jsonl"
LOG_SCRIPT_EXECUTIONS = LOGS_DIR / "script_executions.jsonl"
