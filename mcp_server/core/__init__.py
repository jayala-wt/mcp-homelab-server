"""Core utilities shared across scripts and MCP tools."""

from .db_context import get_db_connection, get_db_path, DATABASES
from .script_logger import get_script_logger
from .script_context import script_execution_context
from .paths import (
    BASE_DIR,
    DATA_DIR,
    LOGS_DIR,
    CONFIG_DIR,
    SCRIPTS_DIR,
    DB_EMAIL_INTELLIGENCE,
    DB_FINANCIAL,
    DB_TODOS,
    DB_HOME_ASSISTANT,
    DB_FINANCIAL_DOCS,
    DB_KNOWLEDGE,
    CONFIG_GMAIL,
    CONFIG_PLEX,
    CONFIG_LINKEDIN,
)

__all__ = [
    # Database
    'get_db_connection',
    'get_db_path',
    'DATABASES',
    
    # Logging
    'get_script_logger',
    
    # Execution context
    'script_execution_context',
    
    # Paths
    'BASE_DIR',
    'DATA_DIR',
    'LOGS_DIR',
    'CONFIG_DIR',
    'SCRIPTS_DIR',
    'DB_EMAIL_INTELLIGENCE',
    'DB_FINANCIAL',
    'DB_TODOS',
    'DB_HOME_ASSISTANT',
    'DB_FINANCIAL_DOCS',
    'DB_KNOWLEDGE',
    'CONFIG_GMAIL',
    'CONFIG_PLEX',
    'CONFIG_LINKEDIN',
]
