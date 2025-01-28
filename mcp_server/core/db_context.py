"""Database connection context manager with centralized path management."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from .paths import (
    DB_EMAIL_INTELLIGENCE,
    DB_FINANCIAL,
    DB_TODOS,
    DB_HOME_ASSISTANT,
    DB_FINANCIAL_DOCS,
    DB_KNOWLEDGE,
)

# Central database registry
DATABASES = {
    "email_intelligence": str(DB_EMAIL_INTELLIGENCE),
    "financial": str(DB_FINANCIAL),
    "todos": str(DB_TODOS),
    "home_assistant": str(DB_HOME_ASSISTANT),
    "financial_docs": str(DB_FINANCIAL_DOCS),
    "knowledge": str(DB_KNOWLEDGE),
}


@contextmanager
def get_db_connection(db_name: str) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager for database connections with automatic cleanup.
    
    Features:
    - Centralized database path management
    - Automatic commit on success
    - Automatic rollback on error
    - Proper connection cleanup
    - Row factory for dict-like access
    
    Args:
        db_name: Database name from DATABASES registry
        
    Yields:
        sqlite3.Connection: Database connection
        
    Raises:
        ValueError: If database name not recognized
        
    Example:
        >>> with get_db_connection("email_intelligence") as conn:
        ...     cursor = conn.cursor()
        ...     cursor.execute("SELECT * FROM emails LIMIT 10")
        ...     for row in cursor.fetchall():
        ...         print(row['subject'])  # Row factory enables column access
    """
    if db_name not in DATABASES:
        raise ValueError(
            f"Unknown database: '{db_name}'. "
            f"Available databases: {list(DATABASES.keys())}"
        )
    
    db_path = DATABASES[db_name]
    
    # Ensure parent directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Create connection
    conn = sqlite3.connect(db_path)
    
    # Enable row factory for dict-like access
    conn.row_factory = sqlite3.Row
    
    try:
        yield conn
        conn.commit()  # Auto-commit on success
    except Exception:
        conn.rollback()  # Auto-rollback on error
        raise
    finally:
        conn.close()  # Always cleanup


def get_db_path(db_name: str) -> str:
    """
    Get database path by name.
    
    Args:
        db_name: Database name from DATABASES registry
        
    Returns:
        str: Absolute path to database file
        
    Raises:
        ValueError: If database name not recognized
        
    Example:
        >>> path = get_db_path("financial")
        >>> print(path)
        /opt/homelab-panel/data/financial.db
    """
    if db_name not in DATABASES:
        raise ValueError(
            f"Unknown database: '{db_name}'. "
            f"Available databases: {list(DATABASES.keys())}"
        )
    return DATABASES[db_name]
