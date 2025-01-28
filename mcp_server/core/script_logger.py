"""Centralized logging configuration for scripts and MCP tools."""

import logging
import sys
from pathlib import Path
from typing import Optional

from .paths import LOGS_DIR

# Singleton storage for loggers
_loggers = {}


def get_script_logger(
    name: str,
    level: int = logging.INFO,
    log_to_file: bool = True,
    log_dir: Optional[Path] = None
) -> logging.Logger:
    """
    Get or create a configured logger for a script or MCP tool.
    
    Features:
    - Singleton pattern (same logger for same name)
    - Console output (stderr)
    - Optional file logging with rotation
    - Consistent formatting across all logs
    - Prevents duplicate handlers
    
    Args:
        name: Logger name (usually __name__)
        level: Logging level (default: INFO)
        log_to_file: Enable file logging (default: True)
        log_dir: Override default log directory
        
    Returns:
        logging.Logger: Configured logger instance
        
    Example:
        >>> logger = get_script_logger(__name__)
        >>> logger.info("Processing started")
        >>> logger.error("Failed to connect", exc_info=True)
    """
    # Return existing logger if already configured
    if name in _loggers:
        return _loggers[name]
    
    # Create new logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # Prevent duplicate logs
    
    # Console handler (stderr keeps MCP stdio stdout clean for JSON-RPC)
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_to_file:
        # Create safe filename from logger name
        safe_name = name.replace('/', '_').replace('.', '_').replace(':', '_')
        
        # Use provided log_dir or default
        target_dir = log_dir or LOGS_DIR
        target_dir.mkdir(parents=True, exist_ok=True)
        
        log_file = target_dir / f"{safe_name}.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # More verbose in files
        file_formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(pathname)s:%(lineno)d]',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Store logger
    _loggers[name] = logger
    
    return logger


def reset_loggers():
    """Reset all loggers (useful for testing)."""
    for logger in _loggers.values():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)
    _loggers.clear()
