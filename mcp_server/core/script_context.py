"""Execution context manager for scripts with automatic timing and audit logging."""

import json
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Optional

from .script_logger import get_script_logger
from .paths import LOG_SCRIPT_EXECUTIONS


@contextmanager
def script_execution_context(
    script_name: str,
    operation: str,
    metadata: Optional[Dict[str, Any]] = None,
    audit_log: bool = True
) -> Generator[Dict[str, Any], None, None]:
    """
    Execution context for scripts with automatic logging, timing, and audit trail.
    
    Features:
    - Automatic timing of execution
    - Success/failure tracking
    - Error capture with full stack trace
    - Audit logging to JSONL file
    - Context dict for passing results
    
    Args:
        script_name: Name of the script (usually __name__)
        operation: Operation being performed (e.g., "scan_emails", "analyze_expenses")
        metadata: Optional metadata to include in audit log
        audit_log: Enable audit logging (default: True)
        
    Yields:
        dict: Context dictionary for storing results/metrics
        
    Example:
        >>> with script_execution_context(__name__, "scan_emails") as ctx:
        ...     # Your script logic here
        ...     emails_processed = 150
        ...     ctx["emails_processed"] = emails_processed
        ...     ctx["new_subscriptions"] = 5
        ... # Automatically logs timing and results
    """
    logger = get_script_logger(script_name)
    start_time = time.time()
    
    # Initialize context
    context = {
        "script": script_name,
        "operation": operation,
        "start_time": datetime.utcnow().isoformat(),
        "metadata": metadata or {},
        "results": {},
        "status": None,
        "error": None,
        "duration_seconds": None
    }
    
    logger.info(f"Starting {operation}")
    
    try:
        # Yield results dict for script to populate
        yield context["results"]
        
        # Success path
        duration = time.time() - start_time
        context["duration_seconds"] = round(duration, 3)
        context["status"] = "success"
        
        logger.info(f"Completed {operation} in {duration:.2f}s")
        
    except Exception as e:
        # Error path
        duration = time.time() - start_time
        context["duration_seconds"] = round(duration, 3)
        context["status"] = "failed"
        context["error"] = str(e)
        context["error_type"] = type(e).__name__
        
        logger.error(f"Failed {operation} after {duration:.2f}s: {e}", exc_info=True)
        raise
        
    finally:
        # Always log to audit trail
        if audit_log:
            _append_audit_log(context)


def _append_audit_log(context: Dict[str, Any]) -> None:
    """Append execution context to audit log file."""
    try:
        # Ensure log directory exists
        LOG_SCRIPT_EXECUTIONS.parent.mkdir(parents=True, exist_ok=True)
        
        # Append to JSONL file
        with open(LOG_SCRIPT_EXECUTIONS, 'a', encoding='utf-8') as f:
            f.write(json.dumps(context, sort_keys=True))
            f.write('\n')
            
    except Exception as e:
        # Don't fail script if audit logging fails
        logger = get_script_logger("script_context")
        logger.warning(f"Failed to write audit log: {e}")
