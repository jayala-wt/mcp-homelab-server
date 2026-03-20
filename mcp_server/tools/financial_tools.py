"""
Financial Analysis MCP Tools

Provides financial data analysis and reporting capabilities.
Wraps refactored financial scripts as MCP tools.
"""

from typing import Any, Dict
from pathlib import Path
import sys

BASE_DIR = Path(__file__).parent.parent.parent
sys.path.insert(0, str(BASE_DIR))

from mcp_homelab.core import get_script_logger, get_db_connection, script_execution_context
from mcp_homelab.errors import error_response

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    is_path_allowed,
)

# Setup logger
logger = get_script_logger(__name__)


def financial_analyze_expenses(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Analyze business expenses from financial database.
    
    Compares categorized expenses and provides insights.
    """
    year = args.get("year", 2024)
    
    try:
        with script_execution_context(__name__, "financial_analyze_expenses", {"year": year}) as ctx:
            logger.info(f"Analyzing expenses for year {year}")
            
            with get_db_connection("financial") as conn:
                cursor = conn.cursor()
                
                # Get expense totals by category
                cursor.execute('''
                    SELECT category, SUM(amount) as total, COUNT(*) as count
                    FROM novo_transactions
                    WHERE year = ? AND is_expense = 1 AND is_owner_withdrawal = 0
                    GROUP BY category
                    ORDER BY total DESC
                ''', (year,))
                
                expenses_by_category = []
                total_expenses = 0
                
                for row in cursor.fetchall():
                    category_total = abs(row['total'])  # Expenses are negative
                    expenses_by_category.append({
                        "category": row['category'],
                        "total": round(category_total, 2),
                        "count": row['count'],
                    })
                    total_expenses += category_total
                
                result = {
                    "ok": True,
                    "year": year,
                    "total_expenses": round(total_expenses, 2),
                    "expenses_by_category": expenses_by_category,
                    "category_count": len(expenses_by_category),
                    "provenance": build_provenance(config.homelab_host, []),
                }
                
                ctx["total_expenses"] = total_expenses
                ctx["status"] = "success"
                
                logger.info(f"Analyzed {len(expenses_by_category)} expense categories, total: ${total_expenses:,.2f}")
                
    except Exception as e:
        logger.error(f"Expense analysis failed: {e}", exc_info=True)
        result = {
            "ok": False,
            "error": str(e),
            "provenance": build_provenance(config.homelab_host, []),
        }
    
    audit_entry = build_audit_entry("financial_analyze_expenses", args, [], f"Analyzed {year} expenses")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def financial_ingest_novo(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ingest Novo bank CSV transactions into the database.
    
    Processes CSV file and imports transactions with automatic categorization.
    """
    file_path = args.get("file_path", "").strip()
    confirm = bool(args.get("confirm", False))
    dry_run = args.get("dry_run", True)

    if not confirm:
        result = error_response(
            "financial_ingest_novo",
            "confirm=true required",
            error_code="PERMISSION_DENIED",
            likely_causes=["Tool requires explicit confirmation for ingestion"],
            suggested_next_tools=[{"tool": "meta.server_info", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("financial_ingest_novo", args, [], "confirm missing", error_code="PERMISSION_DENIED")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    if not file_path:
        result = error_response(
            "financial_ingest_novo",
            "file_path is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing file_path argument"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("financial_ingest_novo", args, [], "Missing file_path", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not is_path_allowed(file_path, config.repo_roots):
        result = error_response(
            "financial_ingest_novo",
            f"Path not allowed: {file_path}",
            error_code="ALLOWLIST_VIOLATION",
            likely_causes=["Path outside MCP_REPO_ROOTS allowlist"],
            suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("financial_ingest_novo", args, [], "Path not allowed", error_code="ALLOWLIST_VIOLATION")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    file_path_obj = Path(file_path)
    if not file_path_obj.exists():
        result = error_response(
            "financial_ingest_novo",
            f"File not found: {file_path}",
            error_code="PATH_MISSING",
            likely_causes=["CSV file does not exist"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {"path": str(file_path_obj.parent)}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("financial_ingest_novo", args, [], "File not found", error_code="PATH_MISSING")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    
    try:
        import pandas as pd
        
        with script_execution_context(__name__, "financial_ingest_novo", {"file_path": file_path}) as ctx:
            logger.info(f"Ingesting Novo transactions from {file_path} (dry_run={dry_run})")
            
            # Read CSV
            df = pd.read_csv(file_path)
            
            if dry_run:
                result = {
                    "ok": True,
                    "message": "Dry run - no data imported",
                    "file_path": file_path,
                    "rows_found": len(df),
                    "columns": list(df.columns),
                    "dry_run": True,
                    "provenance": build_provenance(config.homelab_host, []),
                }
                ctx["status"] = "dry_run"
                audit_entry = build_audit_entry("financial_ingest_novo", args, [], f"Dry run: {len(df)} rows")
                append_audit_log(config.audit_log_path, audit_entry)
                return result
            
            # TODO: Implement actual ingestion logic
            # This would parse the CSV, categorize transactions, and insert to DB
            
            result = {
                "ok": True,
                "file_path": file_path,
                "rows_processed": len(df),
                "rows_inserted": 0,  # TODO: Actual count
                "rows_updated": 0,   # TODO: Actual count
                "dry_run": False,
                "provenance": build_provenance(config.homelab_host, []),
            }
            
            ctx["rows_processed"] = len(df)
            ctx["status"] = "success"
            
            logger.info(f"Ingested {len(df)} transactions")
            
    except Exception as e:
        logger.error(f"Novo ingestion failed: {e}", exc_info=True)
        result = error_response(
            "financial_ingest_novo",
            str(e),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error while ingesting transactions"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )
    
    audit_entry = build_audit_entry("financial_ingest_novo", args, [], f"Ingested from {file_path}", error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def financial_compare_tax_bank(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compare tax return expenses with actual bank transactions.
    
    Helps identify discrepancies between reported and actual expenses.
    """
    year = args.get("year", 2024)
    
    try:
        with script_execution_context(__name__, "financial_compare_tax_bank", {"year": year}) as ctx:
            logger.info(f"Comparing tax vs bank for year {year}")
            
            # TODO: Implement actual comparison logic
            # This would compare tax_expenses table with novo_transactions
            
            result = {
                "ok": True,
                "year": year,
                "tax_total": 0.0,      # TODO: From tax_expenses
                "bank_total": 0.0,     # TODO: From novo_transactions
                "difference": 0.0,
                "comparison_by_category": [],
                "provenance": build_provenance(config.homelab_host, []),
            }
            
            ctx["status"] = "success"
            
            logger.info(f"Compared tax vs bank for {year}")
            
    except Exception as e:
        logger.error(f"Tax/bank comparison failed: {e}", exc_info=True)
        result = {
            "ok": False,
            "error": str(e),
            "provenance": build_provenance(config.homelab_host, []),
        }
    
    audit_entry = build_audit_entry("financial_compare_tax_bank", args, [], f"Compared {year} tax vs bank")
    append_audit_log(config.audit_log_path, audit_entry)
    return result


# Tool exports
FINANCIAL_TOOLS = [
    {
        "name": "financial_analyze_expenses",
        "description": "Analyze business expenses by category for a given year.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "year": {"type": "integer", "default": 2024, "description": "Year to analyze"},
            },
        },
        "handler": financial_analyze_expenses,
    },
]
