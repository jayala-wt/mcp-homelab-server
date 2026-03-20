import re
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mcp_homelab.core import get_db_connection, get_db_path, get_script_logger
from mcp_homelab.errors import error_response

from .util import (
    append_audit_log,
    build_audit_entry,
    build_provenance,
    is_path_allowed,
    run_command,
    truncate_text,
)

# Setup logger
logger = get_script_logger(__name__)


def _sanitize_fts_query(query: str) -> str:
    """Best-effort cleanup for user-entered queries that break FTS syntax.

    Keeps alphanumerics/quotes/spaces, replaces common filename/path punctuation
    with spaces, and normalizes whitespace.
    """
    cleaned = re.sub(r"[^\w\s\"']", " ", query or "")
    cleaned = cleaned.replace("_", " ")
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# Critique Response: External Audit Logging (NotebookLM #2)
# The AI cannot grade its own homework. Every tool_call is logged server-side
# so compliance is DERIVED, not self-reported.
# ---------------------------------------------------------------------------

def _log_tool_call(conn, tool_name: str, args: Dict[str, Any],
                   result_count: int = 0, doc_ids: Optional[List[str]] = None) -> None:
    """Write a tamper-proof heartbeat to tool_audit_log.
    Called by the MCP server, NOT by the AI model."""
    import hashlib, json
    from datetime import datetime, timezone
    try:
        args_hash = hashlib.sha256(
            json.dumps(args, sort_keys=True, default=str).encode()
        ).hexdigest()[:32]
        conn.execute(
            """INSERT INTO tool_audit_log
               (timestamp, tool_name, args_hash, category_filter, query_text,
                result_count, doc_ids_json, session_hint)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                tool_name,
                args_hash,
                args.get("category", ""),
                (args.get("query") or "")[:200],
                result_count,
                json.dumps(doc_ids or []),
                "",  # session_hint filled by middleware if available
            ),
        )
    except Exception as exc:
        logger.debug("Audit log write failed: %s", exc)


def _verify_search_preceded_mark(conn, query_text: str, max_age_seconds: int = 120) -> Dict[str, Any]:
    """Check tool_audit_log for a knowledge.search call within the last N seconds.
    Returns verification dict with evidence."""
    try:
        row = conn.execute(
            """SELECT log_id, timestamp, category_filter, query_text, result_count
               FROM tool_audit_log
               WHERE tool_name = 'knowledge.search'
               AND (julianday('now') - julianday(timestamp)) * 86400 <= ?
               ORDER BY timestamp DESC LIMIT 1""",
            (max_age_seconds,),
        ).fetchone()
        if row:
            return {
                "verified": True,
                "audit_log_id": row[0],
                "search_timestamp": row[1],
                "category_filter": row[2],
                "query_text": row[3],
                "result_count": row[4],
                "had_category_search": bool(row[2]),
            }
        return {"verified": False, "reason": "no_search_in_window"}
    except Exception as exc:
        logger.debug("Audit verification failed: %s", exc)
        return {"verified": False, "reason": f"error: {exc}"}


# ---------------------------------------------------------------------------
# Critique Response: A/B Stochastic Config (NotebookLM #3)
# Read recall rate from stochastic_config table for A/B testing.
# ---------------------------------------------------------------------------

def _get_active_recall_rate(conn, session_counter: int = 0) -> float:
    """Get the active recall probability, supporting A/B splits.
    session_counter: a monotonic int; odd/even determines A/B group."""
    try:
        row = conn.execute(
            """SELECT recall_rate_default, recall_rate_a, recall_rate_b, ab_split_method
               FROM stochastic_config WHERE active = 1
               ORDER BY config_id DESC LIMIT 1"""
        ).fetchone()
        if not row:
            return RANDOM_RECALL_P  # fallback to module constant
        default_rate = row[0]
        rate_a = row[1]
        rate_b = row[2]
        split_method = row[3] or "session_parity"
        # If A/B rates are configured, use session parity to split
        if rate_a is not None and rate_b is not None:
            if split_method == "session_parity":
                return rate_a if session_counter % 2 == 0 else rate_b
            # Future: other split methods
        return default_rate
    except Exception:
        return RANDOM_RECALL_P


# ---------------------------------------------------------------------------
# Critique Response: Continuous Ebbinghaus Decay (NotebookLM #3 / N6)
# R(t) = e^(-t/S) where S = 1 + access_count (stability)
# Demote when R < threshold (default 0.5)
# ---------------------------------------------------------------------------

def _apply_continuous_decay(conn, threshold: float = 0.5) -> int:
    """Apply Ebbinghaus continuous decay to earned-hot documents.
    R(t) = e^(-t/S) where t = days since last access, S = 1 + access_count.
    pinned_hot docs are immune."""
    import math
    from datetime import datetime, timezone
    try:
        rows = conn.execute(
            """SELECT doc_id, last_accessed, access_count, promoted_at
               FROM documents
               WHERE temperature = 'hot'
               AND COALESCE(pinned_hot, 0) = 0
               AND promoted_at IS NOT NULL"""
        ).fetchall()
        now = datetime.now(timezone.utc).isoformat()
        demoted = 0
        for row in rows:
            last_access = row[1] or row[3]  # fallback to promoted_at
            if not last_access:
                continue
            try:
                days_since = (datetime.now(timezone.utc) -
                              datetime.fromisoformat(last_access.replace('Z', '+00:00'))).total_seconds() / 86400
            except (ValueError, TypeError):
                # Try julianday fallback
                jd_row = conn.execute(
                    "SELECT julianday('now') - julianday(?)", (last_access,)
                ).fetchone()
                days_since = jd_row[0] if jd_row else 0
            stability = 1.0 + (row[2] or 0)  # S = 1 + access_count
            retention = math.exp(-days_since / stability)
            if retention < threshold:
                conn.execute(
                    "UPDATE documents SET temperature = 'warm', promoted_at = NULL WHERE doc_id = ?",
                    (row[0],),
                )
                demoted += 1
        return demoted
    except Exception as exc:
        logger.debug("Continuous decay failed: %s", exc)
        return 0


# ---------------------------------------------------------------------------
# Critique Response: Noise Purgatory (NotebookLM #5)
# Move noise docs to documents_archive, not just flag them.
# Add quality penalty for 2-strike docs.
# ---------------------------------------------------------------------------

def _apply_noise_quality_penalty(conn) -> dict:
    """Apply quality_score penalty to 2-strike docs (multiply by 0.5).
    This directly modifies quality_score in the DB so scoring formulas
    naturally deprioritize these docs without inline lookups.
    Returns {penalized: N, doc_ids: [...]}."""
    result = {"penalized": 0, "doc_ids": []}
    try:
        rows = conn.execute(
            """SELECT rp.doc_id, d.quality_score
               FROM random_promotions rp
               JOIN documents d ON d.doc_id = rp.doc_id
               WHERE rp.used_in_output = 0 AND rp.demoted_at IS NOT NULL
               AND COALESCE(d.noise_candidate, 0) = 0
               AND COALESCE(d.pinned_hot, 0) = 0
               GROUP BY rp.doc_id
               HAVING COUNT(*) = 2"""
        ).fetchall()
        for row in rows:
            doc_id = row["doc_id"]
            new_q = max(10, int(row["quality_score"] * 0.5))
            conn.execute(
                "UPDATE documents SET quality_score = ? WHERE doc_id = ?",
                (new_q, doc_id),
            )
            result["doc_ids"].append(doc_id)
        result["penalized"] = len(result["doc_ids"])
        return result
    except Exception:
        return result


def _archive_noise_docs(conn) -> int:
    """Move noise_candidate docs from documents to documents_archive.
    Also moves their chunks to keep the FTS index clean."""
    from datetime import datetime, timezone
    try:
        noise_docs = conn.execute(
            """SELECT * FROM documents WHERE noise_candidate = 1
               AND COALESCE(pinned_hot, 0) = 0"""
        ).fetchall()
        if not noise_docs:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        archived = 0
        for doc in noise_docs:
            doc_id = doc[0]  # doc_id is first column
            # Count noise strikes from random_promotions
            strike_row = conn.execute(
                """SELECT COALESCE(SUM(noise_strikes), 0) FROM random_promotions
                   WHERE doc_id = ?""", (doc_id,)
            ).fetchone()
            noise_strikes = strike_row[0] if strike_row else 0
            # Insert into archive
            conn.execute(
                """INSERT INTO documents_archive
                   (archived_at, archive_reason, doc_id, file_path, file_name,
                    file_type, category, entity, year, title, file_size,
                    total_chunks, indexed_at, last_modified, temperature,
                    quality_score, ingest_status, failure_reason, last_accessed,
                    access_count, promoted_at, pinned_hot, noise_candidate, noise_strikes)
                   VALUES (?, 'noise_threshold', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, doc_id, doc[1], doc[2], doc[3], doc[4], doc[5], doc[6],
                 doc[7], doc[8], doc[9], doc[10], doc[11], doc[12], doc[13],
                 doc[14], doc[15], doc[16], doc[17], doc[18], doc[19], doc[20],
                 noise_strikes),
            )
            # Remove from main tables
            conn.execute("DELETE FROM chunks WHERE doc_id = ?", (doc_id,))
            conn.execute("DELETE FROM documents WHERE doc_id = ?", (doc_id,))
            archived += 1
        return archived
    except Exception as exc:
        logger.debug("Archive noise docs failed: %s", exc)
        return 0


def _normalize_list(value: Optional[Any]) -> List[str]:
    if value is None:
        return []
    raw_items = value if isinstance(value, list) else [value]
    normalized: List[str] = []
    for item in raw_items:
        if item is None:
            continue
        for part in str(item).split(","):
            part = part.strip()
            if part:
                normalized.append(part)
    return normalized


def knowledge_status(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Return knowledge.db status and high-level counts."""
    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM documents")
            doc_count = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM chunks")
            chunk_count = cursor.fetchone()[0]
            cursor.execute("SELECT MAX(indexed_at) FROM documents")
            last_indexed_at = cursor.fetchone()[0]
            cursor.execute("SELECT MAX(last_modified) FROM documents")
            last_modified = cursor.fetchone()[0]
            cursor.execute("SELECT temperature, COUNT(*) AS count FROM documents GROUP BY temperature")
            temperature_counts = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.execute("SELECT ingest_status, COUNT(*) AS count FROM documents GROUP BY ingest_status")
            ingest_status_counts = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.execute("SELECT COUNT(*) FROM documents WHERE ingest_status = 'ocr_needed'")
            ocr_needed_count = cursor.fetchone()[0]

        result = {
            "ok": True,
            "db_path": get_db_path("knowledge"),
            "doc_count": doc_count,
            "chunk_count": chunk_count,
            "last_indexed_at": last_indexed_at,
            "last_modified": last_modified,
            "temperature_counts": temperature_counts,
            "ingest_status_counts": ingest_status_counts,
            "ocr_needed_count": ocr_needed_count,
            "provenance": build_provenance(config.homelab_host, []),
        }
    except Exception as exc:
        logger.error("Knowledge status failed: %s", exc, exc_info=True)
        result = error_response(
            "knowledge.status",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error reading knowledge.db"],
            suggested_next_tools=[{"tool": "meta.health", "args": {}}],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry("knowledge.status", args, [], "knowledge status", error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def knowledge_ocr_queue(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Return prioritized OCR-needed PDFs with importance scores."""
    limit = args.get("limit", 20)
    min_priority = args.get("min_priority", 0)

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            # Priority scoring:
            # +50 if category in {legal, house, hr, tax}
            # +20 if year >= 2025
            # +10 if entity matches priority entities
            # +(access_count * 5)
            # +10 if file_path contains high-value keywords
            cursor.execute(
                """
                SELECT
                    doc_id,
                    file_path,
                    file_name,
                    category,
                    entity,
                    year,
                    access_count,
                    indexed_at,
                    (
                        CASE WHEN category IN ('legal', 'house', 'hr', 'tax') THEN 50 ELSE 0 END +
                        CASE WHEN year >= 2025 THEN 20 ELSE 0 END +
                        CASE WHEN entity IN ('primary', 'default') THEN 10 ELSE 0 END +
                        (COALESCE(access_count, 0) * 5) +
                        CASE 
                            WHEN file_path LIKE '%Operating_Agreement%' OR 
                                 file_path LIKE '%EIN%' OR 
                                 file_path LIKE '%Appraisal%' OR
                                 file_path LIKE '%tax%' OR
                                 file_path LIKE '%W-2%' OR
                                 file_path LIKE '%1099%'
                            THEN 10 ELSE 0 
                        END
                    ) AS priority_score
                FROM documents
                WHERE ingest_status = 'ocr_needed'
                  AND file_type = 'pdf'
                ORDER BY priority_score DESC, indexed_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cursor.fetchall()
            
            # Get summary counts
            cursor.execute(
                """
                SELECT category, COUNT(*) as count
                FROM documents
                WHERE ingest_status = 'ocr_needed'
                GROUP BY category
                ORDER BY count DESC
                """
            )
            category_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.execute(
                """
                SELECT entity, COUNT(*) as count
                FROM documents
                WHERE ingest_status = 'ocr_needed'
                GROUP BY entity
                ORDER BY count DESC
                """
            )
            entity_counts = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.execute("SELECT COUNT(*) FROM documents WHERE ingest_status = 'ocr_needed'")
            total_ocr_needed = cursor.fetchone()[0]

        # Filter by min_priority and format results
        queue = []
        for row in rows:
            priority_score = row[8]
            if priority_score >= min_priority:
                queue.append({
                    "doc_id": row[0],
                    "file_path": row[1],
                    "file_name": row[2],
                    "category": row[3],
                    "entity": row[4],
                    "year": row[5],
                    "access_count": row[6],
                    "indexed_at": row[7],
                    "priority_score": priority_score,
                })

        result = {
            "ok": True,
            "total_ocr_needed": total_ocr_needed,
            "returned_count": len(queue),
            "queue": queue,
            "category_breakdown": category_counts,
            "entity_breakdown": entity_counts,
            "provenance": build_provenance(config.homelab_host, []),
        }
    except Exception as exc:
        logger.error("OCR queue failed: %s", exc, exc_info=True)
        result = error_response(
            "knowledge.ocr_queue",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Database query error"],
            suggested_next_tools=[{"tool": "knowledge.status", "args": {}}],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry("knowledge.ocr_queue", args, [], f"ocr queue: {len(queue)} docs", error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def knowledge_search(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Search knowledge.db via FTS5 with optional filters."""
    query = (args.get("query") or "").strip()
    if not query:
        result = error_response(
            "knowledge.search",
            "query is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing query argument"],
            suggested_next_tools=[{"tool": "knowledge.status", "args": {}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("knowledge.search", args, [], "query missing", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    categories = _normalize_list(args.get("category"))
    entities = _normalize_list(args.get("entity"))
    file_types = _normalize_list(args.get("file_type"))
    years_raw = _normalize_list(args.get("year"))
    temperatures = _normalize_list(args.get("temperature"))
    ingest_statuses = _normalize_list(args.get("ingest_status"))
    include_cold = bool(args.get("include_cold", False))
    min_quality_score_raw = args.get("min_quality_score", 30)

    try:
        years = [int(value) for value in years_raw] if years_raw else []
    except ValueError:
        result = error_response(
            "knowledge.search",
            "year must be an integer or list of integers",
            error_code="INVALID_ARGS",
            likely_causes=["Invalid year format"],
            suggested_next_tools=[{"tool": "knowledge.search", "args": {"query": query}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("knowledge.search", args, [], "invalid year", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if min_quality_score_raw in (None, ""):
        min_quality_score = 30
    else:
        try:
            min_quality_score = int(min_quality_score_raw)
        except (TypeError, ValueError):
            result = error_response(
                "knowledge.search",
                "min_quality_score must be an integer",
                error_code="INVALID_ARGS",
                likely_causes=["Invalid min_quality_score format"],
                suggested_next_tools=[{"tool": "knowledge.search", "args": {"query": query}}],
                host=config.homelab_host,
            )
            audit_entry = build_audit_entry("knowledge.search", args, [], "invalid min_quality_score", error_code="INVALID_ARGS")
            append_audit_log(config.audit_log_path, audit_entry)
            return result

    min_quality_score = max(0, min(min_quality_score, 100))

    limit = int(args.get("limit", 10))
    offset = int(args.get("offset", 0))
    snippet_tokens = int(args.get("snippet_tokens", 64))
    limit = max(1, min(limit, 50))
    offset = max(0, offset)
    snippet_tokens = max(10, min(snippet_tokens, 200))

    temperatures = [value.lower() for value in temperatures] if temperatures else []
    ingest_statuses = [value.lower() for value in ingest_statuses] if ingest_statuses else []
    if not ingest_statuses:
        ingest_statuses = ["indexed"]
    if not temperatures:
        temperatures = ["warm", "hot"]
    primary_temps = [temp for temp in temperatures if temp != "cold"]
    cold_allowed = include_cold or ("cold" in temperatures)

    def _build_search_sql(temps: List[str], row_limit: int, match_query: str) -> Tuple[str, List[Any]]:
        filters = []
        params: List[Any] = [snippet_tokens, match_query]

        if temps:
            placeholders = ",".join("?" for _ in temps)
            filters.append(f"d.temperature IN ({placeholders})")
            params.extend(temps)
        if ingest_statuses:
            placeholders = ",".join("?" for _ in ingest_statuses)
            filters.append(f"d.ingest_status IN ({placeholders})")
            params.extend(ingest_statuses)
        filters.append("d.quality_score >= ?")
        params.append(min_quality_score)
        if categories:
            placeholders = ",".join("?" for _ in categories)
            filters.append(f"d.category IN ({placeholders})")
            params.extend(categories)
        if entities:
            placeholders = ",".join("?" for _ in entities)
            filters.append(f"d.entity IN ({placeholders})")
            params.extend(entities)
        if file_types:
            placeholders = ",".join("?" for _ in file_types)
            filters.append(f"d.file_type IN ({placeholders})")
            params.extend(file_types)
        if years:
            placeholders = ",".join("?" for _ in years)
            filters.append(f"d.year IN ({placeholders})")
            params.extend(years)

        where_clause = ""
        if filters:
            where_clause = " AND " + " AND ".join(filters)

        sql = f"""
            SELECT
                d.doc_id,
                d.file_path,
                d.file_name,
                d.category,
                d.entity,
                d.year,
                d.last_modified,
                d.temperature,
                d.quality_score,
                d.ingest_status,
                d.last_accessed,
                d.access_count,
                snippet(chunks_fts, 0, '>>>', '<<<', '...', ?) AS snippet,
                bm25(chunks_fts) AS rank_score,
                (
                    bm25(chunks_fts)
                    + CASE d.temperature WHEN 'hot' THEN -0.5 WHEN 'warm' THEN -0.1 ELSE 0 END
                    + (d.quality_score * -0.005)
                    + CASE
                        WHEN d.last_modified IS NOT NULL THEN (
                            -0.1 / (1.0 + (julianday('now') - julianday(d.last_modified)))
                        )
                        ELSE 0
                      END
                    + (-0.001 * COALESCE(d.access_count, 0))
                ) AS adjusted_score
            FROM chunks_fts
            JOIN chunks c ON chunks_fts.rowid = c.rowid
            JOIN documents d ON c.doc_id = d.doc_id
            WHERE chunks_fts MATCH ?
            {where_clause}
            ORDER BY adjusted_score ASC, rank_score ASC
            LIMIT ?
        """
        params.append(row_limit)
        return sql, params

    def _build_metadata_search_sql(temps: List[str], row_limit: int, metadata_query: str) -> Tuple[str, List[Any]]:
        filters = []
        like_pattern = f"%{metadata_query}%"
        params: List[Any] = [like_pattern, like_pattern, like_pattern]

        if temps:
            placeholders = ",".join("?" for _ in temps)
            filters.append(f"d.temperature IN ({placeholders})")
            params.extend(temps)
        if ingest_statuses:
            placeholders = ",".join("?" for _ in ingest_statuses)
            filters.append(f"d.ingest_status IN ({placeholders})")
            params.extend(ingest_statuses)
        filters.append("d.quality_score >= ?")
        params.append(min_quality_score)
        if categories:
            placeholders = ",".join("?" for _ in categories)
            filters.append(f"d.category IN ({placeholders})")
            params.extend(categories)
        if entities:
            placeholders = ",".join("?" for _ in entities)
            filters.append(f"d.entity IN ({placeholders})")
            params.extend(entities)
        if file_types:
            placeholders = ",".join("?" for _ in file_types)
            filters.append(f"d.file_type IN ({placeholders})")
            params.extend(file_types)
        if years:
            placeholders = ",".join("?" for _ in years)
            filters.append(f"d.year IN ({placeholders})")
            params.extend(years)

        where_clause = " AND " + " AND ".join(filters) if filters else ""

        sql = f"""
            SELECT
                d.doc_id,
                d.file_path,
                d.file_name,
                d.category,
                d.entity,
                d.year,
                d.last_modified,
                d.temperature,
                d.quality_score,
                d.ingest_status,
                d.last_accessed,
                d.access_count,
                SUBSTR(COALESCE(c.chunk_text, ''), 1, 600) AS snippet,
                NULL AS rank_score,
                (
                    CASE d.temperature WHEN 'hot' THEN -0.5 WHEN 'warm' THEN -0.1 ELSE 0 END
                    + (d.quality_score * -0.005)
                    + CASE
                        WHEN d.last_modified IS NOT NULL THEN (
                            -0.1 / (1.0 + (julianday('now') - julianday(d.last_modified)))
                        )
                        ELSE 0
                      END
                    + (-0.001 * COALESCE(d.access_count, 0))
                ) AS adjusted_score
            FROM documents d
            LEFT JOIN chunks c
                ON c.doc_id = d.doc_id
               AND c.chunk_number = 0
            WHERE (d.file_name LIKE ? OR d.file_path LIKE ? OR COALESCE(d.title, '') LIKE ?)
            {where_clause}
            ORDER BY adjusted_score ASC, d.last_modified DESC
            LIMIT ?
        """
        params.append(row_limit)
        return sql, params

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            target_count = limit + offset
            rows: List[sqlite3.Row] = []
            seen_doc_ids = set()

            def _append_rows(batch: List[sqlite3.Row]) -> None:
                for row in batch:
                    doc_id = row["doc_id"]
                    if doc_id in seen_doc_ids:
                        continue
                    seen_doc_ids.add(doc_id)
                    rows.append(row)

            def _run_fts(match_query: str) -> bool:
                query_ran = False
                if primary_temps:
                    query_ran = True
                    sql, params = _build_search_sql(primary_temps, target_count, match_query)
                    cursor.execute(sql, params)
                    _append_rows(cursor.fetchall())
                if len(rows) < target_count and cold_allowed:
                    query_ran = True
                    remaining = target_count - len(rows)
                    sql, params = _build_search_sql(["cold"], remaining, match_query)
                    cursor.execute(sql, params)
                    _append_rows(cursor.fetchall())
                return query_ran

            def _run_metadata(metadata_query: str) -> bool:
                query_ran = False
                if primary_temps:
                    query_ran = True
                    sql, params = _build_metadata_search_sql(primary_temps, target_count, metadata_query)
                    cursor.execute(sql, params)
                    _append_rows(cursor.fetchall())
                if len(rows) < target_count and cold_allowed:
                    query_ran = True
                    remaining = target_count - len(rows)
                    sql, params = _build_metadata_search_sql(["cold"], remaining, metadata_query)
                    cursor.execute(sql, params)
                    _append_rows(cursor.fetchall())
                return query_ran

            fts_syntax_error = None
            try:
                _run_fts(query)
            except sqlite3.OperationalError as exc:
                msg = str(exc).lower()
                if "fts5" not in msg and "syntax error" not in msg:
                    raise
                fts_syntax_error = exc

            sanitized_query = _sanitize_fts_query(query)
            if (fts_syntax_error or not rows) and sanitized_query and sanitized_query != query:
                try:
                    _run_fts(sanitized_query)
                    fts_syntax_error = None
                except sqlite3.OperationalError as exc:
                    msg = str(exc).lower()
                    if "fts5" not in msg and "syntax error" not in msg:
                        raise

            if not rows:
                metadata_query = query or sanitized_query
                if metadata_query:
                    _run_metadata(metadata_query)

            results = []
            for row in rows:
                snippet = row["snippet"] or ""
                snippet, _ = truncate_text(snippet, 600)
                results.append({
                    "doc_id": row["doc_id"],
                    "file_path": row["file_path"],
                    "file_name": row["file_name"],
                    "category": row["category"],
                    "entity": row["entity"],
                    "year": row["year"],
                    "last_modified": row["last_modified"],
                    "temperature": row["temperature"],
                    "quality_score": row["quality_score"],
                    "ingest_status": row["ingest_status"],
                    "last_accessed": row["last_accessed"],
                    "access_count": row["access_count"],
                    "snippet": snippet,
                    "rank_score": row["rank_score"],
                })

            results = results[offset: offset + limit]
            top_docs = {entry["doc_id"] for entry in results[: min(10, len(results))]}
            if top_docs:
                placeholders = ",".join("?" for _ in top_docs)
                cursor.execute(
                    f"""
                    UPDATE documents
                    SET last_accessed = CURRENT_TIMESTAMP,
                        access_count = access_count + 1
                    WHERE doc_id IN ({placeholders})
                    """,
                    list(top_docs),
                )

            # --- Critique #1: Server-side audit log (NotebookLM) ---
            # The AI CANNOT write to this table. This is deterministic evidence.
            result_doc_ids = [r["doc_id"] for r in results]
            _log_tool_call(
                conn, "knowledge.search", args,
                result_count=len(results), doc_ids=result_doc_ids,
            )

        result = {
            "ok": True,
            "query": query,
            "count": len(results),
            "limit": limit,
            "offset": offset,
            "results": results,
            "provenance": build_provenance(config.homelab_host, []),
        }
    except Exception as exc:
        logger.error("Knowledge search failed: %s", exc, exc_info=True)
        result = error_response(
            "knowledge.search",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error querying knowledge.db"],
            suggested_next_tools=[{"tool": "knowledge.status", "args": {}}],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry("knowledge.search", args, [], truncate_text(str(result), config.audit_preview_limit)[0], error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def _fetch_pin_docs(conn: sqlite3.Connection, query: str, limit: int) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT
            d.doc_id,
            d.file_path,
            d.file_name,
            d.category,
            d.entity,
            d.year,
            d.last_modified,
            d.temperature,
            d.quality_score,
            d.ingest_status,
            c.chunk_text
        FROM documents d
        LEFT JOIN chunks c ON c.doc_id = d.doc_id AND c.chunk_number = 0
        WHERE {query}
        ORDER BY d.last_modified DESC, d.file_path ASC
        LIMIT ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (limit,))
    rows = cursor.fetchall()
    entries = []
    for row in rows:
        snippet = row["chunk_text"] or ""
        snippet, _ = truncate_text(snippet, 600)
        pinned_quality = row["quality_score"] or 0
        entries.append({
            "doc_id": row["doc_id"],
            "file_path": row["file_path"],
            "file_name": row["file_name"],
            "category": row["category"],
            "entity": row["entity"],
            "year": row["year"],
            "last_modified": row["last_modified"],
            "temperature": "hot",
            "quality_score": max(pinned_quality, 90),
            "ingest_status": row["ingest_status"],
            "snippet": snippet,
            "rank_score": None,
        })
    return entries


# ---------------------------------------------------------------------------
# Stochastic recall — "random thought intrusion" (GPT 5.2 collab)
# ---------------------------------------------------------------------------

# Probability of random recall per bootstrap call
RANDOM_RECALL_P = 0.03  # 3% of sessions
MAX_RANDOM_DOCS = 1     # at most 1 random doc per session
NOISE_STRIKE_THRESHOLD = 3  # X unused promotions → noise_candidate


def _stochastic_recall(conn, trigger_query: str = "", session_counter: int = 0) -> Optional[Dict[str, Any]]:
    """
    With probability from A/B config (or RANDOM_RECALL_P fallback), select 1
    cold/warm doc and promote it to trial-hot.

    Critique #3 (NotebookLM): recall rate is now configurable via
    stochastic_config table.  Session parity determines which rate applies.

    Rules:
    - Only non-noise_candidate docs
    - Only indexed docs with quality_score >= 30
    - Weighted by quality (higher quality → more likely to be recalled)
    - Logged in random_promotions table for accountability
    """
    import random
    import json
    from datetime import datetime, timezone

    recall_rate = _get_active_recall_rate(conn, session_counter=session_counter)
    if random.random() > recall_rate:
        return None

    try:
        # Pick a random cold/warm doc, weighted toward higher quality
        # ORDER BY RANDOM() * quality_score DESC → quality-biased random
        row = conn.execute(
            """SELECT d.doc_id, d.file_path, d.file_name, d.category, d.entity,
                      d.year, d.last_modified, d.temperature, d.quality_score,
                      d.ingest_status,
                      c.chunk_text
               FROM documents d
               LEFT JOIN chunks c ON c.doc_id = d.doc_id AND c.chunk_number = 0
               WHERE d.temperature IN ('cold', 'warm')
               AND d.ingest_status = 'indexed'
               AND d.quality_score >= 30
               AND COALESCE(d.noise_candidate, 0) = 0
               ORDER BY RANDOM() * d.quality_score DESC
               LIMIT 1""",
        ).fetchone()

        if not row:
            return None

        now = datetime.now(timezone.utc).isoformat()
        doc_id = row["doc_id"]
        original_temp = row["temperature"]

        # Promote to trial-hot
        conn.execute(
            "UPDATE documents SET temperature = 'hot', promoted_at = ? WHERE doc_id = ?",
            (now, doc_id),
        )

        # Log in random_promotions
        conn.execute(
            """INSERT INTO random_promotions
               (timestamp, doc_id, file_name, original_temp, trigger_session)
               VALUES (?, ?, ?, ?, ?)""",
            (now, doc_id, row["file_name"], original_temp, trigger_query[:200]),
        )

        snippet = row["chunk_text"] or ""
        snippet, _ = truncate_text(snippet, 600)

        return {
            "doc_id": doc_id,
            "file_path": row["file_path"],
            "file_name": row["file_name"],
            "category": row["category"],
            "entity": row["entity"],
            "year": row["year"],
            "last_modified": row["last_modified"],
            "temperature": "hot",  # just promoted
            "quality_score": row["quality_score"],
            "ingest_status": row["ingest_status"],
            "snippet": snippet,
            "rank_score": None,
            "_random_recall": True,
            "_original_temp": original_temp,
        }
    except Exception as exc:
        logger.debug("Stochastic recall skipped: %s", exc)
        return None


def _run_noise_detection(conn) -> Dict[str, int]:
    """
    Check random_promotions for docs promoted >= NOISE_STRIKE_THRESHOLD times
    without ever being used. Mark them as noise_candidate=1 and demote to cold.
    Then archive docs with enough strikes (Critique #5: noise purgatory).
    Returns dict with counts: {"marked": N, "archived": M}.
    """
    from datetime import datetime, timezone

    result = {"marked": 0, "archived": 0, "penalized": 0}
    try:
        # First: penalize 2-strike docs (quality_score * 0.5)
        penalty = _apply_noise_quality_penalty(conn)
        result["penalized"] = penalty["penalized"]

        # Find docs with enough strikes
        noise_rows = conn.execute(
            """SELECT rp.doc_id, COUNT(*) as strike_count
               FROM random_promotions rp
               JOIN documents d ON d.doc_id = rp.doc_id
               WHERE rp.used_in_output = 0
               AND rp.demoted_at IS NOT NULL
               AND COALESCE(d.noise_candidate, 0) = 0
               GROUP BY rp.doc_id
               HAVING COUNT(*) >= ?""",
            (NOISE_STRIKE_THRESHOLD,),
        ).fetchall()

        now = datetime.now(timezone.utc).isoformat()
        for row in noise_rows:
            conn.execute(
                """UPDATE documents 
                   SET noise_candidate = 1, temperature = 'cold'
                   WHERE doc_id = ? AND COALESCE(pinned_hot, 0) = 0""",
                (row["doc_id"],),
            )
            result["marked"] += 1

        # Critique #5: Archive noise docs to purgatory
        result["archived"] = _archive_noise_docs(conn)

        return result
    except Exception as exc:
        logger.debug("Noise detection skipped: %s", exc)
        return result


def _demote_unused_random_promotions(conn) -> int:
    """
    Demote trial-hot docs back to their original temperature if they haven't
    been used (used_in_output=0) and were promoted > 24h ago.
    Returns count of demotions.
    """
    try:
        # Find active random promotions older than 24h that were never used
        stale = conn.execute(
            """SELECT rp.promo_id, rp.doc_id, rp.original_temp
               FROM random_promotions rp
               WHERE rp.demoted_at IS NULL
               AND rp.used_in_output = 0
               AND julianday('now') - julianday(rp.timestamp) > 1.0"""
        ).fetchall()

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        count = 0
        for row in stale:
            conn.execute(
                "UPDATE documents SET temperature = ?, promoted_at = NULL WHERE doc_id = ?",
                (row["original_temp"], row["doc_id"]),
            )
            conn.execute(
                """UPDATE random_promotions 
                   SET demoted_at = ?, noise_strikes = noise_strikes + 1
                   WHERE promo_id = ?""",
                (now, row["promo_id"]),
            )
            count += 1
        return count
    except Exception as exc:
        logger.debug("Random demotion skipped: %s", exc)
        return 0


def knowledge_bootstrap_context(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Return a deterministic startup pack from pins, recent docs, topic search, and stochastic recall."""
    pin_limit = int(args.get("pin_limit", 5))
    recent_limit = int(args.get("recent_limit", 8))
    topic_limit = int(args.get("topic_limit", 10))
    snippet_tokens = int(args.get("snippet_tokens", 64))
    include_devloop = bool(args.get("include_devloop", True))
    devloop_artifact_limit = int(args.get("devloop_artifact_limit", 2))
    pin_limit = max(1, min(pin_limit, 20))
    recent_limit = max(1, min(recent_limit, 25))
    topic_limit = max(1, min(topic_limit, 30))
    snippet_tokens = max(10, min(snippet_tokens, 200))
    devloop_artifact_limit = max(1, min(devloop_artifact_limit, 5))

    topics = args.get("topics")
    if topics is None or topics == "":
        topics = ["mcp", "todo", "automation", "calendar"]
    topics = _normalize_list(topics)

    warnings: List[str] = []
    devloop_summary: Optional[Dict[str, Any]] = None

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name, query, priority, notes FROM context_pins ORDER BY priority ASC, name ASC"
            )
            pins = cursor.fetchall()
            pinned_docs: List[Dict[str, Any]] = []
            for pin in pins:
                pinned_docs.extend(_fetch_pin_docs(conn, pin["query"], pin_limit))

            # --- Pinned hot docs (sigils, corrections) ---
            # These are docs with pinned_hot=1 in documents table.
            # They are the "10 commandments" — always surfaced first.
            seen_pin_ids = {d["doc_id"] for d in pinned_docs}
            cursor.execute(
                """
                SELECT
                    d.doc_id,
                    d.file_path,
                    d.file_name,
                    d.category,
                    d.entity,
                    d.year,
                    d.last_modified,
                    d.temperature,
                    d.quality_score,
                    d.ingest_status,
                    c.chunk_text
                FROM documents d
                LEFT JOIN chunks c ON c.doc_id = d.doc_id AND c.chunk_number = 0
                WHERE d.pinned_hot = 1
                  AND d.ingest_status = 'indexed'
                ORDER BY d.quality_score DESC, d.file_path ASC
                LIMIT ?
                """,
                (pin_limit,),
            )
            for row in cursor.fetchall():
                if row["doc_id"] in seen_pin_ids:
                    continue
                snippet = row["chunk_text"] or ""
                snippet, _ = truncate_text(snippet, 600)
                pinned_docs.append({
                    "doc_id": row["doc_id"],
                    "file_path": row["file_path"],
                    "file_name": row["file_name"],
                    "category": row["category"],
                    "entity": row["entity"],
                    "year": row["year"],
                    "last_modified": row["last_modified"],
                    "temperature": "hot",
                    "quality_score": row["quality_score"],
                    "ingest_status": row["ingest_status"],
                    "snippet": snippet,
                    "rank_score": None,
                })

            cursor.execute(
                """
                SELECT
                    d.doc_id,
                    d.file_path,
                    d.file_name,
                    d.category,
                    d.entity,
                    d.year,
                    d.last_modified,
                    d.temperature,
                    d.quality_score,
                    d.ingest_status,
                    c.chunk_text
                FROM documents d
                LEFT JOIN chunks c ON c.doc_id = d.doc_id AND c.chunk_number = 0
                WHERE d.category = 'documentation'
                  AND d.temperature IN ('hot', 'warm')
                  AND d.ingest_status = 'indexed'
                ORDER BY d.last_modified DESC, d.file_path ASC
                LIMIT ?
                """,
                (recent_limit,),
            )
            recent_rows = cursor.fetchall()
            recent_docs: List[Dict[str, Any]] = []
            for row in recent_rows:
                snippet = row["chunk_text"] or ""
                snippet, _ = truncate_text(snippet, 600)
                recent_docs.append({
                    "doc_id": row["doc_id"],
                    "file_path": row["file_path"],
                    "file_name": row["file_name"],
                    "category": row["category"],
                    "entity": row["entity"],
                    "year": row["year"],
                    "last_modified": row["last_modified"],
                    "temperature": row["temperature"],
                    "quality_score": row["quality_score"],
                    "ingest_status": row["ingest_status"],
                    "snippet": snippet,
                    "rank_score": None,
                })

            topic_docs: List[Dict[str, Any]] = []
            if topics:
                per_topic = max(1, topic_limit // max(len(topics), 1))
                for topic in topics:
                    cursor.execute(
                        """
                        SELECT
                            d.doc_id,
                            d.file_path,
                            d.file_name,
                            d.category,
                            d.entity,
                            d.year,
                            d.last_modified,
                            d.temperature,
                            d.quality_score,
                            d.ingest_status,
                            snippet(chunks_fts, 0, '>>>', '<<<', '...', ?) AS snippet,
                            bm25(chunks_fts) AS rank_score
                        FROM chunks_fts
                        JOIN chunks c ON chunks_fts.rowid = c.rowid
                        JOIN documents d ON c.doc_id = d.doc_id
                        WHERE chunks_fts MATCH ?
                          AND d.category = 'documentation'
                          AND d.temperature IN ('hot', 'warm')
                          AND d.ingest_status = 'indexed'
                        ORDER BY rank_score ASC
                        LIMIT ?
                        """,
                        (snippet_tokens, topic, per_topic),
                    )
                    for row in cursor.fetchall():
                        snippet = row["snippet"] or ""
                        snippet, _ = truncate_text(snippet, 600)
                        topic_docs.append({
                            "doc_id": row["doc_id"],
                            "file_path": row["file_path"],
                            "file_name": row["file_name"],
                            "category": row["category"],
                            "entity": row["entity"],
                            "year": row["year"],
                            "last_modified": row["last_modified"],
                            "temperature": row["temperature"],
                            "quality_score": row["quality_score"],
                            "ingest_status": row["ingest_status"],
                            "snippet": snippet,
                            "rank_score": row["rank_score"],
                        })
                topic_docs.sort(key=lambda item: (item["rank_score"] or 0, item["file_path"]))
                seen = set()
                deduped = []
                for item in topic_docs:
                    key = (item["doc_id"], item["snippet"])
                    if key in seen:
                        continue
                    seen.add(key)
                    deduped.append(item)
                topic_docs = deduped[:topic_limit]

            devloop_summary = None
            if include_devloop:
                try:
                    cursor.execute(
                        """
                        SELECT run_id, created_at, title, goal, origin, status, tags
                        FROM devloop_runs
                        ORDER BY created_at DESC
                        LIMIT 1
                        """
                    )
                    run_row = cursor.fetchone()
                    if run_row:
                        cursor.execute(
                            """
                            SELECT artifact_id, artifact_type, model, content, created_at
                            FROM devloop_artifacts
                            WHERE run_id = ?
                            ORDER BY
                                CASE artifact_type
                                    WHEN 'codex_summary' THEN 1
                                    WHEN 'audit' THEN 2
                                    WHEN 'decision' THEN 3
                                    WHEN 'test_log' THEN 4
                                    ELSE 9
                                END,
                                created_at DESC
                            LIMIT ?
                            """,
                            (run_row["run_id"], devloop_artifact_limit),
                        )
                        artifacts = []
                        for artifact_row in cursor.fetchall():
                            content = artifact_row["content"] or ""
                            content, _ = truncate_text(content, 800)
                            artifacts.append({
                                "artifact_id": artifact_row["artifact_id"],
                                "artifact_type": artifact_row["artifact_type"],
                                "model": artifact_row["model"],
                                "created_at": artifact_row["created_at"],
                                "content": content,
                            })
                        devloop_summary = {
                            "run": {
                                "run_id": run_row["run_id"],
                                "created_at": run_row["created_at"],
                                "title": run_row["title"],
                                "goal": run_row["goal"],
                                "origin": run_row["origin"],
                                "status": run_row["status"],
                                "tags": run_row["tags"],
                            },
                            "artifacts": artifacts,
                        }
                except sqlite3.OperationalError:
                    warnings.append("devloop tables missing; run migration 008_knowledge_tiering_devloop.sql")

            # --- Stochastic recall: "random thought intrusion" ---
            random_doc = None
            random_demoted = 0
            noise_result = {"marked": 0, "archived": 0}
            try:
                # First, demote stale random promotions (>24h, unused)
                random_demoted = _demote_unused_random_promotions(conn)
                # Then run noise detection + purgatory archival
                noise_result = _run_noise_detection(conn)
                # Then maybe surface one random doc (rate from A/B config)
                topic_hint = " ".join(topics) if topics else ""
                # Fix: pass session_counter for A/B split (hash of topic_hint for parity)
                sc = hash(topic_hint) & 0x7FFFFFFF  # positive int from topics
                random_doc = _stochastic_recall(conn, trigger_query=topic_hint, session_counter=sc)
            except Exception as exc:
                logger.debug("Stochastic recall subsystem: %s", exc)
            # --- end stochastic recall ---

        result = {
            "ok": True,
            "pins": pinned_docs,
            "recent": recent_docs,
            "topics": topic_docs,
            "random_recall": random_doc,  # None or a single doc dict
            "memory_exploration": {
                "random_recall_p": RANDOM_RECALL_P,
                "triggered": random_doc is not None,
                "stale_demoted": random_demoted,
                "noise_marked": noise_result.get("marked", 0),
                "noise_archived": noise_result.get("archived", 0),
            },
            "warnings": warnings,
            "provenance": build_provenance(config.homelab_host, []),
        }
        if include_devloop:
            result["devloop"] = devloop_summary
    except sqlite3.OperationalError as exc:
        warnings.append("context_pins table missing; run migration 007_knowledge_context_pins.sql")
        result = error_response(
            "knowledge.bootstrap_context",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Knowledge DB schema missing context_pins table"],
            suggested_next_tools=[{"tool": "knowledge.status", "args": {}}],
            host=config.homelab_host,
        )
        result["warnings"] = warnings
    except Exception as exc:
        logger.error("Knowledge bootstrap failed: %s", exc, exc_info=True)
        result = error_response(
            "knowledge.bootstrap_context",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error generating bootstrap context"],
            suggested_next_tools=[{"tool": "knowledge.status", "args": {}}],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry("knowledge.bootstrap_context", args, [], truncate_text(str(result), config.audit_preview_limit)[0], error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def knowledge_context_mark(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mark which knowledge docs were used for context and compute a confidence hash.
    
    Flow:
    1. AI searches knowledge.db (one or more calls)
    2. AI calls this tool with the doc_ids it used plus the query
    3. This tool computes SHA256(sorted doc_ids + query), scores confidence,
       and writes to context_marks table
    4. Returns the hash + confidence for the AI to reference
    
    GPT 5.2 additions:
    - Cap: max 3 docs promoted to hot per call
    - Details: S components stored per doc in details_json
    - Decay: earned-hot docs decay to warm after 14 days without access
    - pinned_hot docs (sigil category) never decay
    """
    import hashlib
    import json
    import math
    from datetime import datetime, timezone

    MAX_PROMOTIONS = 3

    query_text = (args.get("query") or "").strip()
    doc_ids = _normalize_list(args.get("doc_ids"))
    category_filter = (args.get("category") or "").strip() or None
    expanded = bool(args.get("expanded", False))
    note = (args.get("note") or "").strip() or None

    # Compliance flags — now VERIFIED against audit log, not trusted blindly
    compliance_input = args.get("compliance") or {}
    compliance = {
        "did_step1_category_search": bool(compliance_input.get("did_step1_category_search", False)),
        "used_workarounds_detected": bool(compliance_input.get("used_workarounds_detected", False)),
        "expanded_used": expanded,
        "promotion_count": 0,  # filled in later
        # NEW: server-side verification fields (NotebookLM critique #2)
        "audit_verified": False,
        "audit_log_id": None,
        "audit_had_category_search": False,
    }

    if not query_text:
        return error_response(
            "knowledge.context_mark",
            "query is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing query text"],
            suggested_next_tools=[{"tool": "knowledge.search", "args": {}}],
            host=config.homelab_host,
        )

    details = {"s_components": [], "decay_applied": 0, "compliance": compliance,
               "audit_verification": {}}

    if not doc_ids:
        # No docs found — record a cold mark
        hash_input = f"EMPTY|{query_text}|{category_filter or ''}"
        context_hash = hashlib.sha256(hash_input.encode()).hexdigest()
        confidence_score = 0.0
        confidence_level = "cold"
    else:
        # Compute hash from sorted doc_ids + query
        sorted_ids = sorted(doc_ids)
        hash_input = "|".join(sorted_ids) + "|" + query_text
        context_hash = hashlib.sha256(hash_input.encode()).hexdigest()

        # Compute confidence: C = 1 - e^(-λ * S)
        # Store S components per doc (GPT 5.2: debug why confidence rose/fell)
        temp_weights = {"hot": 1.0, "warm": 0.6, "cold": 0.2}
        S = 0.0
        try:
            with get_db_connection("knowledge") as conn:
                # --- Critique #2: Verify search actually happened (NotebookLM) ---
                verification = _verify_search_preceded_mark(conn, query_text)
                details["audit_verification"] = verification
                compliance["audit_verified"] = verification.get("verified", False)
                compliance["audit_log_id"] = verification.get("audit_log_id")
                compliance["audit_had_category_search"] = verification.get("had_category_search", False)

                placeholders = ",".join("?" for _ in doc_ids)
                rows = conn.execute(
                    f"""SELECT doc_id, quality_score, temperature, file_name
                        FROM documents WHERE doc_id IN ({placeholders})""",
                    doc_ids,
                ).fetchall()

                # --- Critique #3: 2-strike quality penalty (NotebookLM #5) ---
                # Docs with 2 noise strikes get 0.5x quality multiplier
                strike_counts = {}
                try:
                    for did in doc_ids:
                        sc = conn.execute(
                            """SELECT COUNT(*) FROM random_promotions
                               WHERE doc_id = ? AND used_in_output = 0 AND demoted_at IS NOT NULL""",
                            (did,),
                        ).fetchone()
                        strike_counts[did] = sc[0] if sc else 0
                except Exception:
                    pass

                for row in rows:
                    w = temp_weights.get(row[2], 0.3)
                    quality = row[1]
                    # Apply 2-strike penalty
                    strikes = strike_counts.get(row[0], 0)
                    quality_multiplier = 0.5 if strikes >= 2 else 1.0
                    s_i = (quality / 100.0) * w * quality_multiplier
                    S += s_i
                    details["s_components"].append({
                        "doc_id": row[0][:12],
                        "file": row[3],
                        "quality": quality,
                        "temp": row[2],
                        "weight": w,
                        "quality_multiplier": quality_multiplier,
                        "noise_strikes": strikes,
                        "s_i": round(s_i, 4),
                    })

                # --- Critique #2: Continuous Ebbinghaus decay (replaces 14-day cliff) ---
                # R(t) = e^(-t/S) where S = 1 + access_count
                try:
                    decay_count = _apply_continuous_decay(conn, threshold=0.5)
                    details["decay_applied"] = decay_count
                    details["decay_model"] = "ebbinghaus"
                except Exception:
                    pass  # continuous decay columns may not exist in older schemas

        except Exception:
            S = len(doc_ids) * 0.5

        decay_lambda = 0.5
        confidence_score = round(1.0 - math.exp(-decay_lambda * S), 4)

        if confidence_score >= 0.75:
            confidence_level = "hot"
        elif confidence_score >= 0.50:
            confidence_level = "warm"
        elif confidence_score >= 0.25:
            confidence_level = "weak"
        elif confidence_score > 0:
            confidence_level = "cold"
        else:
            confidence_level = "cold"

    details["S_total"] = round(S, 4) if doc_ids else 0.0
    details["lambda"] = 0.5
    details["schema_version"] = 2
    details["compliance"] = compliance

    # Write to context_marks + promote expanded docs to hot (capped at top 3)
    now = datetime.now(timezone.utc).isoformat()
    promoted_count = 0
    try:
        with get_db_connection("knowledge") as conn:
            conn.execute(
                """INSERT INTO context_marks 
                   (timestamp, query_text, category_filter, doc_ids_json, doc_count,
                    context_hash, confidence_score, confidence_level, expanded, note,
                    details_json)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    now, query_text, category_filter,
                    json.dumps(doc_ids), len(doc_ids),
                    context_hash, confidence_score, confidence_level,
                    1 if expanded else 0, note,
                    json.dumps(details),
                ),
            )

            # Promote expanded docs — cap at MAX_PROMOTIONS (GPT 5.2: anti-steamroll)
            if expanded and doc_ids:
                # Sort by S contribution descending, take top N
                top_docs = sorted(
                    details["s_components"],
                    key=lambda d: d.get("s_i", 0),
                    reverse=True,
                )[:MAX_PROMOTIONS]
                promote_ids = [
                    # Expand the truncated doc_id back to full
                    next((did for did in doc_ids if did.startswith(d["doc_id"])), None)
                    for d in top_docs
                ]
                promote_ids = [pid for pid in promote_ids if pid]

                if promote_ids:
                    placeholders = ",".join("?" for _ in promote_ids)
                    cursor = conn.execute(
                        f"""UPDATE documents 
                            SET temperature = 'hot',
                                promoted_at = COALESCE(promoted_at, ?)
                            WHERE doc_id IN ({placeholders})
                            AND (temperature != 'hot' OR promoted_at IS NULL)""",
                        [now] + promote_ids,
                    )
                    promoted_count = cursor.rowcount
                    compliance["promotion_count"] = promoted_count

            # Fix: context_mark should also increment access_count
            # (Ebbinghaus stability S = 1 + access_count must benefit from explicit usage)
            if doc_ids:
                id_placeholders = ",".join("?" for _ in doc_ids)
                conn.execute(
                    f"""UPDATE documents
                        SET access_count = access_count + 1,
                            last_accessed = ?
                        WHERE doc_id IN ({id_placeholders})""",
                    [now] + list(doc_ids),
                )

            # Feedback loop: if any doc_ids match active random promotions,
            # mark them as used_in_output=1 (reinforcement signal)
            random_reinforced = 0
            if doc_ids:
                try:
                    placeholders = ",".join("?" for _ in doc_ids)
                    rc = conn.execute(
                        f"""UPDATE random_promotions
                            SET used_in_output = 1
                            WHERE doc_id IN ({placeholders})
                            AND demoted_at IS NULL
                            AND used_in_output = 0""",
                        doc_ids,
                    )
                    random_reinforced = rc.rowcount
                except Exception:
                    pass  # table may not exist yet

        result = {
            "ok": True,
            "context_hash": context_hash,
            "context_hash_short": context_hash[:16],
            "confidence_score": confidence_score,
            "confidence_level": confidence_level,
            "doc_count": len(doc_ids),
            "expanded": expanded,
            "promoted_to_hot": promoted_count,
            "promotion_cap": MAX_PROMOTIONS,
            "decay_applied": details.get("decay_applied", 0),
            "random_reinforced": random_reinforced,
            "provenance": build_provenance(config.homelab_host, []),
        }
    except Exception as exc:
        logger.error("Context mark failed: %s", exc, exc_info=True)
        result = error_response(
            "knowledge.context_mark",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Failed to write context_marks table"],
            suggested_next_tools=[{"tool": "knowledge.status", "args": {}}],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry(
        "knowledge.context_mark", args, [],
        truncate_text(str(result), config.audit_preview_limit)[0],
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def knowledge_reindex(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Run the incremental knowledge indexer script."""
    confirm = bool(args.get("confirm", False))
    dry_run = bool(args.get("dry_run", True))
    optimize = bool(args.get("optimize", False))
    vacuum = bool(args.get("vacuum", False))

    roots = _normalize_list(args.get("roots"))
    if not roots:
        roots = ["/opt/homelab-panel"]

    for root in roots:
        if not root.startswith("/"):
            result = error_response(
                "knowledge.reindex",
                f"Root path must be absolute: {root}",
                error_code="INVALID_ARGS",
                likely_causes=["Root path must be absolute"],
                suggested_next_tools=[{"tool": "knowledge.reindex", "args": {"roots": ["/opt/homelab-panel"]}}],
                host=config.homelab_host,
            )
            audit_entry = build_audit_entry("knowledge.reindex", args, [], "root not absolute", error_code="INVALID_ARGS")
            append_audit_log(config.audit_log_path, audit_entry)
            return result
        if not is_path_allowed(root, config.repo_roots):
            result = error_response(
                "knowledge.reindex",
                f"Root path not allowed: {root}",
                error_code="ALLOWLIST_VIOLATION",
                likely_causes=["Root path outside MCP_REPO_ROOTS allowlist"],
                suggested_next_tools=[{"tool": "meta.validate_config", "args": {}}],
                host=config.homelab_host,
            )
            audit_entry = build_audit_entry("knowledge.reindex", args, [], "root not allowlisted", error_code="ALLOWLIST_VIOLATION")
            append_audit_log(config.audit_log_path, audit_entry)
            return result

    if not confirm:
        dry_run = True

    script_path = "/opt/homelab-panel/scripts/knowledge/index_knowledge.py"
    if not Path(script_path).exists():
        result = error_response(
            "knowledge.reindex",
            f"Indexer script not found: {script_path}",
            error_code="PATH_MISSING",
            likely_causes=["Indexer script missing or not deployed"],
            suggested_next_tools=[{"tool": "scripts_list", "args": {"path": "/opt/homelab-panel/scripts"}}],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("knowledge.reindex", args, [], "script missing", error_code="PATH_MISSING")
        append_audit_log(config.audit_log_path, audit_entry)
        return result
    argv = ["python3", script_path, "--db", get_db_path("knowledge")]

    for root in roots:
        argv.extend(["--root", root])

    if dry_run:
        argv.append("--dry-run")
    else:
        argv.append("--apply")
        if optimize:
            argv.append("--optimize")
        if vacuum:
            argv.append("--vacuum")

    command_result = run_command(argv, output_limit=config.output_limit)
    preview = truncate_text(command_result.get("stdout", ""), config.output_limit)[0]

    result = {
        "ok": command_result.get("exit_code") == 0,
        "dry_run": dry_run,
        "confirm": confirm,
        "command": argv,
        "stdout": preview,
        "stderr": command_result.get("stderr", ""),
        "exit_code": command_result.get("exit_code"),
        "duration_ms": command_result.get("duration_ms"),
        "provenance": build_provenance(config.homelab_host, [command_result]),
    }

    audit_entry = build_audit_entry("knowledge.reindex", args, [command_result], truncate_text(str(result), config.audit_preview_limit)[0], error_code=result.get("error_code"))
    append_audit_log(config.audit_log_path, audit_entry)
    return result


# ---------------------------------------------------------------------------
# Critique #5: Resurrect — recover docs from the noise purgatory
# ---------------------------------------------------------------------------

def knowledge_resurrect(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """List or restore documents from the noise purgatory (documents_archive).

    Actions:
      - list: Show archived docs (default)
      - restore: Move a doc back into the main documents table by doc_id
    """
    action = args.get("action", "list")
    doc_id = args.get("doc_id")
    limit = min(int(args.get("limit", 20)), 100)
    dry_run = args.get("dry_run", True)
    confirm = args.get("confirm", False)

    try:
        with get_db_connection("knowledge") as conn:
            if action == "list":
                rows = conn.execute(
                    """SELECT doc_id, file_path, file_name, category, entity,
                              quality_score, temperature, noise_strikes,
                              archive_reason, archived_at
                       FROM documents_archive
                       ORDER BY archived_at DESC
                       LIMIT ?""",
                    (limit,),
                ).fetchall()
                return {
                    "ok": True,
                    "action": "list",
                    "count": len(rows),
                    "archived_docs": [dict(r) for r in rows],
                    "provenance": build_provenance(config.homelab_host, []),
                }

            elif action == "restore":
                if not doc_id:
                    return error_response("knowledge.resurrect", "doc_id required for restore action", error_code="MISSING_DOC_ID")

                row = conn.execute(
                    "SELECT * FROM documents_archive WHERE doc_id = ?",
                    (doc_id,),
                ).fetchone()
                if not row:
                    return error_response("knowledge.resurrect", f"doc_id {doc_id} not found in archive", error_code="NOT_FOUND")

                if dry_run and not confirm:
                    return {
                        "ok": True,
                        "action": "restore",
                        "dry_run": True,
                        "doc_id": doc_id,
                        "file_name": row["file_name"],
                        "archive_reason": row["archive_reason"],
                        "noise_strikes": row["noise_strikes"],
                        "message": "Would restore this doc. Set confirm=true to proceed.",
                    }

                # Restore: insert back into documents (reset noise state)
                col_names = [
                    "doc_id", "file_path", "file_name", "file_type",
                    "file_size", "last_modified", "file_hash",
                    "category", "entity", "year", "quality_score",
                    "ingest_status", "ingest_date", "chunk_count",
                    "tags", "notes",
                ]
                values = [row[c] for c in col_names]
                # Override: reset temperature to warm, clear noise
                col_names_ext = col_names + ["temperature", "noise_candidate", "access_count"]
                values_ext = values + ["warm", 0, 0]

                placeholders = ",".join("?" for _ in col_names_ext)
                cols = ",".join(col_names_ext)
                conn.execute(
                    f"INSERT OR REPLACE INTO documents ({cols}) VALUES ({placeholders})",
                    values_ext,
                )
                # Remove from archive
                conn.execute("DELETE FROM documents_archive WHERE doc_id = ?", (doc_id,))
                # Clear noise strikes in random_promotions
                conn.execute(
                    "DELETE FROM random_promotions WHERE doc_id = ?",
                    (doc_id,),
                )
                conn.commit()

                return {
                    "ok": True,
                    "action": "restore",
                    "dry_run": False,
                    "doc_id": doc_id,
                    "file_name": row["file_name"],
                    "new_temperature": "warm",
                    "message": f"Restored {row['file_name']} from archive to warm.",
                    "provenance": build_provenance(config.homelab_host, []),
                }
            else:
                return error_response("knowledge.resurrect", f"Unknown action: {action}", error_code="BAD_ACTION")

    except Exception as exc:
        return error_response("knowledge.resurrect", str(exc), error_code="DB_ERROR")


KNOWLEDGE_TOOLS = [
    {
        "name": "knowledge.status",
        "description": "Return knowledge.db path, counts, and latest timestamps.",
        "inputSchema": {"type": "object", "properties": {}},
        "handler": knowledge_status,
    },
    {
        "name": "knowledge.search",
        "description": "FTS5 search across knowledge.db only. Prefer memory.recall instead — it searches both knowledge.db and devloop in one call.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "FTS query string"},
                "category": {"type": ["string", "array"], "description": "Filter by document category"},
                "entity": {"type": ["string", "array"], "description": "Filter by entity"},
                "year": {"type": ["integer", "array"], "description": "Filter by year"},
                "file_type": {"type": ["string", "array"], "description": "Filter by file type"},
                "temperature": {"type": ["string", "array"], "description": "Filter by temperature (hot|warm|cold)"},
                "include_cold": {"type": "boolean", "default": False, "description": "Include cold docs if results are sparse"},
                "min_quality_score": {"type": "integer", "default": 30, "description": "Minimum quality score (0-100)"},
                "ingest_status": {"type": ["string", "array"], "description": "Filter by ingest status"},
                "limit": {"type": "integer", "default": 10, "description": "Max results"},
                "offset": {"type": "integer", "default": 0, "description": "Pagination offset"},
                "snippet_tokens": {"type": "integer", "default": 64, "description": "Snippet token count"},
            },
            "required": ["query"],
        },
        "handler": knowledge_search,
    },
    {
        "name": "knowledge.bootstrap_context",
        "description": "Return pinned, recent, and topic-based startup context from knowledge.db.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "topics": {"type": ["string", "array"], "description": "Topic queries for FTS search"},
                "pin_limit": {"type": "integer", "default": 5, "description": "Max docs per pin"},
                "recent_limit": {"type": "integer", "default": 8, "description": "Max recent docs"},
                "topic_limit": {"type": "integer", "default": 10, "description": "Max topic results total"},
                "snippet_tokens": {"type": "integer", "default": 64, "description": "Snippet token count"},
                "include_devloop": {"type": "boolean", "default": True, "description": "Include latest devloop summary"},
                "devloop_artifact_limit": {"type": "integer", "default": 2, "description": "Max devloop artifacts"},
            },
        },
        "handler": knowledge_bootstrap_context,
    },
    {
        "name": "knowledge.reindex",
        "description": "Run incremental knowledge indexing (dry run by default).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "roots": {"type": "array", "items": {"type": "string"}, "description": "Root paths to scan"},
                "dry_run": {"type": "boolean", "default": True, "description": "Preview changes without writing"},
                "confirm": {"type": "boolean", "default": False, "description": "Required to apply changes"},
                "optimize": {"type": "boolean", "default": False, "description": "Run FTS optimize after indexing"},
                "vacuum": {"type": "boolean", "default": False, "description": "Run VACUUM after indexing"},
            },
        },
        "handler": knowledge_reindex,
    },
    {
        "name": "knowledge.ocr_queue",
        "description": "Return prioritized list of OCR-needed PDFs with importance scores.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "default": 20, "description": "Max results to return"},
                "min_priority": {"type": "integer", "default": 0, "description": "Minimum priority score"},
            },
        },
        "handler": knowledge_ocr_queue,
    },
    {
        "name": "knowledge.context_mark",
        "description": "Record knowledge activation event (epistemic correction / sigil). Marks which documents were consulted, calculates confidence score using promotion/decay mechanics, and promotes high-quality docs to 'hot' for instant recall. Tracks whether stored corrections are actually being used.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The search query that produced these results"},
                "doc_ids": {"type": "array", "items": {"type": "string"}, "description": "List of doc_ids from search results that were used"},
                "category": {"type": "string", "description": "Category filter used (if any)"},
                "expanded": {"type": "boolean", "default": False, "description": "True if search was expanded after narrow filter returned empty"},
                "note": {"type": "string", "description": "Optional note about context usage"},
                "compliance": {"type": "object", "description": "Compliance flags: {did_step1_category_search: bool, used_workarounds_detected: bool}", "properties": {"did_step1_category_search": {"type": "boolean"}, "used_workarounds_detected": {"type": "boolean"}}},
            },
            "required": ["query"],
        },
        "handler": knowledge_context_mark,
    },
]
