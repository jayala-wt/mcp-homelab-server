import hashlib
import json
import sqlite3
import uuid
from typing import Any, Dict, List

from mcp_homelab.core import get_db_connection, get_script_logger
from mcp_homelab.errors import error_response

from .util import append_audit_log, build_audit_entry, build_provenance, slice_lines, truncate_text

logger = get_script_logger(__name__)


def _hash_content(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def devloop_run_start(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Create a new devloop run entry."""
    title = (args.get("title") or "").strip() or None
    goal = (args.get("goal") or "").strip() or None
    origin = (args.get("origin") or "").strip() or None
    status = (args.get("status") or "").strip() or "open"
    tags = (args.get("tags") or "").strip() or None

    run_id = str(uuid.uuid4())

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO devloop_runs (run_id, title, goal, origin, status, tags)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, title, goal, origin, status, tags),
            )

        result = {
            "ok": True,
            "run_id": run_id,
            "status": status,
            "provenance": build_provenance(config.wanatux_host, []),
        }
    except Exception as exc:
        logger.error("Devloop run start failed: %s", exc, exc_info=True)
        result = error_response(
            "devloop.run_start",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error writing devloop_runs"],
            suggested_next_tools=[{"tool": "devloop.latest", "args": {}}],
            host=config.wanatux_host,
        )

    audit_entry = build_audit_entry(
        "devloop.run_start",
        args,
        [],
        truncate_text(str(result), config.audit_preview_limit)[0],
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def devloop_add_artifact(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Add a devloop artifact with content dedupe."""
    run_id = (args.get("run_id") or "").strip()
    artifact_type = (args.get("artifact_type") or "").strip()
    content_raw = args.get("content")
    content = "" if content_raw is None else str(content_raw)
    model = (args.get("model") or "").strip() or None
    meta_json = (args.get("meta_json") or "").strip() or None
    hash_value = (args.get("hash") or "").strip()

    if not run_id or not artifact_type or not content.strip():
        result = error_response(
            "devloop.add_artifact",
            "run_id, artifact_type, and content are required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing required arguments"],
            suggested_next_tools=[{"tool": "devloop.run_start", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("devloop.add_artifact", args, [], "missing args", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not hash_value:
        hash_value = _hash_content(content)

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT run_id FROM devloop_runs WHERE run_id = ?", (run_id,))
            if not cursor.fetchone():
                result = error_response(
                    "devloop.add_artifact",
                    f"run_id not found: {run_id}",
                    error_code="NOT_FOUND",
                    likely_causes=["Unknown run_id"],
                    suggested_next_tools=[{"tool": "devloop.run_start", "args": {}}],
                    host=config.wanatux_host,
                )
                audit_entry = build_audit_entry("devloop.add_artifact", args, [], "run_id not found", error_code="NOT_FOUND")
                append_audit_log(config.audit_log_path, audit_entry)
                return result

            cursor.execute(
                "SELECT artifact_id FROM devloop_artifacts WHERE run_id = ? AND hash = ?",
                (run_id, hash_value),
            )
            existing = cursor.fetchone()
            if existing:
                artifact_id = existing["artifact_id"]
                result = {
                    "ok": True,
                    "artifact_id": artifact_id,
                    "run_id": run_id,
                    "deduped": True,
                    "hash": hash_value,
                    "provenance": build_provenance(config.wanatux_host, []),
                }
                audit_entry = build_audit_entry(
                    "devloop.add_artifact",
                    args,
                    [],
                    truncate_text(str(result), config.audit_preview_limit)[0],
                    error_code=result.get("error_code"),
                )
                append_audit_log(config.audit_log_path, audit_entry)
                return result

            cursor.execute(
                """
                INSERT INTO devloop_artifacts (
                    run_id, artifact_type, model, content, hash, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (run_id, artifact_type, model, content, hash_value, meta_json),
            )
            artifact_id = cursor.lastrowid

        result = {
            "ok": True,
            "artifact_id": artifact_id,
            "run_id": run_id,
            "deduped": False,
            "hash": hash_value,
            "provenance": build_provenance(config.wanatux_host, []),
        }
    except Exception as exc:
        logger.error("Devloop add artifact failed: %s", exc, exc_info=True)
        result = error_response(
            "devloop.add_artifact",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error writing devloop_artifacts"],
            suggested_next_tools=[{"tool": "devloop.latest", "args": {}}],
            host=config.wanatux_host,
        )

    audit_entry = build_audit_entry(
        "devloop.add_artifact",
        args,
        [],
        truncate_text(str(result), config.audit_preview_limit)[0],
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def devloop_log(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Create a devloop run (if needed) and add a log artifact in one call."""
    run_id = (args.get("run_id") or "").strip()
    title = (args.get("title") or "").strip() or None
    goal = (args.get("goal") or "").strip() or None
    origin = (args.get("origin") or "").strip() or None
    status = (args.get("status") or "").strip() or "open"
    tags = (args.get("tags") or "").strip() or None

    artifact_type = (args.get("artifact_type") or "").strip() or "codex_summary"
    content_raw = args.get("content")
    if content_raw is None:
        content_raw = args.get("message")
    if content_raw is None:
        content_raw = args.get("summary")
    content = "" if content_raw is None else str(content_raw)
    model = (args.get("model") or "").strip() or None
    meta_json = (args.get("meta_json") or "").strip() or None
    hash_value = (args.get("hash") or "").strip()

    if not content.strip():
        result = error_response(
            "devloop.log",
            "content is required (use content, message, or summary)",
            error_code="INVALID_ARGS",
            likely_causes=["Missing log content"],
            suggested_next_tools=[{"tool": "devloop.latest", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("devloop.log", args, [], "missing content", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if not hash_value:
        hash_value = _hash_content(content)

    created_run = False
    deduped = False
    artifact_id = None

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            if not run_id:
                run_id = str(uuid.uuid4())
                cursor.execute(
                    """
                    INSERT INTO devloop_runs (run_id, title, goal, origin, status, tags)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, title, goal, origin, status, tags),
                )
                created_run = True
            else:
                cursor.execute("SELECT run_id FROM devloop_runs WHERE run_id = ?", (run_id,))
                if not cursor.fetchone():
                    cursor.execute(
                        """
                        INSERT INTO devloop_runs (run_id, title, goal, origin, status, tags)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (run_id, title, goal, origin, status, tags),
                    )
                    created_run = True

            cursor.execute(
                "SELECT artifact_id FROM devloop_artifacts WHERE run_id = ? AND hash = ?",
                (run_id, hash_value),
            )
            existing = cursor.fetchone()
            if existing:
                artifact_id = existing["artifact_id"]
                deduped = True
            else:
                cursor.execute(
                    """
                    INSERT INTO devloop_artifacts (
                        run_id, artifact_type, model, content, hash, meta_json
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (run_id, artifact_type, model, content, hash_value, meta_json),
                )
                artifact_id = cursor.lastrowid

        result = {
            "ok": True,
            "run_id": run_id,
            "artifact_id": artifact_id,
            "artifact_type": artifact_type,
            "created_run": created_run,
            "deduped": deduped,
            "hash": hash_value,
            "provenance": build_provenance(config.wanatux_host, []),
        }
    except Exception as exc:
        logger.error("Devloop log failed: %s", exc, exc_info=True)
        result = error_response(
            "devloop.log",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error writing devloop tables"],
            suggested_next_tools=[{"tool": "devloop.latest", "args": {}}],
            host=config.wanatux_host,
        )

    audit_entry = build_audit_entry(
        "devloop.log",
        args,
        [],
        truncate_text(str(result), config.audit_preview_limit)[0],
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def devloop_latest(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Return the latest devloop run and a few artifacts."""
    artifact_limit = int(args.get("artifact_limit", 2))
    artifact_limit = max(1, min(artifact_limit, 5))
    artifact_types = args.get("artifact_types") or ["codex_summary", "audit", "decision", "digest"]
    if not isinstance(artifact_types, list):
        artifact_types = [artifact_types]
    artifact_types = [str(value).strip() for value in artifact_types if str(value).strip()]

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT run_id, created_at, title, goal, origin, status, tags
                FROM devloop_runs
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            run_row = cursor.fetchone()
            if not run_row:
                result = {
                    "ok": True,
                    "run": None,
                    "artifacts": [],
                    "provenance": build_provenance(config.wanatux_host, []),
                }
            else:
                artifacts: List[Dict[str, Any]] = []
                if artifact_types:
                    placeholders = ",".join("?" for _ in artifact_types)
                    cursor.execute(
                        f"""
                        SELECT artifact_id, artifact_type, model, content, created_at,
                               COALESCE(temperature, 'hot') AS temperature
                        FROM devloop_artifacts
                        WHERE run_id = ?
                          AND artifact_type IN ({placeholders})
                          AND COALESCE(temperature, 'hot') != 'cold'
                        ORDER BY created_at DESC
                        LIMIT ?
                        """,
                        (run_row["run_id"], *artifact_types, artifact_limit),
                    )
                    for artifact_row in cursor.fetchall():
                        content = artifact_row["content"] or ""
                        content, _ = truncate_text(content, 800)
                        artifacts.append({
                            "artifact_id": artifact_row["artifact_id"],
                            "artifact_type": artifact_row["artifact_type"],
                            "model": artifact_row["model"],
                            "created_at": artifact_row["created_at"],
                            "content": content,
                            "temperature": artifact_row["temperature"],
                        })

                result = {
                    "ok": True,
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
                    "provenance": build_provenance(config.wanatux_host, []),
                }
    except Exception as exc:
        logger.error("Devloop latest failed: %s", exc, exc_info=True)
        result = error_response(
            "devloop.latest",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error reading devloop tables"],
            suggested_next_tools=[{"tool": "devloop.run_start", "args": {}}],
            host=config.wanatux_host,
        )

    audit_entry = build_audit_entry(
        "devloop.latest",
        args,
        [],
        truncate_text(str(result), config.audit_preview_limit)[0],
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def devloop_search(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Search devloop artifacts content with LIKE."""
    query = (args.get("query") or "").strip()
    if not query:
        result = error_response(
            "devloop.search",
            "query is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing query argument"],
            suggested_next_tools=[{"tool": "devloop.latest", "args": {}}],
            host=config.wanatux_host,
        )
        audit_entry = build_audit_entry("devloop.search", args, [], "query missing", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    limit = int(args.get("limit", 10))
    limit = max(1, min(limit, 50))
    artifact_types = args.get("artifact_type")
    artifact_types = [str(value).strip() for value in _normalize_types(artifact_types)]

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            params: List[Any] = [f"%{query}%"]
            filters = ["a.content LIKE ?"]
            if artifact_types:
                placeholders = ",".join("?" for _ in artifact_types)
                filters.append(f"a.artifact_type IN ({placeholders})")
                params.extend(artifact_types)
            where_clause = " AND ".join(filters)
            cursor.execute(
                f"""
                SELECT
                    a.artifact_id,
                    a.run_id,
                    a.artifact_type,
                    a.model,
                    a.created_at,
                    r.title AS run_title,
                    r.status AS run_status,
                    a.content,
                    COALESCE(a.temperature, 'hot') AS temperature
                FROM devloop_artifacts a
                LEFT JOIN devloop_runs r ON a.run_id = r.run_id
                WHERE {where_clause}
                ORDER BY a.created_at DESC
                LIMIT ?
                """,
                (*params, limit),
            )
            rows = cursor.fetchall()

        results = []
        for row in rows:
            content = row["content"] or ""
            content, _ = truncate_text(content, 600)
            results.append({
                "artifact_id": row["artifact_id"],
                "run_id": row["run_id"],
                "artifact_type": row["artifact_type"],
                "model": row["model"],
                "created_at": row["created_at"],
                "run_title": row["run_title"],
                "run_status": row["run_status"],
                "content_preview": content,
                "temperature": row["temperature"],
            })

        result = {
            "ok": True,
            "query": query,
            "count": len(results),
            "results": results,
            "provenance": build_provenance(config.wanatux_host, []),
        }
    except Exception as exc:
        logger.error("Devloop search failed: %s", exc, exc_info=True)
        result = error_response(
            "devloop.search",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected error searching devloop artifacts"],
            suggested_next_tools=[{"tool": "devloop.latest", "args": {}}],
            host=config.wanatux_host,
        )

    audit_entry = build_audit_entry(
        "devloop.search",
        args,
        [],
        truncate_text(str(result), config.audit_preview_limit)[0],
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def _normalize_types(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = [value]
    return [str(item).strip() for item in raw if str(item).strip()]


def devloop_get_artifact(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch full content of a devloop artifact by ID, with optional line-range slicing."""
    artifact_id = args.get("artifact_id")
    if not artifact_id:
        return error_response(
            "devloop.get_artifact",
            "artifact_id is required",
            error_code="INVALID_ARGS",
            likely_causes=["Missing artifact_id argument"],
            suggested_next_tools=[{"tool": "devloop.search", "args": {"query": "..."}}],
            host=config.wanatux_host,
        )

    try:
        artifact_id = int(artifact_id)
    except (ValueError, TypeError):
        return error_response(
            "devloop.get_artifact",
            "artifact_id must be an integer",
            error_code="INVALID_ARGS",
            likely_causes=["artifact_id must be numeric"],
            host=config.wanatux_host,
        )

    start_line = int(args.get("start_line", 0))
    max_lines = args.get("max_lines")
    if max_lines is not None:
        max_lines = int(max_lines)

    try:
        with get_db_connection("knowledge") as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT a.artifact_id, a.run_id, a.artifact_type, a.model,
                       a.content, a.created_at, a.meta_json,
                       COALESCE(a.temperature, 'hot') AS temperature,
                       a.original_chars, a.compacted_at,
                       r.title AS run_title, r.goal AS run_goal,
                       r.origin AS run_origin, r.tags AS run_tags
                FROM devloop_artifacts a
                LEFT JOIN devloop_runs r ON a.run_id = r.run_id
                WHERE a.artifact_id = ?
                """,
                (artifact_id,),
            )
            row = cursor.fetchone()

        if not row:
            return error_response(
                "devloop.get_artifact",
                f"Artifact {artifact_id} not found",
                error_code="NOT_FOUND",
                likely_causes=["artifact_id does not exist"],
                suggested_next_tools=[{"tool": "devloop.search", "args": {"query": "..."}}],
                host=config.wanatux_host,
            )

        meta = {}
        if row["meta_json"]:
            try:
                meta = json.loads(row["meta_json"])
            except (json.JSONDecodeError, TypeError):
                pass

        full_content = row["content"] or ""
        total_lines = full_content.count("\n") + 1 if full_content else 0

        # Apply line-range slice if requested
        if start_line > 0 or max_lines is not None:
            content, truncated = slice_lines(full_content, start_line, max_lines)
        else:
            content = full_content
            truncated = False

        result = {
            "ok": True,
            "artifact": {
                "artifact_id": row["artifact_id"],
                "run_id": row["run_id"],
                "artifact_type": row["artifact_type"],
                "model": row["model"],
                "content": content,
                "content_chars": len(content),
                "total_chars": len(full_content),
                "total_lines": total_lines,
                "start_line": start_line,
                "max_lines": max_lines,
                "truncated": truncated,
                "created_at": row["created_at"],
                "temperature": row["temperature"],
                "original_chars": row["original_chars"],
                "compacted_at": row["compacted_at"],
                "meta": meta,
                "run": {
                    "title": row["run_title"],
                    "goal": row["run_goal"],
                    "origin": row["run_origin"],
                    "tags": row["run_tags"],
                },
            },
            "provenance": build_provenance(config.wanatux_host, []),
        }

        # Warn if this is a cold artifact (content is a pointer, not the original)
        if row["temperature"] == "cold":
            result["warning"] = (
                "This artifact is COLD — content is a pointer to an externalized file. "
                "Read the raw file at the path shown in content to get original data."
            )

    except Exception as exc:
        logger.error("Devloop get_artifact failed: %s", exc, exc_info=True)
        result = error_response(
            "devloop.get_artifact",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Unexpected DB error"],
            host=config.wanatux_host,
        )

    audit_entry = build_audit_entry(
        "devloop.get_artifact",
        args,
        [],
        f"artifact_id={artifact_id} chars={len(row['content'] or '') if 'row' in dir() else 0}",
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


DEVLOOP_TOOLS = [
    {
        "name": "devloop.run_start",
        "description": "Create a devloop run entry.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "title": {"type": "string", "description": "Short run title"},
                "goal": {"type": "string", "description": "Run goal"},
                "origin": {"type": "string", "description": "Origin (gpt52|codex|sonnet|manual)"},
                "status": {"type": "string", "description": "Run status"},
                "tags": {"type": "string", "description": "Comma-separated tags"},
            },
        },
        "handler": devloop_run_start,
    },
    {
        "name": "devloop.add_artifact",
        "description": "Add a devloop artifact with hash dedupe.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "Devloop run id"},
                "artifact_type": {"type": "string", "description": "Artifact type"},
                "model": {"type": "string", "description": "Model name"},
                "content": {"type": "string", "description": "Artifact content"},
                "hash": {"type": "string", "description": "Optional sha256 hash"},
                "meta_json": {"type": "string", "description": "Optional JSON metadata"},
            },
            "required": ["run_id", "artifact_type", "content"],
        },
        "handler": devloop_add_artifact,
    },
    {
        "name": "devloop.log",
        "description": "Create a devloop run (if needed) and add a log artifact.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string", "description": "Existing run id (optional)"},
                "title": {"type": "string", "description": "Short run title"},
                "goal": {"type": "string", "description": "Run goal"},
                "origin": {"type": "string", "description": "Origin (gpt52|codex|sonnet|manual)"},
                "status": {"type": "string", "description": "Run status"},
                "tags": {"type": "string", "description": "Comma-separated tags"},
                "artifact_type": {"type": "string", "description": "Artifact type"},
                "content": {"type": "string", "description": "Log content"},
                "message": {"type": "string", "description": "Alias for content"},
                "summary": {"type": "string", "description": "Alias for content"},
                "model": {"type": "string", "description": "Model name"},
                "hash": {"type": "string", "description": "Optional sha256 hash"},
                "meta_json": {"type": "string", "description": "Optional JSON metadata"},
            },
        },
        "handler": devloop_log,
    },
    {
        "name": "devloop.latest",
        "description": "Return the latest devloop run and key artifacts. For general search use memory.recall instead.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "artifact_types": {"type": ["string", "array"], "description": "Artifact types to include"},
                "artifact_limit": {"type": "integer", "default": 2, "description": "Max artifacts returned"},
            },
        },
        "handler": devloop_latest,
    },
    {
        "name": "devloop.search",
        "description": "Keyword search across devloop artifacts only. Prefer memory.recall instead — it searches both devloop and knowledge.db in one call.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search keyword"},
                "artifact_type": {"type": ["string", "array"], "description": "Filter by artifact type"},
                "limit": {"type": "integer", "default": 10, "description": "Max results"},
            },
            "required": ["query"],
        },
        "handler": devloop_search,
    },
    {
        "name": "devloop.get_artifact",
        "description": (
            "Fetch content of a devloop artifact by ID with optional line-range slicing. "
            "Use when devloop_search or devloop_latest returned a truncated content_preview. "
            "Returns total_lines so you can paginate: call again with start_line + max_lines to read the next window."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "artifact_id": {"type": "integer", "description": "The artifact_id to fetch"},
                "start_line": {"type": "integer", "default": 0, "description": "First line to return (0-indexed). Omit to start from the beginning."},
                "max_lines": {"type": "integer", "description": "Max lines to return. Omit for full content. Use with start_line to paginate large artifacts."},
            },
            "required": ["artifact_id"],
        },
        "handler": devloop_get_artifact,
    },
]
