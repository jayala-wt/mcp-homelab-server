"""
Decision Capture MCP tools for logging epistemic activation events.

Exposes the Decision Capture ledger (miss/win/ambiguous) as MCP tools
so models can log epistemic non-events directly during chat sessions.

Phase 1 of the Epistemic Trace system:
- No explanations, just structured fields
- Human detection remains (no automation)
- Fields: tool_expected, context_type, reason_unknown

Reference: OSF component uf5hn – "Defining the Epistemic Activation Problem"
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from typing import Any, Dict, List, Optional

from mcp_homelab.core import get_script_logger
from mcp_homelab.errors import error_response

from .util import append_audit_log, build_audit_entry, build_provenance

logger = get_script_logger(__name__)

INTENT_LABELS = ("STATEFUL", "VERIFY", "ACTION", "CONCEPTUAL")
TAGS = ("miss", "win", "ambiguous")
TOOL_ELIGIBLE_INTENTS = ("STATEFUL", "VERIFY", "ACTION")
DEFAULT_SOURCE = "mcp-chat"
DEFAULT_LIMIT = 20
MAX_LIMIT = 100

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "data", "decision_captures.db")


def _db_path() -> str:
    return os.environ.get("DECISION_CAPTURE_DB", DEFAULT_DB_PATH)


def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    """Ensure decision capture schema exists."""
    conn = _get_db()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_captures (
            id TEXT PRIMARY KEY,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            source TEXT NOT NULL DEFAULT 'mcp-chat',
            model_id TEXT NOT NULL,
            intent_label TEXT NOT NULL CHECK (intent_label IN ('STATEFUL','VERIFY','ACTION','CONCEPTUAL')),
            tag TEXT NOT NULL CHECK (tag IN ('miss','win','ambiguous')),
            user_prompt TEXT NOT NULL,
            outcome_summary TEXT NOT NULL,
            tools_expected TEXT NULL,
            tools_used TEXT NULL,
            reconstruction_detected INTEGER NOT NULL DEFAULT 0,
            decision_notes TEXT NULL,
            files_touched TEXT NULL,
            related_mcp_call_ids TEXT NULL,
            latency_ms INTEGER NULL,
            success INTEGER NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_decision_captures_created_at ON decision_captures(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_decision_captures_intent ON decision_captures(intent_label)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_decision_captures_tag ON decision_captures(tag)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_decision_captures_model ON decision_captures(model_id)")
    conn.commit()
    conn.close()


def _normalize_json_array(value: Any) -> Optional[str]:
    """Normalize a value into a JSON array string for storage."""
    if value is None:
        return None
    if isinstance(value, list):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        return json.dumps(cleaned) if cleaned else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return json.dumps([text])
        if isinstance(parsed, list):
            cleaned = [str(item).strip() for item in parsed if str(item).strip()]
            return json.dumps(cleaned) if cleaned else None
        return json.dumps([parsed])
    return json.dumps([value])


def _parse_json_array(value: Optional[str]) -> List[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass
    return []


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    data = dict(row)
    for field in ("tools_expected", "tools_used", "files_touched", "related_mcp_call_ids"):
        data[field] = _parse_json_array(data.get(field))
    data["reconstruction_detected"] = bool(data.get("reconstruction_detected"))
    if data.get("success") is not None:
        data["success"] = bool(data.get("success"))
    return data


def _metric_rates(tool_eligible_total: int, tool_adopted: int, reconstruction_only: int) -> Dict[str, float]:
    if tool_eligible_total <= 0:
        return {"tar": 0.0, "tmr": 0.0, "rr": 0.0}
    tar = tool_adopted / tool_eligible_total
    rr = reconstruction_only / tool_eligible_total
    tmr = 1 - tar
    return {"tar": round(tar, 4), "tmr": round(tmr, 4), "rr": round(rr, 4)}


# ── Tool handlers ──────────────────────────────────────────────────────────


def decision_capture_log(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Log a decision capture – an epistemic activation event (miss, win, or ambiguous).

    This is the primary tool for Phase 1 of the Epistemic Trace system.
    Call this when you detect a moment where tool use was relevant but wasn't
    activated, or where it was correctly activated.
    """
    _init_db()

    model_id = (args.get("model_id") or "").strip()
    intent_label = (args.get("intent_label") or "").strip().upper()
    tag = (args.get("tag") or "").strip().lower()
    user_prompt = (args.get("user_prompt") or "").strip()
    outcome_summary = (args.get("outcome_summary") or "").strip()

    # Validate required fields
    missing = []
    if not model_id:
        missing.append("model_id")
    if not intent_label:
        missing.append("intent_label")
    if not tag:
        missing.append("tag")
    if not user_prompt:
        missing.append("user_prompt")
    if not outcome_summary:
        missing.append("outcome_summary")

    if missing:
        result = error_response(
            "decision_capture.log",
            f"Missing required fields: {', '.join(missing)}",
            error_code="INVALID_ARGS",
            likely_causes=["Required fields not provided"],
            suggested_next_tools=[],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("decision_capture.log", args, [], "missing args", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if intent_label not in INTENT_LABELS:
        result = error_response(
            "decision_capture.log",
            f"Invalid intent_label '{intent_label}'. Allowed: {INTENT_LABELS}",
            error_code="INVALID_ARGS",
            likely_causes=["Unknown intent label"],
            suggested_next_tools=[],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("decision_capture.log", args, [], "invalid intent", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    if tag not in TAGS:
        result = error_response(
            "decision_capture.log",
            f"Invalid tag '{tag}'. Allowed: {TAGS}",
            error_code="INVALID_ARGS",
            likely_causes=["Unknown tag value"],
            suggested_next_tools=[],
            host=config.homelab_host,
        )
        audit_entry = build_audit_entry("decision_capture.log", args, [], "invalid tag", error_code="INVALID_ARGS")
        append_audit_log(config.audit_log_path, audit_entry)
        return result

    # Optional fields
    source = (args.get("source") or DEFAULT_SOURCE).strip()
    tools_expected = _normalize_json_array(args.get("tools_expected"))
    tools_used = _normalize_json_array(args.get("tools_used"))
    reconstruction_detected = 1 if args.get("reconstruction_detected") else 0
    decision_notes = (args.get("decision_notes") or "").strip() or None
    files_touched = _normalize_json_array(args.get("files_touched"))
    related_mcp_call_ids = _normalize_json_array(args.get("related_mcp_call_ids"))

    latency_ms = None
    if args.get("latency_ms") is not None:
        try:
            latency_ms = int(args["latency_ms"])
        except (TypeError, ValueError):
            pass

    success = None
    if args.get("success") is not None:
        success = 1 if args["success"] else 0

    capture_id = str(uuid.uuid4())

    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO decision_captures (
                id, source, model_id, intent_label, tag, user_prompt, outcome_summary,
                tools_expected, tools_used, reconstruction_detected, decision_notes,
                files_touched, related_mcp_call_ids, latency_ms, success
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                capture_id, source, model_id, intent_label, tag,
                user_prompt, outcome_summary, tools_expected, tools_used,
                reconstruction_detected, decision_notes, files_touched,
                related_mcp_call_ids, latency_ms, success,
            ),
        )
        conn.commit()
        conn.close()

        result = {
            "ok": True,
            "capture_id": capture_id,
            "tag": tag,
            "intent_label": intent_label,
            "model_id": model_id,
            "message": f"Epistemic {tag} logged: {outcome_summary[:80]}",
            "provenance": build_provenance(config.homelab_host, []),
        }

    except Exception as exc:
        logger.error("Decision capture log failed: %s", exc, exc_info=True)
        result = error_response(
            "decision_capture.log",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Database write error"],
            suggested_next_tools=[{"tool": "decision_capture.list", "args": {}}],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry(
        "decision_capture.log",
        args,
        [],
        f"tag={tag} intent={intent_label}" if result.get("ok") else str(result.get("error_code")),
        error_code=result.get("error_code"),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def decision_capture_list(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """List recent decision captures with optional filters."""
    _init_db()

    tag = (args.get("tag") or "").strip().lower() or None
    intent = (args.get("intent") or "").strip().upper() or None
    model_id = (args.get("model_id") or "").strip() or None

    try:
        limit = int(args.get("limit", DEFAULT_LIMIT))
    except (TypeError, ValueError):
        limit = DEFAULT_LIMIT
    limit = max(1, min(limit, MAX_LIMIT))

    conditions: List[str] = []
    params: List[Any] = []

    if tag and tag in TAGS:
        conditions.append("tag = ?")
        params.append(tag)
    if intent and intent in INTENT_LABELS:
        conditions.append("intent_label = ?")
        params.append(intent)
    if model_id:
        conditions.append("model_id = ?")
        params.append(model_id)

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""

    try:
        conn = _get_db()

        count_row = conn.execute(
            f"SELECT COUNT(*) as count FROM decision_captures{where_clause}", params
        ).fetchone()
        total = count_row["count"] if count_row else 0

        rows = conn.execute(
            f"SELECT * FROM decision_captures{where_clause} ORDER BY datetime(created_at) DESC LIMIT ?",
            params + [limit],
        ).fetchall()
        conn.close()

        items = [_row_to_dict(row) for row in rows]

        result = {
            "ok": True,
            "total": total,
            "returned": len(items),
            "items": items,
            "provenance": build_provenance(config.homelab_host, []),
        }

    except Exception as exc:
        logger.error("Decision capture list failed: %s", exc, exc_info=True)
        result = error_response(
            "decision_capture.list",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Database read error"],
            suggested_next_tools=[],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry(
        "decision_capture.list",
        args,
        [],
        f"total={result.get('total', '?')}" if result.get("ok") else str(result.get("error_code")),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


def decision_capture_metrics(config, args: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get epistemic activation metrics: TAR, TMR, RR.

    - TAR (Tool Adoption Rate): % of tool-eligible prompts that used tools
    - TMR (Tool Miss Rate): 1 - TAR
    - RR (Reconstruction Rate): % where model reconstructed instead of querying
    """
    _init_db()

    model_id = (args.get("model_id") or "").strip() or None
    from_date = (args.get("from_date") or "").strip() or None
    to_date = (args.get("to_date") or "").strip() or None

    conditions: List[str] = []
    params: List[Any] = []

    if from_date:
        conditions.append("date(created_at) >= date(?)")
        params.append(from_date)
    if to_date:
        conditions.append("date(created_at) <= date(?)")
        params.append(to_date)
    if model_id:
        conditions.append("model_id = ?")
        params.append(model_id)

    where_clause = f" WHERE {' AND '.join(conditions)}" if conditions else ""

    placeholders = ",".join("?" for _ in TOOL_ELIGIBLE_INTENTS)
    tool_eligible_clause = f"intent_label IN ({placeholders})"
    tool_eligible_params = list(TOOL_ELIGIBLE_INTENTS)

    try:
        conn = _get_db()

        totals_row = conn.execute(
            f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN {tool_eligible_clause} THEN 1 ELSE 0 END) as tool_eligible_total,
                SUM(CASE WHEN {tool_eligible_clause}
                    AND tools_used IS NOT NULL
                    AND trim(tools_used) NOT IN ('', '[]')
                    THEN 1 ELSE 0 END) as tool_adopted,
                SUM(CASE WHEN {tool_eligible_clause}
                    AND reconstruction_detected = 1
                    AND (tools_used IS NULL OR trim(tools_used) IN ('', '[]'))
                    THEN 1 ELSE 0 END) as reconstruction_only
            FROM decision_captures{where_clause}
            """,
            tool_eligible_params + tool_eligible_params + tool_eligible_params + params,
        ).fetchone()

        total = totals_row["total"] or 0
        tool_eligible_total = totals_row["tool_eligible_total"] or 0
        tool_adopted = totals_row["tool_adopted"] or 0
        reconstruction_only = totals_row["reconstruction_only"] or 0

        rates = _metric_rates(tool_eligible_total, tool_adopted, reconstruction_only)
        tor = round(tool_eligible_total / total, 4) if total else 0.0

        # Counts by tag
        tag_counts = {t: 0 for t in TAGS}
        for row in conn.execute(
            f"SELECT tag, COUNT(*) as count FROM decision_captures{where_clause} GROUP BY tag",
            params,
        ).fetchall():
            tag_counts[row["tag"]] = row["count"]

        # Counts by intent
        intent_counts = {label: 0 for label in INTENT_LABELS}
        for row in conn.execute(
            f"SELECT intent_label, COUNT(*) as count FROM decision_captures{where_clause} GROUP BY intent_label",
            params,
        ).fetchall():
            intent_counts[row["intent_label"]] = row["count"]

        conn.close()

        result = {
            "ok": True,
            "total": total,
            "tool_eligible_total": tool_eligible_total,
            "tor": tor,
            "tar": rates["tar"],
            "tmr": rates["tmr"],
            "rr": rates["rr"],
            "counts_by_tag": tag_counts,
            "counts_by_intent": intent_counts,
            "interpretation": {
                "tar": f"Tool Adoption Rate: {rates['tar']:.1%} of tool-eligible prompts used tools",
                "tmr": f"Tool Miss Rate: {rates['tmr']:.1%} of tool-eligible prompts missed tool use",
                "rr": f"Reconstruction Rate: {rates['rr']:.1%} of tool-eligible prompts reconstructed instead",
            },
            "provenance": build_provenance(config.homelab_host, []),
        }

    except Exception as exc:
        logger.error("Decision capture metrics failed: %s", exc, exc_info=True)
        result = error_response(
            "decision_capture.metrics",
            str(exc),
            error_code="UNKNOWN",
            likely_causes=["Database read error"],
            suggested_next_tools=[{"tool": "decision_capture.list", "args": {}}],
            host=config.homelab_host,
        )

    audit_entry = build_audit_entry(
        "decision_capture.metrics",
        args,
        [],
        f"total={result.get('total', '?')}" if result.get("ok") else str(result.get("error_code")),
    )
    append_audit_log(config.audit_log_path, audit_entry)
    return result


# ── Tool definitions ───────────────────────────────────────────────────────

DECISION_CAPTURE_TOOLS = [
    {
        "name": "decision_capture.log",
        "description": (
            "Log an epistemic activation event (miss, win, or ambiguous). "
            "Call when you detect a moment where MCP tool use was relevant but "
            "wasn't activated (miss), was correctly used (win), or unclear (ambiguous). "
            "This is the core tool for the Epistemic Trace system."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_id": {
                    "type": "string",
                    "description": "Model that produced the response (e.g. sonnet, opus, codex, gpt52)",
                },
                "intent_label": {
                    "type": "string",
                    "enum": ["STATEFUL", "VERIFY", "ACTION", "CONCEPTUAL"],
                    "description": "STATEFUL=needed live data, VERIFY=needed confirmation, ACTION=needed to do something, CONCEPTUAL=pure reasoning",
                },
                "tag": {
                    "type": "string",
                    "enum": ["miss", "win", "ambiguous"],
                    "description": "miss=tool should have been used but wasn't, win=tool was correctly used, ambiguous=unclear",
                },
                "user_prompt": {
                    "type": "string",
                    "description": "The user prompt or question that triggered this event",
                },
                "outcome_summary": {
                    "type": "string",
                    "description": "Brief description of what happened (e.g. 'Reconstructed tool list from memory instead of querying')",
                },
                "tools_expected": {
                    "type": ["string", "array"],
                    "description": "Tool(s) that should have been used (e.g. 'knowledge_status', 'devloop.latest')",
                },
                "tools_used": {
                    "type": ["string", "array"],
                    "description": "Tool(s) that were actually used (empty array for misses)",
                },
                "reconstruction_detected": {
                    "type": "boolean",
                    "description": "True if the model reconstructed/guessed instead of querying",
                },
                "decision_notes": {
                    "type": "string",
                    "description": "Optional notes on why the miss/win happened",
                },
                "source": {
                    "type": "string",
                    "description": "Source of the capture (default: mcp-chat)",
                },
            },
            "required": ["model_id", "intent_label", "tag", "user_prompt", "outcome_summary"],
        },
        "handler": decision_capture_log,
    },
    {
        "name": "decision_capture.list",
        "description": "List recent epistemic activation captures with optional filters by tag, intent, or model.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "tag": {
                    "type": "string",
                    "enum": ["miss", "win", "ambiguous"],
                    "description": "Filter by tag",
                },
                "intent": {
                    "type": "string",
                    "enum": ["STATEFUL", "VERIFY", "ACTION", "CONCEPTUAL"],
                    "description": "Filter by intent label",
                },
                "model_id": {
                    "type": "string",
                    "description": "Filter by model ID",
                },
                "limit": {
                    "type": "integer",
                    "default": 20,
                    "description": "Max results (1-100)",
                },
            },
        },
        "handler": decision_capture_list,
    },
    {
        "name": "decision_capture.metrics",
        "description": (
            "Get epistemic activation metrics: TAR (Tool Adoption Rate), "
            "TMR (Tool Miss Rate), RR (Reconstruction Rate). "
            "Shows how often tools were used vs missed across captures."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_id": {
                    "type": "string",
                    "description": "Filter metrics by model ID",
                },
                "from_date": {
                    "type": "string",
                    "description": "Start date (YYYY-MM-DD)",
                },
                "to_date": {
                    "type": "string",
                    "description": "End date (YYYY-MM-DD)",
                },
            },
        },
        "handler": decision_capture_metrics,
    },
]
