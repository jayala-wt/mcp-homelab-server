"""
Memory Recall Tools — unified search across devloop + knowledge.db.

Exposes two MCP tools:
    memory.recall  — search both stores, merge/rank, return recall_id
    memory.confirm — reinforce used results (access_count++, promotion)
"""

from mcp_homelab.memory_recall import memory_recall_search, memory_confirm_recall


# ── Handlers (called by MCP server dispatcher) ──────────────────────────────

def memory_recall(config, args: dict) -> dict:
    """Unified memory search across devloop + knowledge.db."""
    query = args.get("query", "")
    origin = args.get("origin")
    limit = args.get("limit", 10)
    mode = args.get("mode", "balanced")
    category = args.get("category")
    return memory_recall_search(query=query, origin=origin, limit=limit, mode=mode, category=category)


def memory_confirm(config, args: dict) -> dict:
    """Confirm a recall — reinforce the results that were actually used."""
    recall_id = args.get("recall_id", "")
    origin = args.get("origin")
    return memory_confirm_recall(recall_id=recall_id, origin=origin)


# ── Tool definitions ─────────────────────────────────────────────────────────

MEMORY_TOOLS = [
    {
        "name": "memory.recall",
        "description": (
            "PRIMARY SEARCH TOOL — use this first before knowledge.search or devloop.search. "
            "Searches both session memory (devloop) and long-term knowledge (knowledge.db) in one call. "
            "Returns merged ranked results. Required param: query (string)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (used for FTS on knowledge.db and LIKE on devloop). Long compound queries are auto-split into sub-terms.",
                },
                "origin": {
                    "type": "string",
                    "description": "Which model/source is searching (claude, chatgpt, codex, manual)",
                },
                "limit": {
                    "type": "integer",
                    "default": 10,
                    "description": "Max results to return (1-50)",
                },
                "mode": {
                    "type": "string",
                    "enum": ["balanced", "recent", "reference"],
                    "default": "balanced",
                    "description": "Source weighting: balanced (default), recent (prefer devloop session memory), reference (prefer knowledge.db long-term docs)",
                },
                "category": {
                    "type": "string",
                    "description": "Filter knowledge.db results by category (e.g. devloop_digest, sigil, documentation, financial). Devloop results not affected.",
                },
            },
            "required": ["query"],
        },
        "handler": memory_recall,
    },
    {
        "name": "memory.confirm",
        "description": (
            "Confirm a memory recall — reinforce results that were actually useful. "
            "Knowledge docs get access_count++ and possible warm→hot promotion. "
            "Devloop artifacts get recall_used_count++. "
            "Also closes the sigil reinforcement loop: writes to context_marks table "
            "and triggers Ebbinghaus decay on stale hot docs. "
            "Call this after using results from memory.recall to strengthen the memory."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "recall_id": {
                    "type": "string",
                    "description": "The recall_id returned by memory.recall",
                },
                "origin": {
                    "type": "string",
                    "description": "Which model/source is confirming (claude, chatgpt, codex, manual)",
                },
            },
            "required": ["recall_id"],
        },
        "handler": memory_confirm,
    },
]
