MAX_LOG_CHARS = 12_000
MAX_CHUNK_CHARS = 6_000

ANALYZE_SYSTEM = (
    "You are an incident triage engineer. You receive a production log or stack trace "
    "from a Python service. Call submit_analysis exactly once.\n"
    "- hypothesis: 1-3 sentences stating the most likely root cause, grounded in the log.\n"
    "- search_query: a short query (max 12 words) for searching the codebase. Put exact "
    "identifiers first: error codes or constants, exception class names, and function or "
    "method names from the stack frames. Do not include file system paths or line numbers."
)

FIX_SYSTEM = (
    "You are a senior Python engineer fixing a production bug. You are given the error log, "
    "a root-cause hypothesis, and code chunks retrieved from the repository. Call submit_fix "
    "exactly once.\n"
    "Rules:\n"
    "- chunk_id must be the id of the single function or method you are changing, taken from "
    "the provided chunks.\n"
    "- new_code must be the complete replacement for that entire function or method: the same "
    "name, the full body, valid Python. Do not add imports or change other functions.\n"
    "- new_code must be plain code only: no diff syntax, no explanations, no markdown fences.\n"
    "- If a previous attempt was rejected you will be told why; correct that specific problem.\n"
    "- hypothesis: update it if the code shows a more precise root cause."
)

ANALYZE_TOOL = {
    "type": "function",
    "function": {
        "name": "submit_analysis",
        "description": "Submit the root-cause hypothesis and a codebase search query.",
        "parameters": {
            "type": "object",
            "properties": {
                "hypothesis": {"type": "string", "description": "Likely root cause, 1-3 sentences."},
                "search_query": {"type": "string", "description": "Short code search query, identifiers first."},
            },
            "required": ["hypothesis", "search_query"],
        },
    },
}


def build_fix_tool(chunk_ids: list[str]) -> dict:
    return {
        "type": "function",
        "function": {
            "name": "submit_fix",
            "description": "Submit replacement code for exactly one function or method.",
            "parameters": {
                "type": "object",
                "properties": {
                    "hypothesis": {"type": "string", "description": "Updated root cause, 1-3 sentences."},
                    "chunk_id": {"type": "string", "enum": list(chunk_ids),
                                 "description": "Id of the function or method being replaced."},
                    "new_code": {"type": "string",
                                 "description": "Complete replacement function or method, plain code only."},
                },
                "required": ["hypothesis", "chunk_id", "new_code"],
            },
        },
    }


def _tail(text: str, limit: int) -> str:
    return text if len(text) <= limit else "[...truncated...]\n" + text[-limit:]


def _head(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[:limit] + "\n# [...truncated...]"


def build_analyze_prompt(log: str) -> str:
    return f"## Error log\n```\n{_tail(log, MAX_LOG_CHARS)}\n```"


def build_fix_prompt(log: str, hypothesis: str, chunks: list[dict], feedback: str | None) -> str:
    parts = [
        f"## Error log\n```\n{_tail(log, MAX_LOG_CHARS)}\n```",
        f"## Hypothesis\n{hypothesis}",
        "## Retrieved code chunks",
    ]
    for c in chunks:
        parts.append(
            f"### chunk_id: {c['chunk_id']}\n"
            f"type: {c['chunk_type']} | name: {c['qualified_name']} | "
            f"file: {c['file_path']} lines {c['start_line']}-{c['end_line']}\n"
            f"```python\n{_head(c['content'], MAX_CHUNK_CHARS)}\n```"
        )
    if feedback:
        parts.append(
            f"## Previous attempt was rejected\n{feedback}\n"
            "Produce a corrected fix that resolves exactly this problem."
        )
    return "\n\n".join(parts)