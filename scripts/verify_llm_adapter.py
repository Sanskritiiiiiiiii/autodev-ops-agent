"""Module 5B offline verification: Groq adapter against a fake client."""
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.agent.llm_groq import GroqLLM, LLMOutputError, make_groq_llm  # noqa: E402

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def tool_response(name: str, args) -> SimpleNamespace:
    raw = args if isinstance(args, str) else json.dumps(args)
    call = SimpleNamespace(function=SimpleNamespace(name=name, arguments=raw))
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(tool_calls=[call]))])


NO_TOOL = SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(tool_calls=None))])


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls: list[dict] = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self._create))

    def _create(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


def raises(fn) -> bool:
    try:
        fn()
    except LLMOutputError:
        return True
    return False


CHUNKS = [
    {"chunk_id": "db::acquire", "chunk_type": "method", "qualified_name": "ConnectionPool.acquire",
     "name": "acquire", "file_path": "shop/db.py", "start_line": 19, "end_line": 26,
     "content": "    def acquire(self):\n        return self"},
    {"chunk_id": "db::pool", "chunk_type": "class", "qualified_name": "ConnectionPool",
     "name": "ConnectionPool", "file_path": "shop/db.py", "start_line": 10, "end_line": 29,
     "content": "class ConnectionPool:"},
    {"chunk_id": "db::run", "chunk_type": "function", "qualified_name": "run_query",
     "name": "run_query", "file_path": "shop/db.py", "start_line": 32, "end_line": 37,
     "content": "def run_query(pool, sql):\n    return 1"},
]

# ---- analyze ----
c = FakeClient([tool_response("submit_analysis", {"hypothesis": " Pool exhausted. ", "search_query": " ERR_DB_TIMEOUT acquire "})])
out = GroqLLM(c, "test-model").analyze("some log")
check("analyze parses and trims tool arguments",
      out == {"hypothesis": "Pool exhausted.", "search_query": "ERR_DB_TIMEOUT acquire"}, str(out))
kw = c.calls[0]
check("analyze request: model, temperature 0, forced tool",
      kw["model"] == "test-model" and kw["temperature"] == 0
      and kw["tool_choice"] == {"type": "function", "function": {"name": "submit_analysis"}}, str(kw["tool_choice"]))
check("analyze: no tool call raises LLMOutputError", raises(lambda: GroqLLM(FakeClient([NO_TOOL]), "m").analyze("x")))
check("analyze: invalid JSON raises LLMOutputError",
      raises(lambda: GroqLLM(FakeClient([tool_response("submit_analysis", "{not json")]), "m").analyze("x")))
check("analyze: missing field raises LLMOutputError",
      raises(lambda: GroqLLM(FakeClient([tool_response("submit_analysis", {"hypothesis": "h"})]), "m").analyze("x")))

# ---- draft_fix ----
fenced = "```python\ndef acquire(self):\n    return self\n```"
c = FakeClient([
    tool_response("submit_fix", {"hypothesis": "h2", "chunk_id": "db::acquire", "new_code": fenced}),
    tool_response("submit_fix", {"hypothesis": "h3", "chunk_id": "db::acquire", "new_code": "def acquire(self):\n    return 1"}),
])
llm = GroqLLM(c, "test-model")
out = llm.draft_fix("LOGTEXT ERR_DB_TIMEOUT", "h1", CHUNKS, None)
check("draft: parsed result with markdown fences stripped",
      out == {"hypothesis": "h2", "chunk_id": "db::acquire", "new_code": "def acquire(self):\n    return self"}, str(out))
enum = c.calls[0]["tools"][0]["function"]["parameters"]["properties"]["chunk_id"]["enum"]
check("draft: tool enum lists only function/method chunks", enum == ["db::acquire", "db::run"], str(enum))
prompt1 = c.calls[0]["messages"][1]["content"]
check("draft: prompt contains log, chunk id and file location",
      "LOGTEXT ERR_DB_TIMEOUT" in prompt1 and "db::acquire" in prompt1 and "shop/db.py lines 19-26" in prompt1)
check("draft: no 'rejected' section without feedback", "rejected" not in prompt1)
llm.draft_fix("log", "h1", CHUNKS, "SyntaxError: expected ':' (line 1)")
prompt2 = c.calls[1]["messages"][1]["content"]
check("draft: feedback included on retry",
      "Previous attempt was rejected" in prompt2 and "SyntaxError: expected ':' (line 1)" in prompt2)

out = GroqLLM(FakeClient([NO_TOOL]), "m").draft_fix("log", "h1", CHUNKS, None)
check("draft: malformed output becomes an empty fix (no exception)",
      out == {"hypothesis": "h1", "chunk_id": None, "new_code": None}, str(out))

c = FakeClient([])
out = GroqLLM(c, "m").draft_fix("log", "h1", [CHUNKS[1]], None)
check("draft: no fixable chunks -> no API call, empty fix",
      c.calls == [] and out["chunk_id"] is None and out["new_code"] is None)

c = FakeClient([tool_response("submit_fix", {"hypothesis": "h", "chunk_id": "db::acquire", "new_code": "def acquire(self):\n    pass"})])
GroqLLM(c, "m").draft_fix("x" * 50_000 + "TAIL_MARKER", "h1", CHUNKS, None)
p = c.calls[0]["messages"][1]["content"]
check("long log is truncated but keeps the tail", "TAIL_MARKER" in p and len(p) < 20_000, str(len(p)))

# ---- factory ----
saved = {k: os.environ.pop(k, None) for k in ("GROQ_API_KEY", "GROQ_MODEL")}
try:
    try:
        make_groq_llm()
        check("factory: missing API key raises", False)
    except RuntimeError as e:
        check("factory: missing API key raises", "GROQ_API_KEY" in str(e), str(e))
    os.environ["GROQ_API_KEY"] = "dummy"
    try:
        make_groq_llm()
        check("factory: missing model raises", False)
    except RuntimeError as e:
        check("factory: missing model raises", "GROQ_MODEL" in str(e), str(e))
finally:
    for k, v in saved.items():
        os.environ.pop(k, None)
        if v is not None:
            os.environ[k] = v

print("\nMODULE 5B (offline): ALL CHECKS PASSED" if not failures else f"\nMODULE 5B (offline): FAILED -> {failures}")
sys.exit(1 if failures else 0)