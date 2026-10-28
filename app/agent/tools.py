import ast
import difflib
import re
import textwrap

FIXABLE_TYPES = ("function", "method")
_CHUNK_KEYS = ("chunk_id", "file_path", "chunk_type", "name", "qualified_name",
               "start_line", "end_line", "content")


def retrieve_code_context(retriever, query: str, top_k: int = 3) -> list[dict]:
    """Tool 1: hybrid search (Module 3) returning plain dicts for the agent."""
    hits = retriever.search(query, top_k=top_k)
    return [
        {**{k: h.chunk[k] for k in _CHUNK_KEYS}, "rerank_score": h.rerank_score}
        for h in hits
    ]


def verify_syntax_and_lint(code: str, expected_name: str) -> dict:
    """Tool 2: AST + compile check of replacement code.
    Returns {"ok": bool, "errors": [str]}."""
    src = textwrap.dedent(code)
    try:
        tree = ast.parse(src)
    except SyntaxError as exc:
        return {"ok": False, "errors": [f"SyntaxError: {exc.msg} (line {exc.lineno})"]}

    errors: list[str] = []
    body = tree.body
    if len(body) != 1 or not isinstance(body[0], (ast.FunctionDef, ast.AsyncFunctionDef)):
        errors.append("code must contain exactly one top-level function definition")
    elif body[0].name != expected_name:
        errors.append(f"function name must remain '{expected_name}', got '{body[0].name}'")

    if not errors:
        try:
            compile(src, "<proposed_fix>", "exec")
        except (SyntaxError, ValueError) as exc:
            errors.append(f"CompileError: {exc}")
    return {"ok": not errors, "errors": errors}


def check_fix(fix: dict | None, chunks: list[dict]) -> dict:
    """Validate a proposed fix against the retrieved chunks, then verify its code."""
    if not fix or not fix.get("chunk_id") or not fix.get("new_code"):
        return {"ok": False, "errors": ["proposed_fix must contain 'chunk_id' and 'new_code'"]}
    target = next((c for c in chunks if c["chunk_id"] == fix["chunk_id"]), None)
    if target is None:
        return {"ok": False, "errors": [f"chunk_id '{fix['chunk_id']}' is not among the retrieved chunks"]}
    if target["chunk_type"] not in FIXABLE_TYPES:
        return {"ok": False, "errors": ["only function or method chunks can be replaced"]}

    result = verify_syntax_and_lint(fix["new_code"], target["name"])
    if result["ok"] and (textwrap.dedent(fix["new_code"]).strip()
                         == textwrap.dedent(target["content"]).strip()):
        return {"ok": False, "errors": ["proposed fix is identical to the original code"]}
    return result


def _reindent(new_code: str, original: str) -> str:
    first = original.split("\n", 1)[0]
    indent = first[: len(first) - len(first.lstrip())]
    return textwrap.indent(textwrap.dedent(new_code).strip("\n"), indent)


_HUNK = re.compile(r"^@@ -(\d+)(,\d+)? \+(\d+)(,\d+)? @@")


def build_diff_preview(chunk: dict, new_code: str) -> str:
    """Unified diff of one chunk, hunk line numbers shifted to real file lines.
    Context is limited to the chunk; Module 7 builds the real patch."""
    old_lines = chunk["content"].split("\n")
    new_lines = _reindent(new_code, chunk["content"]).split("\n")
    offset = chunk["start_line"] - 1
    out: list[str] = []
    for line in difflib.unified_diff(
        old_lines, new_lines,
        fromfile=f"a/{chunk['file_path']}", tofile=f"b/{chunk['file_path']}", lineterm="",
    ):
        m = _HUNK.match(line)
        if m:
            line = (f"@@ -{int(m[1]) + offset}{m[2] or ''} "
                    f"+{int(m[3]) + offset}{m[4] or ''} @@" + line[m.end():])
        out.append(line)
    return "\n".join(out)