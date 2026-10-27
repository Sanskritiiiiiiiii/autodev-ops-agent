import ast
import os
from pathlib import Path

from app.ingestion.models import CodeChunk, IngestResult, SkippedFile

EXCLUDED_DIRS = {
    ".git", "__pycache__", ".venv", "venv", "node_modules",
    ".tox", ".mypy_cache", ".pytest_cache", "build", "dist",
}
_FUNC_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)


def _span(node) -> tuple[int, int]:
    """1-based inclusive line span, extended upward to include decorators."""
    start = node.lineno
    for dec in node.decorator_list:
        start = min(start, dec.lineno)
    return start, node.end_lineno


def _chunk(file_path, chunk_type, name, qualified_name, class_name, start, end, content):
    return CodeChunk(
        chunk_id=f"{file_path}::{qualified_name}@{start}",
        file_path=file_path,
        chunk_type=chunk_type,
        name=name,
        qualified_name=qualified_name,
        class_name=class_name,
        start_line=start,
        end_line=end,
        content=content,
    )


def _class_content(lines: list[str], node: ast.ClassDef) -> str:
    """Class text minus method bodies; consecutive blank lines collapsed."""
    start, end = _span(node)
    covered: set[int] = set()
    for child in node.body:
        if isinstance(child, _FUNC_NODES):
            s, e = _span(child)
            covered.update(range(s, e + 1))
    out: list[str] = []
    for n in range(start, end + 1):
        if n in covered:
            continue
        line = lines[n - 1]
        if line.strip() == "" and out and out[-1].strip() == "":
            continue
        out.append(line)
    return "\n".join(out).rstrip()


def chunk_source(file_path: str, source: str) -> list[CodeChunk]:
    """Chunk one Python source string. Raises SyntaxError on invalid code."""
    tree = ast.parse(source)
    lines = source.split("\n")
    chunks: list[CodeChunk] = []

    for node in tree.body:
        if isinstance(node, _FUNC_NODES):
            s, e = _span(node)
            chunks.append(_chunk(file_path, "function", node.name, node.name,
                                 None, s, e, "\n".join(lines[s - 1:e])))
        elif isinstance(node, ast.ClassDef):
            s, e = _span(node)
            chunks.append(_chunk(file_path, "class", node.name, node.name,
                                 None, s, e, _class_content(lines, node)))
            for child in node.body:
                if isinstance(child, _FUNC_NODES):
                    ms, me = _span(child)
                    chunks.append(_chunk(
                        file_path, "method", child.name,
                        f"{node.name}.{child.name}", node.name,
                        ms, me, "\n".join(lines[ms - 1:me]),
                    ))
    return chunks


def ingest_repo(repo_root: str | Path) -> IngestResult:
    """Walk a repository and chunk every .py file, in deterministic order."""
    root = Path(repo_root).resolve()
    if not root.is_dir():
        raise NotADirectoryError(f"Not a directory: {root}")

    py_files: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in EXCLUDED_DIRS)
        for fname in sorted(filenames):
            if fname.endswith(".py"):
                py_files.append(Path(dirpath) / fname)

    chunks: list[CodeChunk] = []
    skipped: list[SkippedFile] = []
    for path in py_files:
        rel = path.relative_to(root).as_posix()
        try:
            source = path.read_text(encoding="utf-8-sig")
            chunks.extend(chunk_source(rel, source))
        except (SyntaxError, UnicodeDecodeError, ValueError) as e:
            skipped.append(SkippedFile(rel, f"{type(e).__name__}: {e}"))
    return IngestResult(chunks=chunks, skipped=skipped)