import difflib
import re
from dataclasses import dataclass
from pathlib import Path

from app.agent.tools import _reindent

_JOB_ID = re.compile(r"^[A-Za-z0-9_-]+$")


class PatchError(Exception):
    """A patch could not be built safely (stale index, unsupported file, bad path)."""


@dataclass(frozen=True)
class PatchedFile:
    rel_path: str
    qualified_name: str
    start_line: int
    end_line: int
    original_text: str
    new_text: str
    patch: str


def _split_keepends(text: str) -> list[str]:
    """Split on '\\n' only, keeping line endings (including any '\\r')."""
    parts = text.split("\n")
    lines = [p + "\n" for p in parts[:-1]]
    if parts[-1] != "":
        lines.append(parts[-1])  # final line without a trailing newline
    return lines


def _diff_lines(old: list[str], new: list[str], rel: str):
    for line in difflib.unified_diff(old, new, fromfile=f"a/{rel}", tofile=f"b/{rel}", n=3):
        yield line if line.endswith("\n") else line + "\n\\ No newline at end of file\n"


def build_patched_file(repo_root: Path, chunk: dict, new_code: str) -> PatchedFile:
    """Apply the fix to the real file on disk (in memory) and build its git-style patch."""
    root = Path(repo_root).resolve()
    rel = chunk["file_path"]
    target = (root / rel).resolve()
    if target != root and root not in target.parents:
        raise PatchError(f"path escapes the repository: {rel}")
    if not target.is_file():
        raise PatchError(f"file not found: {rel}")

    raw = target.read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raise PatchError(f"files with a UTF-8 BOM are not supported: {rel}")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PatchError(f"file is not valid UTF-8: {rel}") from exc

    lines = _split_keepends(text)
    start, end = chunk["start_line"], chunk["end_line"]
    if end > len(lines):
        raise PatchError(f"{rel} is shorter than when it was indexed; re-ingest the repository")
    region = lines[start - 1:end]
    current = "".join(region).replace("\r\n", "\n").rstrip("\n")
    if current != chunk["content"]:
        raise PatchError(
            f"{rel} changed since it was indexed (lines {start}-{end} differ); re-ingest the repository"
        )

    eol = "\r\n" if region[0].endswith("\r\n") else "\n"
    new_text = _reindent(new_code.replace("\r\n", "\n"), chunk["content"])
    new_region = [ln + eol for ln in new_text.split("\n")]
    if not region[-1].endswith("\n"):  # chunk ends the file with no trailing newline
        new_region[-1] = new_region[-1][: -len(eol)]

    new_lines = lines[:start - 1] + new_region + lines[end:]
    if new_lines == lines:
        raise PatchError("the fix produces no change")
    patch = f"diff --git a/{rel} b/{rel}\n" + "".join(_diff_lines(lines, new_lines, rel))
    return PatchedFile(
        rel_path=rel, qualified_name=chunk["qualified_name"], start_line=start, end_line=end,
        original_text="".join(lines), new_text="".join(new_lines), patch=patch,
    )


def build_patch(repo_root: Path, chunk: dict, new_code: str) -> str:
    return build_patched_file(repo_root, chunk, new_code).patch


def write_patch(patch_dir: Path, job_id: str, patch: str) -> Path:
    """Write <patch_dir>/<job_id>.patch as bytes, so no newline translation happens."""
    if not _JOB_ID.match(job_id):
        raise PatchError(f"invalid job id for a patch file name: {job_id!r}")
    patch_dir = Path(patch_dir)
    patch_dir.mkdir(parents=True, exist_ok=True)
    path = patch_dir / f"{job_id}.patch"
    path.write_bytes(patch.encode("utf-8"))
    return path