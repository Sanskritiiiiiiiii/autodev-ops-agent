import re
from pathlib import Path

import app.config as config
from app.patch.builder import PatchError, build_patch, write_patch

_REPO_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def make_patch_for_state(state: dict, repo_name: str, job_id: str,
                         repos_dir: Path | None = None, patch_dir: Path | None = None) -> dict:
    """Return {"patch", "patch_path", "patch_error"}; all None unless the fix passed."""
    if state.get("evaluation_status") != "passed":
        return {"patch": None, "patch_path": None, "patch_error": None}

    repos_dir = Path(repos_dir or config.REPOS_DIR)
    patch_dir = Path(patch_dir or config.PATCH_DIR)
    try:
        if not _REPO_NAME.match(repo_name) or repo_name in (".", ".."):
            raise PatchError(f"invalid repo name: {repo_name!r}")
        repo_root = repos_dir / repo_name
        if not repo_root.is_dir():
            raise PatchError(f"repository directory not found: {repo_root}")
        fix = state["proposed_fix"]
        chunk = next((c for c in state["retrieved_chunks"] if c["chunk_id"] == fix["chunk_id"]), None)
        if chunk is None:
            raise PatchError("fixed chunk is missing from the retrieved chunks")
        patch = build_patch(repo_root, chunk, fix["new_code"])
        path = write_patch(patch_dir, job_id, patch)
    except PatchError as exc:
        return {"patch": None, "patch_path": None, "patch_error": str(exc)}
    return {"patch": patch, "patch_path": str(path), "patch_error": None}