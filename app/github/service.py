import os
import re
from pathlib import Path

from app.github.client import DEFAULT_API, GitHubClient, GitHubError
from app.github.pr import PRError, create_draft_pr
from app.patch.builder import PatchError
from app.patch.service import build_for_state

_OWNER_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_NONE = {"pr_url": None, "pr_number": None, "pr_error": None}


def parse_repo_map(raw: str) -> dict[str, str]:
    """Parse 'repo_a=owner/repo,repo_b=owner/other' into a dict."""
    mapping: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        name, sep, target = part.partition("=")
        name, target = name.strip(), target.strip()
        if not sep or not name or not _OWNER_REPO.match(target):
            raise ValueError(f"invalid AUTODEV_GITHUB_REPOS entry: {part!r} (expected name=owner/repo)")
        mapping[name] = target
    return mapping


def maybe_create_pr(state: dict, repo_name: str, job_id: str,
                    environ=None, repos_dir: Path | None = None) -> dict:
    """Create a draft PR if GitHub is configured for this repo. Never raises."""
    env = os.environ if environ is None else environ
    token, raw_map = env.get("GITHUB_TOKEN"), env.get("AUTODEV_GITHUB_REPOS", "")
    if not token or not raw_map or state.get("evaluation_status") != "passed":
        return dict(_NONE)
    try:
        target = parse_repo_map(raw_map).get(repo_name)
    except ValueError as exc:
        return {**_NONE, "pr_error": str(exc)}
    if not target:
        return dict(_NONE)

    try:
        patched = build_for_state(state, repo_name, repos_dir)
        client = GitHubClient(token, env.get("AUTODEV_GITHUB_API", DEFAULT_API))
        pr = create_draft_pr(client, target, env.get("AUTODEV_GITHUB_BASE_BRANCH", "main"),
                             job_id, patched, state["hypothesis"], state["attempts"])
    except (PatchError, PRError, GitHubError) as exc:
        return {**_NONE, "pr_error": str(exc)}
    return {"pr_url": pr["url"], "pr_number": pr["number"], "pr_error": None}