import base64
import urllib.parse

from app.github.client import GitHubClient, GitHubError
from app.patch.builder import PatchedFile

MAX_PATCH_IN_BODY = 20_000
MAX_HYPOTHESIS_IN_BODY = 5_000


class PRError(Exception):
    """The PR could not be created safely (for example the remote file differs)."""


def _norm(text: str) -> str:
    return text.replace("\r\n", "\n")


def build_pr_body(job_id: str, patched: PatchedFile, hypothesis: str, attempts: int) -> str:
    patch, note = patched.patch, ""
    if len(patch) > MAX_PATCH_IN_BODY:
        patch, note = patch[:MAX_PATCH_IN_BODY], "\n\n_(patch truncated; see the commit for the full change)_"
    return (
        "## Automated incident triage (AutoDev-Ops Agent)\n\n"
        f"**Job:** `{job_id}`  \n"
        f"**Verified in:** {attempts} attempt(s) (syntax and compile check only; tests were not run)\n\n"
        "### Root cause hypothesis\n\n"
        f"{hypothesis[:MAX_HYPOTHESIS_IN_BODY]}\n\n"
        "### Change\n\n"
        f"Replaces `{patched.qualified_name}` in `{patched.rel_path}` "
        f"(lines {patched.start_line}-{patched.end_line}).\n\n"
        "### Patch\n\n````diff\n" + patch + "\n````" + note + "\n\n"
        "> Generated automatically. Review carefully before merging."
    )


def _get_file(client: GitHubClient, repo: str, path: str, ref: str) -> tuple[str, str]:
    """Return (blob_sha, decoded_text) of a file at a branch."""
    _, data = client.request(
        "GET",
        f"/repos/{repo}/contents/{urllib.parse.quote(path, safe='/')}?ref={urllib.parse.quote(ref, safe='')}",
    )
    return data["sha"], base64.b64decode(data["content"]).decode("utf-8")


def create_draft_pr(client: GitHubClient, repo: str, base: str, job_id: str,
                    patched: PatchedFile, hypothesis: str, attempts: int) -> dict:
    """Create (or find) the draft PR for a job. Returns {"url", "number", "existing"}."""
    owner = repo.split("/", 1)[0]
    branch = f"autodev/{job_id}"
    quoted_path = urllib.parse.quote(patched.rel_path, safe="/")

    # 1. idempotency: a PR for this job's branch may already exist
    query = urllib.parse.urlencode({"head": f"{owner}:{branch}", "state": "all"})
    _, pulls = client.request("GET", f"/repos/{repo}/pulls?{query}")
    if pulls:
        return {"url": pulls[0]["html_url"], "number": pulls[0]["number"], "existing": True}

    # 2-3. base commit and remote file; refuse (with no side effects) if it differs from local
    _, ref = client.request("GET", f"/repos/{repo}/git/ref/heads/{urllib.parse.quote(base, safe='/')}")
    base_sha = ref["object"]["sha"]
    blob_sha, remote = _get_file(client, repo, patched.rel_path, base)
    if _norm(remote) != _norm(patched.original_text):
        raise PRError(f"{patched.rel_path} on '{base}' differs from the local checkout; refusing to commit")

    new_text = _norm(patched.new_text)
    if "\r\n" in remote:  # follow the remote file's line-ending style
        new_text = new_text.replace("\n", "\r\n")

    # 4. create the branch, or resume if it already exists
    skip_commit = False
    try:
        client.request("POST", f"/repos/{repo}/git/refs", {"ref": f"refs/heads/{branch}", "sha": base_sha})
    except GitHubError as exc:
        if exc.status != 422:
            raise
        blob_sha, branch_text = _get_file(client, repo, patched.rel_path, branch)
        if _norm(branch_text) == _norm(patched.new_text):
            skip_commit = True  # the fix was already committed by an earlier attempt
        elif _norm(branch_text) != _norm(patched.original_text):
            raise PRError(f"branch {branch} already exists with unexpected content") from None

    # 5. commit the patched file
    if not skip_commit:
        client.request("PUT", f"/repos/{repo}/contents/{quoted_path}", {
            "message": f"AutoDev-Ops: fix {patched.qualified_name}\n\nJob {job_id}",
            "content": base64.b64encode(new_text.encode("utf-8")).decode("ascii"),
            "sha": blob_sha,
            "branch": branch,
        })

    # 6. open the draft PR
    _, pr = client.request("POST", f"/repos/{repo}/pulls", {
        "title": f"AutoDev-Ops: fix {patched.qualified_name}",
        "head": branch,
        "base": base,
        "body": build_pr_body(job_id, patched, hypothesis, attempts),
        "draft": True,
    })
    return {"url": pr["html_url"], "number": pr["number"], "existing": False}