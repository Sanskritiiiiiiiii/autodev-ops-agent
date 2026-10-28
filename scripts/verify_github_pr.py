"""Module 7B verification: draft PR creation against a fake GitHub API."""
import asyncio
import base64
import dataclasses
import hashlib
import http.server
import io
import json
import os
import shutil
import socket
import sys
import tempfile
import threading
import urllib.parse
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import redis.asyncio as aioredis  # noqa: E402
from rich.console import Console  # noqa: E402

import app.config as config  # noqa: E402
from app.cli.render import EventRenderer  # noqa: E402
from app.db.chunk_store import init_schema, init_vector_index, replace_repo_chunks  # noqa: E402
from app.db.connection import get_connection  # noqa: E402
from app.github.client import GitHubClient, GitHubError  # noqa: E402
from app.github.pr import PRError, build_pr_body, create_draft_pr  # noqa: E402
from app.github.service import maybe_create_pr, parse_repo_map  # noqa: E402
from app.ingestion.ast_chunker import ingest_repo  # noqa: E402
from app.patch.builder import build_patch, build_patched_file  # noqa: E402
from app.queue.events import events_key  # noqa: E402
from app.queue.jobs import enqueue_job, job_key  # noqa: E402
from app.queue.triage_handler import make_triage_handler  # noqa: E402
from app.search.dense_search import embed_missing_chunks  # noqa: E402
from app.search.embedder import Embedder  # noqa: E402
from app.search.reranker import Reranker  # noqa: E402

TOKEN, REPO, FILE, JOB = "test-token", "testuser/sample_repo", "shop/db.py", "c" * 32
BRANCH = f"autodev/{JOB}"
ORIGINAL_BYTES = (ROOT / "sample_repo" / "shop" / "db.py").read_bytes().replace(b"\r\n", b"\n")
NEW_CODE = (
    "def acquire(self, timeout=5.0):\n"
    "    deadline = time.monotonic() + timeout\n"
    "    while self.in_use >= self.max_size:\n"
    "        if time.monotonic() > deadline:\n"
    "            raise DatabaseTimeout(ERR_DB_TIMEOUT)\n"
    "        time.sleep(0.01)\n"
    "    self.in_use += 1\n"
    "    return self\n"
)
BAD_SYNTAX = "def acquire(self, timeout=5.0)\n    return self\n"
LOG = (
    "Traceback (most recent call last):\n"
    '  File "shop/db.py", line 23, in acquire\n'
    "shop.db.DatabaseTimeout: ERR_DB_TIMEOUT"
)
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def lf_copy_of_sample(dest: Path) -> Path:
    shutil.copytree(ROOT / "sample_repo", dest, ignore=shutil.ignore_patterns("__pycache__"))
    for p in dest.rglob("*.py"):
        p.write_bytes(p.read_bytes().replace(b"\r\n", b"\n"))
    return dest


# ---------------------------- fake GitHub API ----------------------------
class FakeGitHub:
    def __init__(self):
        self.token = TOKEN
        self.reset()

    def reset(self, main_content: bytes | None = None) -> None:
        self.base_sha = "b" * 40
        content = ORIGINAL_BYTES if main_content is None else main_content
        self.branches = {"main": {"commit": self.base_sha, "files": {FILE: content}}}
        self.pulls: list[dict] = []
        self.commits: list[str] = []
        self.requests: list[tuple] = []

    @staticmethod
    def blob_sha(content: bytes) -> str:
        return hashlib.sha1(content).hexdigest()


class GitHubHandler(http.server.BaseHTTPRequestHandler):
    def _send(self, status: int, obj) -> None:
        raw = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _handle(self, method: str) -> None:
        fake: FakeGitHub = self.server.fake
        split = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(split.path)
        query = urllib.parse.parse_qs(split.query)
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else None
        prefix = f"/repos/{REPO}"
        rest = path[len(prefix):] if path.startswith(prefix) else path
        fake.requests.append((method, rest, query, body, self.headers.get("Authorization")))

        if self.headers.get("Authorization") != f"Bearer {fake.token}":
            return self._send(401, {"message": "Bad credentials"})
        if not path.startswith(prefix):
            return self._send(404, {"message": "Not Found"})

        if method == "GET" and rest == "/pulls":
            ref = query["head"][0].split(":", 1)[1]
            return self._send(200, [p for p in fake.pulls if p["head"]["ref"] == ref])
        if method == "GET" and rest.startswith("/git/ref/heads/"):
            name = rest[len("/git/ref/heads/"):]
            br = fake.branches.get(name)
            if not br:
                return self._send(404, {"message": "Not Found"})
            return self._send(200, {"ref": f"refs/heads/{name}", "object": {"sha": br["commit"]}})
        if method == "POST" and rest == "/git/refs":
            name = body["ref"][len("refs/heads/"):]
            if name in fake.branches:
                return self._send(422, {"message": "Reference already exists"})
            fake.branches[name] = {"commit": body["sha"], "files": dict(fake.branches["main"]["files"])}
            return self._send(201, {"ref": body["ref"]})
        if rest.startswith("/contents/"):
            fpath = rest[len("/contents/"):]
            if method == "GET":
                br = fake.branches.get(query["ref"][0])
                if not br or fpath not in br["files"]:
                    return self._send(404, {"message": "Not Found"})
                content = br["files"][fpath]
                return self._send(200, {"sha": fake.blob_sha(content), "path": fpath, "encoding": "base64",
                                        "content": base64.encodebytes(content).decode()})
            if method == "PUT":
                br = fake.branches.get(body["branch"])
                if not br or fpath not in br["files"]:
                    return self._send(404, {"message": "Not Found"})
                if body["sha"] != fake.blob_sha(br["files"][fpath]):
                    return self._send(409, {"message": "sha does not match"})
                br["files"][fpath] = base64.b64decode(body["content"])
                fake.commits.append(body["message"])
                return self._send(200, {"commit": {"sha": "c" * 40}})
        if method == "POST" and rest == "/pulls":
            number = 7 + len(fake.pulls)
            pr = {"number": number, "html_url": f"https://github.com/{REPO}/pull/{number}",
                  "title": body["title"], "body": body["body"], "head": {"ref": body["head"]},
                  "base": {"ref": body["base"]}, "draft": body.get("draft", False)}
            fake.pulls.append(pr)
            return self._send(201, pr)
        return self._send(404, {"message": "Not Found"})

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_PUT(self):
        self._handle("PUT")

    def log_message(self, *args):
        pass


fake = FakeGitHub()
server = http.server.ThreadingHTTPServer(("127.0.0.1", free_port()), GitHubHandler)
server.fake = fake
threading.Thread(target=server.serve_forever, daemon=True).start()
API = f"http://127.0.0.1:{server.server_address[1]}"


def seq() -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in fake.requests]


NORMAL_SEQ = [("GET", "/pulls"), ("GET", "/git/ref/heads/main"), ("GET", f"/contents/{FILE}"),
              ("POST", "/git/refs"), ("PUT", f"/contents/{FILE}"), ("POST", "/pulls")]

chunks = {c.qualified_name: c.to_dict() for c in ingest_repo(ROOT / "sample_repo").chunks}
acq = chunks["ConnectionPool.acquire"]

with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_s:
    tmp = Path(tmp_s)
    repo = lf_copy_of_sample(tmp / "lf" / "sample_repo")
    patched = build_patched_file(repo, acq, NEW_CODE)
    client = GitHubClient(TOKEN, API)

    # ---------- builder refactor ----------
    check("build_patched_file: metadata and texts",
          patched.rel_path == FILE and patched.qualified_name == "ConnectionPool.acquire"
          and (patched.start_line, patched.end_line) == (19, 26)
          and patched.original_text.encode() == ORIGINAL_BYTES and "timeout=5.0" in patched.new_text)
    check("build_patch still returns the same patch text", build_patch(repo, acq, NEW_CODE) == patched.patch)

    # ---------- full flow ----------
    fake.reset()
    out = create_draft_pr(client, REPO, "main", JOB, patched, "Pool exhausted.", 2)
    check("flow: returns the PR url and number",
          out == {"url": f"https://github.com/{REPO}/pull/7", "number": 7, "existing": False}, str(out))
    check("flow: exact request sequence (6 calls, no extras)", seq() == NORMAL_SEQ, str(seq()))
    check("flow: branch created with the patched file, main untouched",
          fake.branches[BRANCH]["files"][FILE] == patched.new_text.encode()
          and fake.branches["main"]["files"][FILE] == ORIGINAL_BYTES)
    pr = fake.pulls[0]
    check("flow: PR is a draft from the job branch into main",
          pr["draft"] is True and pr["head"]["ref"] == BRANCH and pr["base"]["ref"] == "main")
    check("flow: PR title names the fixed function", pr["title"] == "AutoDev-Ops: fix ConnectionPool.acquire")
    check("flow: PR body has summary, patch and the honest verification note",
          all(s in pr["body"] for s in ("Root cause hypothesis", "Pool exhausted.", "````diff",
                                         "@@ -16,7 +16,7 @@", JOB, "tests were not run")))
    check("flow: commit message carries the job id", len(fake.commits) == 1 and JOB in fake.commits[0])
    check("flow: every request sent the bearer token", all(r[4] == f"Bearer {TOKEN}" for r in fake.requests))
    check("flow: token never appears in any request body",
          TOKEN not in json.dumps([r[3] for r in fake.requests]))

    # ---------- idempotency ----------
    n = len(fake.requests)
    again = create_draft_pr(client, REPO, "main", JOB, patched, "Pool exhausted.", 2)
    check("retry: returns the existing PR", again["existing"] is True and again["number"] == 7)
    check("retry: makes exactly one read-only request", seq()[n:] == [("GET", "/pulls")])

    # ---------- partial-failure recovery ----------
    fake.reset()
    fake.branches[BRANCH] = {"commit": fake.base_sha, "files": {FILE: ORIGINAL_BYTES}}
    out = create_draft_pr(client, REPO, "main", JOB, patched, "h", 1)
    check("resume: existing empty branch is reused and committed",
          out["existing"] is False and fake.branches[BRANCH]["files"][FILE] == patched.new_text.encode()
          and seq() == [("GET", "/pulls"), ("GET", "/git/ref/heads/main"), ("GET", f"/contents/{FILE}"),
                        ("POST", "/git/refs"), ("GET", f"/contents/{FILE}"), ("PUT", f"/contents/{FILE}"),
                        ("POST", "/pulls")], str(seq()))
    check("resume: second contents lookup targeted the job branch", fake.requests[4][2]["ref"] == [BRANCH])

    fake.reset()
    fake.branches[BRANCH] = {"commit": fake.base_sha, "files": {FILE: patched.new_text.encode()}}
    out = create_draft_pr(client, REPO, "main", JOB, patched, "h", 1)
    check("resume: already-committed branch skips the commit",
          out["number"] == 7 and ("PUT", f"/contents/{FILE}") not in seq() and not fake.commits)

    fake.reset()
    fake.branches[BRANCH] = {"commit": fake.base_sha, "files": {FILE: ORIGINAL_BYTES + b"# other\n"}}
    try:
        create_draft_pr(client, REPO, "main", JOB, patched, "h", 1)
        check("resume: branch with unexpected content is refused", False)
    except PRError as e:
        check("resume: branch with unexpected content is refused", "unexpected content" in str(e), str(e))

    # ---------- drift refusal ----------
    fake.reset(main_content=ORIGINAL_BYTES + b"# remote change\n")
    try:
        create_draft_pr(client, REPO, "main", JOB, patched, "h", 1)
        check("drift: differing remote file is refused", False)
    except PRError as e:
        check("drift: differing remote file is refused", "differs from the local checkout" in str(e), str(e))
    check("drift: refusal had no side effects (no branch, no writes)",
          BRANCH not in fake.branches and not any(m in ("POST", "PUT") for m, _ in seq()))

    # ---------- line endings ----------
    crepo = lf_copy_of_sample(tmp / "crlf" / "sample_repo")
    cdb = crepo / "shop" / "db.py"
    cdb.write_bytes(cdb.read_bytes().replace(b"\n", b"\r\n"))
    cpatched = build_patched_file(crepo, acq, NEW_CODE)
    fake.reset()
    create_draft_pr(client, REPO, "main", JOB, cpatched, "h", 1)
    pushed = fake.branches[BRANCH]["files"][FILE]
    check("line endings: CRLF local + LF remote commits LF",
          b"\r" not in pushed and pushed == patched.new_text.encode(), "")
    fake.reset(main_content=ORIGINAL_BYTES.replace(b"\n", b"\r\n"))
    create_draft_pr(client, REPO, "main", JOB, patched, "h", 1)
    pushed = fake.branches[BRANCH]["files"][FILE]
    check("line endings: LF local + CRLF remote commits CRLF throughout",
          pushed.count(b"\n") == pushed.count(b"\r\n") and b"timeout=5.0" in pushed)

    # ---------- API errors ----------
    fake.reset()
    try:
        create_draft_pr(GitHubClient("wrong-token-xyz", API), REPO, "main", JOB, patched, "h", 1)
        check("errors: bad token raises GitHubError 401", False)
    except GitHubError as e:
        check("errors: bad token raises GitHubError 401",
              e.status == 401 and "Bad credentials" in str(e) and "wrong-token-xyz" not in str(e), str(e))
    try:
        create_draft_pr(client, REPO, "no-such-branch", JOB, patched, "h", 1)
        check("errors: missing base branch raises GitHubError 404", False)
    except GitHubError as e:
        check("errors: missing base branch raises GitHubError 404", e.status == 404, str(e))

    # ---------- maybe_create_pr gating ----------
    state = {"evaluation_status": "passed", "hypothesis": "Pool exhausted.", "attempts": 2,
             "proposed_fix": {"chunk_id": acq["chunk_id"], "new_code": NEW_CODE},
             "retrieved_chunks": [acq]}
    repos_dir = tmp / "repos"
    lf_copy_of_sample(repos_dir / "sample_repo")
    env_ok = {"GITHUB_TOKEN": TOKEN, "AUTODEV_GITHUB_REPOS": f"sample_repo={REPO}", "AUTODEV_GITHUB_API": API}
    nothing = {"pr_url": None, "pr_number": None, "pr_error": None}

    fake.reset()
    out = maybe_create_pr(state, "sample_repo", JOB, environ=env_ok, repos_dir=repos_dir)
    check("service: configured repo opens a PR (default base branch is main)",
          out == {"pr_url": f"https://github.com/{REPO}/pull/7", "pr_number": 7, "pr_error": None}, str(out))
    n = len(fake.requests)
    check("service: no token -> nothing happens",
          maybe_create_pr(state, "sample_repo", JOB, environ={**env_ok, "GITHUB_TOKEN": ""}, repos_dir=repos_dir) == nothing
          and len(fake.requests) == n)
    check("service: unmapped repo -> nothing happens",
          maybe_create_pr(state, "other_repo", JOB, environ=env_ok, repos_dir=repos_dir) == nothing and len(fake.requests) == n)
    check("service: failed evaluation -> nothing happens",
          maybe_create_pr({**state, "evaluation_status": "failed"}, "sample_repo", JOB,
                          environ=env_ok, repos_dir=repos_dir) == nothing and len(fake.requests) == n)
    out = maybe_create_pr(state, "sample_repo", JOB,
                          environ={"GITHUB_TOKEN": TOKEN, "AUTODEV_GITHUB_REPOS": "sample_repo=nope"})
    check("service: malformed mapping is reported, not raised", "AUTODEV_GITHUB_REPOS" in (out["pr_error"] or ""))
    fake.reset()
    out = maybe_create_pr(state, "sample_repo", "d" * 32, environ={**env_ok, "GITHUB_TOKEN": "wrong-token-xyz"},
                          repos_dir=repos_dir)
    check("service: bad token is reported without leaking it",
          "401" in (out["pr_error"] or "") and "wrong-token-xyz" not in (out["pr_error"] or ""))
    out = maybe_create_pr(state, "sample_repo", "e" * 32,
                          environ={**env_ok, "AUTODEV_GITHUB_API": f"http://127.0.0.1:{free_port()}"},
                          repos_dir=repos_dir)
    check("service: unreachable API is reported as a network error",
          (out["pr_error"] or "").startswith("network error"), str(out))
    out = maybe_create_pr(state, "sample_repo", "f" * 32,
                          environ={**env_ok, "AUTODEV_GITHUB_BASE_BRANCH": "develop"}, repos_dir=repos_dir)
    check("service: configurable base branch (missing one gives a 404 error)", "404" in (out["pr_error"] or ""))
    out = maybe_create_pr(state, "sample_repo", "a" * 32, environ=env_ok, repos_dir=tmp / "nowhere")
    check("service: missing local checkout is reported", "not found" in (out["pr_error"] or ""))

    # ---------- parse_repo_map / body truncation / renderer ----------
    check("parse_repo_map: multiple entries", parse_repo_map("a=x/y, b=u/v-w ") == {"a": "x/y", "b": "u/v-w"})
    check("parse_repo_map: empty string", parse_repo_map("") == {})
    try:
        parse_repo_map("a=nope")
        check("parse_repo_map: invalid entry raises", False)
    except ValueError:
        check("parse_repo_map: invalid entry raises", True)
    body = build_pr_body(JOB, dataclasses.replace(patched, patch="x" * 100_000), "h", 1)
    check("PR body truncates huge patches", len(body) < 30_000 and "patch truncated" in body)

    buf = io.StringIO()
    EventRenderer(Console(file=buf, width=160, force_terminal=False, color_system=None)).render(
        "job_completed", {"result": {"evaluation_status": "passed", "attempts": 1, "patch_path": "p.patch",
                                     "patch_error": None, "pr_url": "https://github.com/x/y/pull/7", "pr_error": None}})
    check("renderer: shows the draft PR url", "Draft PR: https://github.com/x/y/pull/7" in buf.getvalue())
    buf = io.StringIO()
    EventRenderer(Console(file=buf, width=160, force_terminal=False, color_system=None)).render(
        "job_completed", {"result": {"evaluation_status": "passed", "attempts": 1, "patch_path": "p.patch",
                                     "patch_error": None, "pr_url": None, "pr_error": "GitHub API error 401: Bad credentials"}})
    check("renderer: shows the PR error", "Draft PR could not be created: GitHub API error 401" in buf.getvalue())

    # ---------- full pipeline through the real handler ----------
    class ScriptedLLM:
        def __init__(self):
            self.calls = 0

        def analyze(self, log):
            return {"hypothesis": "Connection pool exhausted.", "search_query": "ERR_DB_TIMEOUT"}

        def draft_fix(self, log, hypothesis, chunks_, feedback):
            code = [BAD_SYNTAX, NEW_CODE][min(self.calls, 1)]
            self.calls += 1
            target = next(c for c in chunks_ if c["qualified_name"] == "ConnectionPool.acquire")
            return {"hypothesis": hypothesis, "chunk_id": target["chunk_id"], "new_code": code}

    ENV_KEYS = ("GITHUB_TOKEN", "AUTODEV_GITHUB_REPOS", "AUTODEV_GITHUB_API", "AUTODEV_GITHUB_BASE_BRANCH")

    async def pipeline() -> None:
        print("Loading models and indexing sample_repo...")
        embedder, reranker = Embedder(), Reranker()
        conn = get_connection()
        try:
            init_schema(conn)
            init_vector_index(conn)
            replace_repo_chunks(conn, "sample_repo", ingest_repo(ROOT / "sample_repo").chunks)
            embed_missing_chunks(conn, "sample_repo", embedder)
        finally:
            conn.close()

        e2e_repos = tmp / "e2e_repos"
        lf_copy_of_sample(e2e_repos / "sample_repo")
        expected_new = build_patched_file(e2e_repos / "sample_repo", acq, NEW_CODE).new_text.encode()
        saved_cfg = (config.REPOS_DIR, config.PATCH_DIR)
        saved_env = {k: os.environ.get(k) for k in ENV_KEYS}
        config.REPOS_DIR, config.PATCH_DIR = e2e_repos, tmp / "e2e_patches"
        r = aioredis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
        stream = f"autodev:test:{uuid.uuid4().hex[:8]}:unused"
        jids: list[str] = []

        async def run_job() -> tuple[str, dict]:
            jid = await enqueue_job(r, "sample_repo", LOG, stream_key=stream)
            jids.append(jid)
            job = await r.hgetall(job_key(jid))
            await make_triage_handler(r, ScriptedLLM(), embedder, reranker)(jid, job)
            return jid, json.loads(await r.hget(job_key(jid), "result"))

        try:
            os.environ.update({"GITHUB_TOKEN": TOKEN, "AUTODEV_GITHUB_REPOS": f"sample_repo={REPO}",
                               "AUTODEV_GITHUB_API": API, "AUTODEV_GITHUB_BASE_BRANCH": "main"})
            fake.reset()
            jid, result = await run_job()
            check("pipeline: result carries a draft PR url and no PR error",
                  result["pr_url"] == f"https://github.com/{REPO}/pull/7" and result["pr_error"] is None, str(result.get("pr_error")))
            check("pipeline: patch file is still produced", bool(result["patch_path"]) and Path(result["patch_path"]).is_file())
            check("pipeline: branch holds exactly the patched file",
                  fake.branches[f"autodev/{jid}"]["files"][FILE] == expected_new)
            check("pipeline: PR is a draft whose body has the agent's hypothesis",
                  fake.pulls[0]["draft"] is True and "Connection pool exhausted." in fake.pulls[0]["body"])

            os.environ["GITHUB_TOKEN"] = "wrong-token-xyz"
            _, result2 = await run_job()  # must not raise
            check("pipeline: a GitHub failure does not fail the job",
                  result2["evaluation_status"] == "passed" and bool(result2["patch"])
                  and result2["pr_url"] is None and "401" in (result2["pr_error"] or ""))
        finally:
            config.REPOS_DIR, config.PATCH_DIR = saved_cfg
            for k, v in saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            await r.delete(stream, *[job_key(j) for j in jids], *[events_key(j) for j in jids])
            await r.aclose()

    asyncio.run(pipeline())

server.shutdown()
print("\nMODULE 7B: ALL CHECKS PASSED" if not failures else f"\nMODULE 7B: FAILED -> {failures}")
sys.exit(1 if failures else 0)