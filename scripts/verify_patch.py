"""Module 7A verification: patch generation, validated with real `git apply`."""
import ast
import asyncio
import json
import shutil
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import redis.asyncio as aioredis  # noqa: E402
from rich.console import Console  # noqa: E402
import io  # noqa: E402

import app.config as config  # noqa: E402
from app.cli.render import EventRenderer  # noqa: E402
from app.db.chunk_store import init_schema, init_vector_index, replace_repo_chunks  # noqa: E402
from app.db.connection import get_connection  # noqa: E402
from app.ingestion.ast_chunker import ingest_repo  # noqa: E402
from app.patch.builder import PatchError, build_patch, write_patch  # noqa: E402
from app.patch.service import make_patch_for_state  # noqa: E402
from app.queue.events import events_key  # noqa: E402
from app.queue.jobs import enqueue_job, job_key  # noqa: E402
from app.queue.triage_handler import make_triage_handler  # noqa: E402
from app.search.dense_search import embed_missing_chunks  # noqa: E402
from app.search.embedder import Embedder  # noqa: E402
from app.search.reranker import Reranker  # noqa: E402

failures: list[str] = []
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
JOB = "a" * 32


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def git(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


def init_repo(path: Path) -> None:
    git(path, "init", "-q")
    git(path, "config", "core.autocrlf", "false")
    git(path, "config", "user.email", "test@example.com")
    git(path, "config", "user.name", "test")
    git(path, "add", "-A")
    git(path, "commit", "-q", "-m", "init")


def lf_copy_of_sample(dest: Path) -> Path:
    """Copy sample_repo to dest with LF line endings so the baseline is predictable."""
    shutil.copytree(ROOT / "sample_repo", dest, ignore=shutil.ignore_patterns("__pycache__"))
    for p in dest.rglob("*.py"):
        p.write_bytes(p.read_bytes().replace(b"\r\n", b"\n"))
    return dest


def apply_patch(repo: Path, patch: str, name: str) -> tuple[bool, bool]:
    """Return (check_ok, apply_ok) using a patch file stored next to the repo."""
    pf = repo.parent / f"{name}.patch"
    pf.write_bytes(patch.encode("utf-8"))
    chk = git(repo, "apply", "--check", str(pf)).returncode == 0
    ok = chk and git(repo, "apply", str(pf)).returncode == 0
    return chk, ok


if shutil.which("git") is None:
    print("git is not on PATH; install git to run this verification.")
    sys.exit(2)

chunks = {c.qualified_name: c.to_dict() for c in ingest_repo(ROOT / "sample_repo").chunks}
acq = chunks["ConnectionPool.acquire"]

with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_s:
    tmp = Path(tmp_s)

    # =============== Part 1a: LF repo ===============
    repo = lf_copy_of_sample(tmp / "lf" / "sample_repo")
    init_repo(repo)
    original = (repo / "shop" / "db.py").read_bytes()
    patch = build_patch(repo, acq, NEW_CODE)
    check("patch header is a git diff for shop/db.py", patch.startswith("diff --git a/shop/db.py b/shop/db.py\n"))
    check("patch has ---/+++ headers", "--- a/shop/db.py\n+++ b/shop/db.py\n" in patch)
    check("patch has the real hunk header with 3 lines of context", "@@ -16,7 +16,7 @@" in patch, patch)
    check("patch shows the one-line change",
          "-    def acquire(self, timeout=1.0):\n+    def acquire(self, timeout=5.0):\n" in patch)
    check("LF patch contains no carriage returns", "\r" not in patch)
    chk, ok = apply_patch(repo, patch, "lf")
    check("git apply --check accepts the patch", chk)
    check("git apply applies the patch", ok)
    patched = (repo / "shop" / "db.py").read_text(encoding="utf-8")
    check("patched file parses and has the new signature",
          "    def acquire(self, timeout=5.0):" in patched and ast.parse(patched) is not None)
    check("git diff --numstat reports exactly 1 added / 1 removed line",
          git(repo, "diff", "--numstat").stdout.strip() == "1\t1\tshop/db.py", git(repo, "diff", "--numstat").stdout)
    check("applying twice is rejected (patch is exact)",
          git(repo, "apply", "--check", str(repo.parent / "lf.patch")).returncode != 0)
    rev = git(repo, "apply", "-R", str(repo.parent / "lf.patch"))
    check("reverse apply restores the original bytes",
          rev.returncode == 0 and (repo / "shop" / "db.py").read_bytes() == original)

    # =============== Part 1b: CRLF repo ===============
    crepo = lf_copy_of_sample(tmp / "crlf" / "sample_repo")
    db = crepo / "shop" / "db.py"
    db.write_bytes(db.read_bytes().replace(b"\n", b"\r\n"))
    init_repo(crepo)
    cpatch = build_patch(crepo, acq, NEW_CODE)
    check("CRLF file gets CRLF content lines in the patch",
          "-    def acquire(self, timeout=1.0):\r\n+    def acquire(self, timeout=5.0):\r\n" in cpatch)
    chk, ok = apply_patch(crepo, cpatch, "crlf")
    raw = db.read_bytes()
    check("CRLF patch applies with git", chk and ok)
    check("CRLF file keeps CRLF on every line after applying",
          raw.count(b"\n") == raw.count(b"\r\n") and b"timeout=5.0" in raw)

    # =============== Part 1c: no trailing newline ===============
    nonl = tmp / "nonl"
    nonl.mkdir()
    (nonl / "x.py").write_bytes(b"def f():\n    return 1")
    init_repo(nonl)
    fchunk = ingest_repo(nonl).chunks[0].to_dict()
    npatch = build_patch(nonl, fchunk, "def f():\n    return 2")
    check("no-newline patch carries git's marker on both sides",
          npatch.count("\\ No newline at end of file") == 2, npatch)
    chk, ok = apply_patch(nonl, npatch, "nonl")
    check("no-newline patch applies and keeps the missing final newline",
          chk and ok and (nonl / "x.py").read_bytes() == b"def f():\n    return 2")

    # =============== Part 1d: refusals ===============
    stale = lf_copy_of_sample(tmp / "stale" / "sample_repo")
    sdb = stale / "shop" / "db.py"
    sdb.write_text(sdb.read_text(encoding="utf-8").replace("time.sleep(0.01)", "time.sleep(0.02)"), encoding="utf-8")
    try:
        build_patch(stale, acq, NEW_CODE)
        check("stale index is refused", False)
    except PatchError as e:
        check("stale index is refused", "changed since it was indexed" in str(e), str(e))

    bom = tmp / "bom"
    bom.mkdir()
    (bom / "x.py").write_bytes(b"\xef\xbb\xbfdef f():\n    return 1\n")
    bchunk = ingest_repo(bom).chunks[0].to_dict()
    try:
        build_patch(bom, bchunk, "def f():\n    return 2")
        check("BOM file is refused", False)
    except PatchError as e:
        check("BOM file is refused", "BOM" in str(e), str(e))

    for label, mutate, needle in [
        ("path traversal is refused", {**acq, "file_path": "../outside.py"}, "escapes"),
        ("missing file is refused", {**acq, "file_path": "shop/nope.py"}, "not found"),
    ]:
        try:
            build_patch(repo, mutate, NEW_CODE)
            check(label, False)
        except PatchError as e:
            check(label, needle in str(e), str(e))
    try:
        build_patch(repo, acq, acq["content"])
        check("no-op fix is refused", False)
    except PatchError as e:
        check("no-op fix is refused", "no change" in str(e), str(e))

    wp = write_patch(tmp / "out", JOB, patch)
    check("write_patch writes <job_id>.patch as exact bytes (no newline translation)",
          wp.name == f"{JOB}.patch" and wp.read_bytes() == patch.encode("utf-8") and b"\r\n" not in wp.read_bytes())

    # =============== Part 2: service layer ===============
    repos_dir = tmp / "svc_repos"
    lf_copy_of_sample(repos_dir / "sample_repo")
    state = {"evaluation_status": "passed",
             "proposed_fix": {"chunk_id": acq["chunk_id"], "new_code": NEW_CODE},
             "retrieved_chunks": [chunks["run_query"], acq]}
    res = make_patch_for_state(state, "sample_repo", JOB, repos_dir=repos_dir, patch_dir=tmp / "svc_patches")
    check("service: patch built and written",
          res["patch_error"] is None and Path(res["patch_path"]).name == f"{JOB}.patch"
          and Path(res["patch_path"]).read_text(encoding="utf-8") == res["patch"], str(res.get("patch_error")))
    none = make_patch_for_state({"evaluation_status": "failed"}, "sample_repo", JOB, repos_dir, tmp / "x")
    check("service: failed evaluation yields no patch and no error",
          none == {"patch": None, "patch_path": None, "patch_error": None})
    res = make_patch_for_state(state, "other_repo", JOB, repos_dir=repos_dir, patch_dir=tmp / "x")
    check("service: missing repo directory is reported", "not found" in (res["patch_error"] or ""))
    res = make_patch_for_state(state, "../x", JOB, repos_dir=repos_dir, patch_dir=tmp / "x")
    check("service: unsafe repo name is rejected", "invalid repo name" in (res["patch_error"] or ""))

    # =============== Part 2b: CLI shows patch info ===============
    buf = io.StringIO()
    rnd = EventRenderer(Console(file=buf, width=160, force_terminal=False, color_system=None))
    rnd.render("job_completed", {"result": {"evaluation_status": "passed", "attempts": 1,
                                            "patch_path": "patches/abc.patch", "patch_error": None}})
    check("renderer: shows patch file and git apply command",
          "Patch file: patches/abc.patch" in buf.getvalue() and "git apply patches/abc.patch" in buf.getvalue())
    buf = io.StringIO()
    EventRenderer(Console(file=buf, width=160, force_terminal=False, color_system=None)).render(
        "job_completed", {"result": {"evaluation_status": "passed", "attempts": 1,
                                     "patch_path": None, "patch_error": "file not found: x"}})
    check("renderer: shows patch error", "Patch could not be generated: file not found: x" in buf.getvalue())

    # =============== Part 3: full pipeline through the real handler ===============
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
        saved = (config.REPOS_DIR, config.PATCH_DIR)
        config.REPOS_DIR, config.PATCH_DIR = e2e_repos, tmp / "e2e_patches"
        r = aioredis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)
        stream = f"autodev:test:{uuid.uuid4().hex[:8]}:unused"
        jid = None
        try:
            jid = await enqueue_job(r, "sample_repo", LOG, stream_key=stream)
            job = await r.hgetall(job_key(jid))
            await make_triage_handler(r, ScriptedLLM(), embedder, reranker)(jid, job)
            result = json.loads(await r.hget(job_key(jid), "result"))

            check("pipeline: agent verified a fix",
                  result["evaluation_status"] == "passed" and result["attempts"] == 2)
            check("pipeline: result has patch_path, patch text and no error",
                  bool(result["patch"]) and result["patch_error"] is None
                  and Path(result["patch_path"]).name == f"{jid}.patch" and Path(result["patch_path"]).is_file())
            check("pipeline: stored patch has the real hunk header", "@@ -16,7 +16,7 @@" in result["patch"])
            check("pipeline: chunk preview diff is still in the result", "@@ -19,4 +19,4 @@" in result["diff_preview"])

            fresh = lf_copy_of_sample(tmp / "fresh" / "sample_repo")
            init_repo(fresh)
            apply = git(fresh, "apply", result["patch_path"])
            fixed = (fresh / "shop" / "db.py").read_text(encoding="utf-8")
            check("pipeline: the worker-written .patch applies to a fresh repo with git apply",
                  apply.returncode == 0 and "    def acquire(self, timeout=5.0):" in fixed, apply.stderr)
        finally:
            config.REPOS_DIR, config.PATCH_DIR = saved
            if jid:
                await r.delete(stream, job_key(jid), events_key(jid))
            await r.aclose()

    asyncio.run(pipeline())

print("\nMODULE 7A: ALL CHECKS PASSED" if not failures else f"\nMODULE 7A: FAILED -> {failures}")
sys.exit(1 if failures else 0)