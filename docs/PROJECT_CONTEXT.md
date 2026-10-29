# AutoDev-Ops Agent: Project Context (handoff document)

Status as of 2026-10-29: Version 1.0 complete. All 8 roadmap modules (plus Part 8C, API-key auth) are built,
verified and pushed. Purpose of this file: let a fresh chat (or a new contributor) continue the project
without the original conversation. Read this first, then README.md, then only the source files a task touches.

## 1. What the system does

An autonomous incident triage and root-cause-analysis engine. A user submits a production log or stack
trace for an indexed repository. The system finds the relevant functions (hybrid search), asks an LLM for a
root-cause hypothesis and a replacement for one function, verifies the replacement (AST + compile), builds a
real `git apply`-able patch, optionally opens a **draft** GitHub PR, and streams every step live (SSE) to a
Rich terminal CLI.

Stack: Python 3.11, FastAPI (async), Redis (Streams), PostgreSQL 16 + pgvector, rank-bm25,
sentence-transformers (BAAI/bge-base-en-v1.5, cross-encoder/ms-marco-MiniLM-L-6-v2), LangGraph, Groq SDK
(model supplied via env, developed with `openai/gpt-oss-120b`), sse-starlette, Rich, Docker Compose.

## 2. Working agreement (how this project is built)

1. One small module/part at a time; never generate the whole system at once.
2. No new dependency, framework or feature without asking the owner first.
3. Every part ships with exact file paths, minimal code and exact terminal commands, plus a deterministic
   verification script (`scripts/verify_*.py`).
4. Checkpoint after every part: the owner runs the test and pastes the output; only then move on.
5. Nothing speculative: if behavior is uncertain, say so and test it. Never loosen an assertion to make a test pass.
6. Owner develops on Windows (PowerShell); every command should have a PowerShell form.

## 3. Architecture

```
CLI / curl --POST submit--> FastAPI --(job hash + XADD, one MULTI/EXEC)--> Redis
CLI <--SSE stream-- FastAPI <--XREAD-- Redis event stream (per job)
Redis job stream --XREADGROUP--> asyncio Worker --asyncio.to_thread--> LangGraph agent
agent -> HybridRetriever (BM25 + pgvector dense + RRF + cross-encoder) -> Postgres
agent -> Groq LLM (forced tool calling)      worker -> patch builder -> patches/<job_id>.patch
worker -> (optional) GitHub draft PR         worker -> Redis (step events, result, final status)
index_repo CLI -> AST chunker -> Postgres (+ embeddings)
```

Life of a job: submit -> `202 {job_id}` -> worker claims it (`job_started` event) -> agent steps stream as
`step` events -> result stored in the job hash -> `status=completed` -> `job_completed` event (carries the result)
-> `XACK` + `XDEL` -> SSE stream closes.

Agent graph (explicit control flow, no free tool selection by the model):
`START -> analyze_log -> retrieve_code -> draft_fix -> verify -> (passed) output_diff -> END`;
`verify` failed and attempts < 3 -> back to `draft_fix` with the verifier message; failed at 3 attempts -> END.

## 4. Repository layout

```
Dockerfile  .dockerignore  docker-compose.yml  .env.example  .gitignore  requirements.txt  README.md
docs/PROJECT_CONTEXT.md
app/
  config.py                 Redis settings, key names, CONSUMER_GROUP, REPOS_DIR, PATCH_DIR, JOB_TTL_S
  ingestion/  models.py (CodeChunk, SkippedFile, IngestResult)   ast_chunker.py (chunk_source, ingest_repo)
  db/         connection.py (get_connection)   chunk_store.py (schema, upsert, fetch, vector search, count)
  search/     tokenizer.py  bm25_index.py  embedder.py  dense_search.py  reranker.py  hybrid.py
  agent/      state.py  tools.py  graph.py  prompts.py  llm_groq.py  runner.py
  queue/      jobs.py  worker.py  events.py  errors.py  triage_handler.py  run_worker.py  handlers.py (unused placeholder)
  patch/      builder.py  service.py
  github/     client.py  pr.py  service.py
  api/        main.py  schemas.py  stream.py  auth.py
  cli/        sse_client.py  render.py  triage_cli.py  index_repo.py
sample_repo/shop/   __init__.py  db.py  orders.py  broken_syntax.py (deliberately invalid)
scripts/            init-db.sql  sample_payload.json  sample_error.log  show_job.py  verify_*.py
patches/ (gitignored, generated)   repos/ (gitignored, repo checkouts for Docker)
```

Note: `requirements.txt` still lists `tree-sitter` (from the original roadmap) but nothing imports it. The
chunker uses native `ast`.

## 5. Module map (what each part does and the key facts)

### Module 1: infrastructure
`docker-compose.yml` (Postgres 16 + pgvector, Redis 7), `scripts/init-db.sql` (`CREATE EXTENSION vector`),
`requirements.txt`, `scripts/verify_infra.py`. Dev lessons: a Windows Postgres service on 5432 shadowed the
container (host port is 5433); `POSTGRES_*` env vars apply only to an empty data dir, so stale volumes keep old
credentials.

### Module 2: AST ingestion (`app/ingestion`)
- Native Python `ast` (tree-sitter was not used; no grammar package approved).
- `CodeChunk`: `chunk_id` (`<file_path>::<qualified_name>@<start_line>`), `file_path` (POSIX, repo-relative),
  `chunk_type` (`function` | `method` | `class`), `name`, `qualified_name` (`func` or `Class.method`),
  `class_name`, `start_line`/`end_line` (1-based, inclusive, decorators included), `content`.
- Function/async function at top level -> `function`; method directly inside a class -> `method`;
  top-level class -> `class` chunk whose content is the class text **minus method bodies** (header, docstring,
  attributes; blank runs collapsed), but whose line range is the whole class. Nested functions stay in their parent.
- Not chunked: module-level statements (constants, scripts) and functions under module-level `if`/`try`.
- Skips (reports, never crashes): SyntaxError, UnicodeDecodeError, ValueError. Reads with `utf-8-sig`.
  Excluded dirs: .git, __pycache__, .venv, venv, node_modules, .tox, .mypy_cache, .pytest_cache, build, dist.
  Deterministic order (sorted).

### Module 3: hybrid search (`app/db`, `app/search`)
- Table `code_chunks`: PK `(repo_name, chunk_id)`; columns repo_name, chunk_id, file_path, chunk_type, name,
  qualified_name, class_name, start_line, end_line, content, `embedding vector(768)`, created_at.
  HNSW index `idx_code_chunks_embedding_hnsw` (cosine ops). `replace_repo_chunks` deletes the repo's rows then
  inserts (so embeddings are lost and `embed_missing_chunks` re-embeds).
- Tokenizer: lowercased whole identifier plus snake_case/camelCase parts (`ERR_DB_TIMEOUT` -> `err_db_timeout`,
  `err`, `db`, `timeout`). No stemming, no stop words.
- BM25: `BM25Index` (rank-bm25 Okapi), in memory, rebuilt from DB rows per `HybridRetriever`; zero-score hits dropped.
- Dense: bge-base-en-v1.5, normalized vectors, query prefix `"Represent this sentence for searching relevant
  passages: "` (queries only). Embedding text = `file_path\nchunk_type qualified_name\ncontent`. Vectors go to
  Postgres as text literals cast with `::vector`. Cosine similarity = `1 - (embedding <=> q)`.
- `HybridRetriever(conn, repo, embedder, reranker, candidate_k=20, rerank_k=10)`: BM25 top-20 + dense top-20 ->
  RRF (k=60) -> top-10 shortlist -> cross-encoder rerank -> `search(query, top_k=3)` returns `SearchHit`
  (chunk, rrf_score, bm25_rank, dense_rank, rerank_score). If BM25 finds nothing, dense still answers.

### Module 4: queue (`app/queue`, `app/api`)
- `POST /api/v1/triage/submit` body `{repo_name (1-200 chars), log (1-200000 chars)}` -> `202 {job_id, status:"queued"}`.
  Job id = uuid4 hex (32 chars). Writes hash `autodev:job:<id>` (status, repo_name, log, created_at) and
  `XADD autodev:triage:jobs {job_id}` in one transaction. `503` if Redis is down.
- Worker (`Worker`, custom asyncio on Redis Streams consumer groups, group `triage-workers`): defaults
  block 1 s, claim idle 300 s, reclaim interval 30 s, job timeout 240 s (must be < claim idle), max deliveries 3.
  At-least-once; handlers must be idempotent. Failure of an attempt -> status `retrying` and the message stays
  pending until reclaimed (`XAUTOCLAIM`); at the max or on `PermanentJobError` -> status `failed`.
  A stream message with no job hash is dropped. After a terminal state: hash gets `EXPIRE JOB_TTL_S` (7 days),
  and `_ack` does `XACK` then `XDEL` (stream holds only unprocessed jobs).
- Handler contract: `async handler(job_id, job_dict)`; `triage_handler.make_triage_handler` is the real one.

### Module 5: agent (`app/agent`)
- `AgentState` (TypedDict): log, search_query, retrieved_chunks, hypothesis, proposed_fix
  (`{chunk_id, new_code}`), verification_feedback, attempts, evaluation_status (`pending|passed|failed`), diff_preview.
- Tools: `retrieve_code_context(retriever, query, top_k=3)`; `verify_syntax_and_lint(code, expected_name)`
  (dedent, `ast.parse`, exactly one top-level function whose name equals the target, `compile()`; **no external
  linter, tests are not run**); `check_fix` (also requires a retrieved function/method chunk, and rejects no-op
  fixes); `build_diff_preview` (chunk-relative diff with hunk numbers shifted to real lines; the real patch is
  built in Module 7). Only `function` and `method` chunks (`FIXABLE_TYPES`) can be replaced.
- The LLM returns **replacement code for one chunk**, never a diff (models write bad diff line numbers).
- `GroqLLM` (`llm_groq.py`): official Groq SDK via an OpenAI-compatible call; forced tool calls
  `submit_analysis` and `submit_fix`; `chunk_id` is a JSON-schema enum of the fixable retrieved chunks;
  temperature 0, max_tokens 3000; log truncated to its last 12,000 chars, chunks capped at 6,000;
  markdown fences stripped from `new_code`; malformed analysis -> `LLMOutputError`; malformed fix -> empty fix
  (verifier rejects it and the loop retries). `make_groq_llm()` returns a **GroqLLM adapter** (not a raw client)
  and needs `GROQ_API_KEY` and `GROQ_MODEL`; client timeout 60 s, 2 retries.
- `run_triage(log, repo, llm, embedder, reranker, on_step=None)` is synchronous (run it via `asyncio.to_thread`);
  raises `RepoNotIndexedError` if the repo has no chunks. `build_result(state)` makes the stored result.

### Module 6: streaming and CLI (`app/queue/events.py`, `app/api/stream.py`, `app/cli`)
- Per-job Redis Stream `autodev:job:<id>:events`, entries `{type, data(JSON)}`, expiry 24 h refreshed on each publish.
  Event types: `job_started{attempt}`, `step{node...}`, `job_retrying{attempt,error}`,
  `job_completed{result}`, `job_failed{error}`; the endpoint adds `timeout` and `stream_error`.
  Step payloads: analyze_log{hypothesis,search_query}; retrieve_code{chunks[qualified_name,chunk_type,file_path,
  start_line,end_line]}; draft_fix{attempt,hypothesis,chunk_id,new_code}; verify{evaluation_status,feedback};
  output_diff{diff}.
- `GET /api/v1/triage/stream/{job_id}` (sse-starlette `EventSourceResponse`): replays history from the start,
  resumes after the `Last-Event-ID` header (SSE `id` = Redis stream id), closes after a terminal event, ends with
  `timeout` after 900 s idle (`IDLE_TIMEOUT_S`, `BLOCK_MS`=5000 are module constants). `404` unknown or malformed
  job id (32 hex chars), `400` bad `Last-Event-ID`.
- CLI (`python -m app.cli.triage_cli [--api URL] submit --repo NAME --log-file F | watch JOB_ID`), stdlib
  `urllib` client with auto-reconnect (3 tries, `Last-Event-ID`), Rich renderer built from `Text` objects (log text
  with `[brackets]` is safe). Default API `http://127.0.0.1:8000` (not `localhost`, to avoid the Windows IPv6 delay).
  Exit codes: 0 verified fix, 1 completed with no verified fix, 2 usage error (argparse), 3 job failed, 4 stream/connection/auth problem.

### Module 7: patch and PR
- Patch (`app/patch`): `build_patched_file(repo_root, chunk, new_code) -> PatchedFile` works on the real file
  on disk: path-escape guard, BOM refused, non-UTF-8 refused, **stale-index check** (the file's lines must equal the
  chunk content, CRLF-normalized), keeps CRLF, handles a missing final newline (`\ No newline at end of file`),
  3 lines of context, header `diff --git a/<p> b/<p>`. `write_patch` writes **bytes** to `<PATCH_DIR>/<job_id>.patch`.
  `make_patch_for_state` returns `{patch, patch_path, patch_error}`; patch problems never fail the job.
  Repo checkout convention: `<AUTODEV_REPOS_DIR>/<repo_name>` (default: project root).
- GitHub (`app/github`), stdlib `urllib`, **opt-in**: needs `GITHUB_TOKEN` and `AUTODEV_GITHUB_REPOS`
  (`name=owner/repo[,name=owner/repo]`), base branch `AUTODEV_GITHUB_BASE_BRANCH` (default `main`).
  Flow: look for an existing PR (idempotent) -> read base branch sha -> read remote file and **refuse if it differs
  from the local original** (line endings ignored; no side effects) -> create branch `autodev/<job_id>` (resume if it
  exists) -> commit the patched file via the Contents API -> open a **draft** PR (body has hypothesis, patch and a
  note that tests were not run). Failures are stored as `pr_error`; they never fail the job.
- Stored result keys: `evaluation_status, attempts, hypothesis, search_query, verification_feedback,
  proposed_fix (only if passed), diff_preview, retrieved[{chunk_id, qualified_name, chunk_type, file_path,
  start_line, end_line, rerank_score}], patch, patch_path, patch_error, pr_url, pr_number, pr_error`.
- A job's `status` is `completed` whenever the agent finished; the verdict is `result.evaluation_status`.

### Module 8: production packaging
- One image for API and worker (`python:3.11-slim`, CPU-only PyTorch, non-root uid 1000, `HF_HOME=/models/hf`).
  Compose services: postgres (password auth, published on 127.0.0.1:5433), redis (AOF on, 127.0.0.1:6379),
  api (127.0.0.1:8000, health check on `/openapi.json`), worker (mounts `./repos` read-only at `/repos`, `./patches`
  at `/data/patches`, volume `hf_cache`). Volumes `postgres_data`, `redis_data`, `hf_cache` (renamed from the old
  dev `pgdata`/`redisdata`, which had trust auth baked in).
- `python -m app.cli.index_repo --path <dir> --name <repo>`: ingest + store + embed (run in the worker container
  with `docker compose run --rm worker ...`).
- API auth (Part 8C): `X-API-Key` required on submit and stream, checked with `hmac.compare_digest` against
  `AUTODEV_API_KEY` (min 16 chars, else the API refuses to start); unset = open with a startup warning (dev only);
  Compose requires it. `/docs` and `/openapi.json` stay public. The CLI reads the key from the environment only.

## 6. Configuration reference

| Variable | Default | Used by |
|---|---|---|
| POSTGRES_HOST / PORT / USER / PASSWORD / DB | localhost / 5433 / autodev / autodev_pw (dev fallback) / autodev | db connection (host-side code and worker) |
| REDIS_HOST / REDIS_PORT | localhost / 6379 | API, worker, scripts |
| GROQ_API_KEY, GROQ_MODEL | none (required by the worker) | LLM adapter; the model must support tool calling |
| AUTODEV_API_KEY | none (required by Compose) | API auth and CLI |
| GITHUB_TOKEN, AUTODEV_GITHUB_REPOS, AUTODEV_GITHUB_BASE_BRANCH, AUTODEV_GITHUB_API | empty / empty / main / api.github.com | draft PRs (the API override exists for tests) |
| AUTODEV_REPOS_DIR, AUTODEV_PATCH_DIR | project root, ./patches (`/repos`, `/data/patches` in Docker) | patch step |
| AUTODEV_JOB_TTL_S | 604800 | job hash expiry |
| REPOS_HOST_DIR, PATCHES_HOST_DIR, POSTGRES_HOST_PORT | ./repos, ./patches, 5433 | Compose only |

Redis keys: `autodev:triage:jobs` (stream), group `triage-workers`, `autodev:job:<id>` (hash),
`autodev:job:<id>:events` (stream).

## 7. Design decisions (and why)

| Decision | Why | Status |
|---|---|---|
| Native `ast`, not tree-sitter | Exact for Python, zero new dependency | owner confirmed |
| No module-level chunks | Spec said "strictly by function and class boundaries" | owner confirmed |
| Custom asyncio worker on Redis Streams (no Celery) | Fewer deps, async-native, works on Windows, reuses Redis for events | owner confirmed |
| Groq official SDK directly, no LangChain | Minimal dependencies; adapter sits behind an `LLM` protocol so providers are swappable | owner confirmed |
| No external linter | AST + compile is enough for the spec's "linter / AST check" | owner confirmed |
| `sse-starlette` for SSE | Matches the spec's `EventSourceResponse`; handles pings and disconnects | owner approved |
| Stdlib `urllib` for the CLI client and GitHub | Avoids an unapproved HTTP dependency | design choice |
| LLM returns replacement code, we build the diff | Models are unreliable at diff line numbers | design choice |
| Explicit graph, bounded retries | Deterministic, debuggable | design choice |
| Patch built from the real file with stale-index refusal | A patch that cannot apply is worse than none | design choice |
| PR via Contents API, always draft, opt-in | No push credentials; human review before merge | owner approved |
| Single shared API key | Simple and safe enough behind TLS; not per-user | owner approved |

## 8. Verification scripts (the regression net)

Run from the project root with the venv active. "Infra" = Postgres + Redis up. "Models" = loads the embedding
model and reranker (10-40 s). Host scripts need `POSTGRES_PASSWORD` set to match `.env`.

| Script | Needs | Covers | Final line |
|---|---|---|---|
| verify_infra.py | infra | imports, Postgres + pgvector, Redis | MODULE 1 |
| verify_ingestion.py | none | chunking, metadata, skipped files | MODULE 2 |
| verify_bm25.py | infra | schema, store, tokenizer, BM25 | MODULE 3A |
| verify_dense.py | infra, models | embeddings, HNSW, dense search | MODULE 3B |
| verify_hybrid.py | infra, models | RRF, rerank, fallback | MODULE 3C |
| verify_submit.py | infra + API running | submit endpoint and enqueue | MODULE 4A |
| verify_worker.py | infra | consumer group, ack, retry, timeout, crash recovery | MODULE 4B |
| verify_agent.py | infra, models | tools, graph transitions, retry loop (scripted LLM) | MODULE 5A |
| verify_llm_adapter.py | none | Groq adapter against a fake client | MODULE 5B (offline) |
| verify_llm_live.py | infra, models, Groq env | a real model through the graph (structure only) | MODULE 5B (live) |
| verify_triage_worker.py | infra, models | graph in the worker, off the event loop | MODULE 5C |
| verify_events.py | infra, models | per-job event streams | MODULE 6A |
| verify_sse.py | infra | SSE endpoint, replay, resume, client, renderer | MODULE 6B |
| verify_patch.py | infra, models, git | patches validated with real `git apply` (LF, CRLF, no final newline) | MODULE 7A |
| verify_github_pr.py | infra, models | draft-PR flow against a fake GitHub API | MODULE 7B |
| verify_cleanup.py | infra | XDEL after ack, TTLs | MODULE 8A (cleanup) |
| verify_index_cli.py | infra, models | index_repo CLI | MODULE 8A (index CLI) |
| verify_stack.py | Compose stack up, repo indexed, Groq, key | the live containers, one real job | MODULE 8A (stack) |
| verify_auth.py | infra | API-key auth in both modes | MODULE 8C |

Full regression before a release: every script above that applies. Tests that use a scripted LLM are fully
deterministic; the live ones assert structure, not answer quality. Scripts that run the real handler leave
`patches/*.patch` files (safe to delete).

## 9. Operations runbook

```
cp .env.example .env     # set POSTGRES_PASSWORD, AUTODEV_API_KEY, GROQ_API_KEY, GROQ_MODEL (+ optional GitHub)
mkdir -p repos patches && cp -r sample_repo repos/sample_repo
docker compose up -d --build
docker compose run --rm worker python -m app.cli.index_repo --path /repos/sample_repo --name sample_repo
python -m app.cli.triage_cli submit --repo sample_repo --log-file scripts/sample_error.log
python scripts/show_job.py <job_id>          # inspect a stored job
git apply --directory=sample_repo patches/<job_id>.patch     # from the project root
```
Local dev without containerizing the app: `docker compose up -d postgres redis`, then run
`uvicorn app.api.main:app --port 8000` and `python -m app.queue.run_worker` on the host.

## 10. Gotchas and lessons learned

- Windows: `localhost` may resolve to IPv6 first and add ~2 s; use `127.0.0.1`. PowerShell needs `curl.exe`.
- Postgres env vars and `docker-entrypoint-initdb.d` scripts apply only on first init of an empty volume.
- Write patch files as bytes; Python text mode would rewrite line endings on Windows.
- `asyncio.wait_for` cannot kill a thread started by `to_thread`; a timed-out agent finishes in the background.
- `make_groq_llm()` must return the `GroqLLM` adapter; returning the raw `Groq` client breaks `.analyze`.
- The Groq model must support tool calling; model IDs change, so it is configuration, never hardcoded.
- sse-starlette separates lines with `\r\n`; the client parser accepts both `\r\n` and `\n`.
- Rich markup: build `Text` objects from untrusted strings, never f-string markup.
- Deleting the volumes (`docker compose down -v`) wipes the index; re-run `index_repo`.
- `app/patch/builder.py` imports the private helper `_reindent` from `app/agent/tools.py` (candidate to make public).
- `app/queue/handlers.py` (placeholder handler) is unused; `tree-sitter` in requirements is unused.

## 11. Known limitations

No rate limiting or TLS (use a reverse proxy); one shared API key, no per-user identity; logs and code go to the
LLM provider; "verified" means syntax/compile only (tests are never run); one function per job and Python only;
the index must be fresh or patches are refused; at-least-once delivery; one image carries PyTorch for the API too;
the retriever (BM25 index) is rebuilt per job; the reranker is trained on web text, not code.

## 12. Version 2 roadmap (proposals, not yet decided; each needs the owner's go-ahead and dependency approval)

Suggested order: webhook (smallest), multi-language chunking, then the sandbox (largest and riskiest).

### 12.1 Multi-language AST chunking
- Where: `app/ingestion/ast_chunker.py` is Python-only; `CodeChunk` and the `code_chunks` table have no language.
- Likely approach: dispatch by file extension to per-language chunkers using tree-sitter (already in
  requirements, but each language needs its grammar package: a new dependency to approve); add a `language`
  column (the schema uses `CREATE TABLE IF NOT EXISTS`, so add an `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`
  migration); keep the chunk contract (qualified name, line range, skeleton-for-classes).
- Ripple effects: `verify_syntax_and_lint` and `check_fix` are Python-specific (need a per-language verifier,
  for example a tree-sitter error-node check); prompts say "Python"; `FIXABLE_TYPES`; `_reindent`; fixture repos
  and tests per language. The patch builder, retrieval and queue are already language-agnostic.
- Risks: per-language definitions of function/method/class (arrow functions, closures, impl blocks); embedding
  quality for non-Python code; stale-index rules unchanged.

### 12.2 Test execution sandbox (pytest)
- Goal: run the target repo's tests against the patched code before calling a fix "verified".
- Likely approach: new graph node after `verify` (copy the repo to a temp dir, apply the replacement, run tests,
  feed failures back into `draft_fix`); run a baseline test pass first to separate pre-existing failures; store
  test results in the job result and stream them as events.
- **Security is the hard part:** running repository code is arbitrary code execution. Do not run it in the worker
  process. Options to evaluate: a separate sandbox service with no network, no secrets, CPU/memory/time limits,
  read-only root and a non-root user; per-run containers. Mounting the Docker socket into the worker is
  effectively root on the host, so avoid it; consider gVisor, nsjail or a dedicated runner. The target repo's
  dependencies must be installed in the sandbox image (per-repo configuration).
- Add a `tests_passed` dimension to the PR body, replacing today's "tests were not run" note when applicable.

### 12.3 GitHub webhook integration
- New endpoint (for example `POST /api/v1/webhooks/github`). GitHub cannot send `X-API-Key`, so it needs its own
  auth: verify `X-Hub-Signature-256` (HMAC-SHA256 of the raw body with a shared secret, constant-time compare);
  use `X-GitHub-Event` for routing and `X-GitHub-Delivery` for idempotency (store seen ids in Redis with a TTL).
- Candidate triggers: failed CI runs (fetch the run's logs through the API, then enqueue a job); optionally
  `push` events to re-index a repo automatically (also fixes index freshness).
- Needs: a publicly reachable URL (reverse proxy or tunnel), `GITHUB_WEBHOOK_SECRET`, a token that can read
  Actions logs, a mapping from `owner/repo` to the internal `repo_name` (inverse of `AUTODEV_GITHUB_REPOS`),
  log-size limits, and the existing draft-PR step for the output.

### 12.4 Smaller improvements worth batching in
Make `_reindent` public; cache the BM25 retriever per repo with invalidation on re-index; split API and worker
requirements/images; module-level chunks; per-user API keys; remove unused `handlers.py` and the `tree-sitter`
requirement if not used by 12.1.

## 13. How to start a Version 2 chat

1. Tag v1.0, create a branch per feature (`git checkout -b v2/<feature>`).
2. Open a new chat and attach: this file, `README.md`, and only the source files the feature touches.
3. Paste something like: "Read PROJECT_CONTEXT.md. We follow section 2 (working agreement). Task: <one
   feature>. Start with a short plan and the dependency questions; do not write code until I answer."
4. After each part: run the affected `verify_*.py` plus the full regression, then commit.