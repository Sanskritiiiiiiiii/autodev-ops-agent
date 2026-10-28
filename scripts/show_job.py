"""Pretty-print a job: python scripts/show_job.py <job_id>"""
import json
import sys
from pathlib import Path

import redis

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console  # noqa: E402
from rich.syntax import Syntax  # noqa: E402

from app.config import REDIS_HOST, REDIS_PORT  # noqa: E402
from app.queue.jobs import job_key  # noqa: E402

if len(sys.argv) != 2:
    sys.exit("usage: python scripts/show_job.py <job_id>")
h = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True).hgetall(job_key(sys.argv[1]))
if not h:
    sys.exit("job not found")

c = Console()
c.print(f"status: {h.get('status')}   attempt: {h.get('attempt')}   error: {h.get('error')}")
if "result" in h:
    res = json.loads(h["result"])
    c.print(f"evaluation: {res['evaluation_status']}   attempts: {res['attempts']}")
    c.print(f"query: {res['search_query']}\nhypothesis: {res['hypothesis']}")
    if res["verification_feedback"]:
        c.print(f"last feedback: {res['verification_feedback']}")
    if res["diff_preview"]:
        c.print(Syntax(res["diff_preview"], "diff"))