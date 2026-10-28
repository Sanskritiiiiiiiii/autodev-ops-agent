import os
from pathlib import Path

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

STREAM_KEY = "autodev:triage:jobs"
JOB_KEY_PREFIX = "autodev:job:"
CONSUMER_GROUP = "triage-workers"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Repo checkouts live at <REPOS_DIR>/<repo_name>; patches are written to PATCH_DIR.
REPOS_DIR = Path(os.environ.get("AUTODEV_REPOS_DIR", str(PROJECT_ROOT)))
PATCH_DIR = Path(os.environ.get("AUTODEV_PATCH_DIR", str(PROJECT_ROOT / "patches")))

# Finished (completed or failed) job hashes expire after this many seconds.
JOB_TTL_S = int(os.environ.get("AUTODEV_JOB_TTL_S", str(7 * 24 * 3600)))