import os

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

STREAM_KEY = "autodev:triage:jobs"
JOB_KEY_PREFIX = "autodev:job:"
CONSUMER_GROUP = "triage-workers"