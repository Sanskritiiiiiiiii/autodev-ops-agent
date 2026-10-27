import os
import sys
from pathlib import Path

import psycopg2
import redis

# Print headers using basic formatting
print("==========================================")
print("       INFRASTRUCTURE VERIFICATION        ")
print("==========================================")

# 1. Python Version Check
py_version = sys.version.split()[0]
if sys.version_info >= (3, 11):
    print(f"[PASS] Python version        {py_version}")
else:
    print(f"[FAIL] Python version        {py_version} (Requires >= 3.11)")

# 2. Dependency Imports Check
required_modules = [
    "fastapi",
    "uvicorn",
    "pydantic",
    "sqlalchemy",
    "psycopg2",
    "pgvector",
    "redis",
    "rich",
    "httpx",
]

missing_modules = []
for mod in required_modules:
    try:
        __import__(mod)
    except ImportError:
        missing_modules.append(mod)

if not missing_modules:
    print(f"[PASS] Dependency imports     {len(required_modules)} modules import OK")
else:
    print(f"[FAIL] Dependency imports     Missing: {', '.join(missing_modules)}")

# 3. PostgreSQL + pgvector Check
PG = dict(
    host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
    port=int(os.environ.get("POSTGRES_PORT", "5433")),
    user=os.environ.get("POSTGRES_USER", "autodev"),
    password=os.environ.get("POSTGRES_PASSWORD", "autodev_pw"),
    dbname=os.environ.get("POSTGRES_DB", "autodev"),
)

pg_passed = False
try:
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()

    cur.execute("SELECT version();")
    pg_ver = cur.fetchone()[0].split()[1]

    cur.execute(
        "SELECT extversion FROM pg_extension WHERE extname = 'vector';"
    )
    vec_res = cur.fetchone()
    vec_ver = vec_res[0] if vec_res else "NOT INSTALLED"

    # Quick vector sanity test
    cur.execute("SELECT '[1,2,3]'::vector <-> '[4,5,6]'::vector;")
    dist = cur.fetchone()[0]

    cur.close()
    conn.close()

    print(
        f"[PASS] PostgreSQL + pgvector PostgreSQL {pg_ver} | pgvector {vec_ver} | distance test = {dist}"
    )
    pg_passed = True
except Exception as e:
    print(f"[FAIL] PostgreSQL + pgvector {type(e).__name__}: {e}")

# 4. Redis Check
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

redis_passed = False
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, socket_timeout=3)
    r.ping()
    r.set("test_key", "test_val", ex=10)
    val = r.get("test_key")
    r.delete("test_key")

    info = r.info("server")
    redis_ver = info.get("redis_version", "unknown")

    if val == b"test_val":
        print(
            f"[PASS] Redis                PING OK | SET/GET OK | server {redis_ver}"
        )
        redis_passed = True
    else:
        print("[FAIL] Redis                SET/GET validation mismatch")
except Exception as e:
    print(f"[FAIL] Redis                {type(e).__name__}: {e}")

print("------------------------------------------")
if pg_passed and redis_passed:
    print("MODULE 1: ALL CHECKS PASSED")
else:
    print("MODULE 1: FAILED -> Check database or container setup")
print("------------------------------------------")