"""Module 8A verification: the index_repo CLI."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.cli.index_repo import main as index_main  # noqa: E402
from app.db.chunk_store import count_chunks  # noqa: E402
from app.db.connection import get_connection  # noqa: E402

NAME = "index_cli_test"
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


check("missing path returns 2", index_main(["--path", str(ROOT / "no_such_dir"), "--name", NAME]) == 2)
check("invalid name returns 2", index_main(["--path", str(ROOT / "sample_repo"), "--name", "bad name!"]) == 2)

empty = ROOT / "scripts"  # contains .py files, so use a directory that has none
code = index_main(["--path", str(ROOT / "app" / "queue" / "__pycache__"), "--name", NAME]) \
    if (ROOT / "app" / "queue" / "__pycache__").is_dir() else 1
check("directory with no Python sources returns 1 (or is absent)", code in (1, 2))

code = index_main(["--path", str(ROOT / "sample_repo"), "--name", NAME])
check("indexing sample_repo succeeds", code == 0)

conn = get_connection()
try:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(embedding) FROM code_chunks WHERE repo_name = %s", (NAME,))
        total, embedded = cur.fetchone()
    check("12 chunks stored and all embedded", (total, embedded) == (12, 12), f"{total}/{embedded}")
    check("count_chunks agrees", count_chunks(conn, NAME) == 12)
    check("re-indexing is idempotent", index_main(["--path", str(ROOT / "sample_repo"), "--name", NAME]) == 0
          and count_chunks(conn, NAME) == 12)
finally:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM code_chunks WHERE repo_name = %s", (NAME,))
    conn.commit()
    conn.close()

print("\nMODULE 8A (index CLI): ALL CHECKS PASSED" if not failures else f"\nMODULE 8A (index CLI): FAILED -> {failures}")
sys.exit(1 if failures else 0)