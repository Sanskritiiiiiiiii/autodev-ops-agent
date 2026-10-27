"""Module 2 verification: AST ingestion on sample_repo."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # lets `import app...` work when run from scripts/

from rich.console import Console  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.syntax import Syntax  # noqa: E402
from rich.table import Table  # noqa: E402

from app.ingestion.ast_chunker import ingest_repo  # noqa: E402

REPO = ROOT / "sample_repo"

EXPECTED = {
    ("shop/db.py", "class", "DatabaseTimeout"),
    ("shop/db.py", "class", "ConnectionPool"),
    ("shop/db.py", "method", "ConnectionPool.__init__"),
    ("shop/db.py", "method", "ConnectionPool.acquire"),
    ("shop/db.py", "method", "ConnectionPool.release"),
    ("shop/db.py", "function", "run_query"),
    ("shop/orders.py", "function", "get_order"),
    ("shop/orders.py", "function", "fetch_orders_async"),
    ("shop/orders.py", "class", "OrderService"),
    ("shop/orders.py", "method", "OrderService.__init__"),
    ("shop/orders.py", "method", "OrderService.normalize_id"),
    ("shop/orders.py", "method", "OrderService.cancel"),
}

console = Console()
result = ingest_repo(REPO)
by_qn = {(c.file_path, c.qualified_name): c for c in result.chunks}

# ---- display ----
table = Table(title="Extracted chunks")
for col in ("File", "Type", "Qualified name", "Lines"):
    table.add_column(col)
for c in result.chunks:
    table.add_row(c.file_path, c.chunk_type, c.qualified_name, f"{c.start_line}-{c.end_line}")
console.print(table)

for key in [("shop/db.py", "ConnectionPool"), ("shop/db.py", "ConnectionPool.acquire")]:
    c = by_qn.get(key)
    if c:
        console.print(Panel(
            Syntax(c.content, "python", line_numbers=True, start_line=c.start_line),
            title=f"{c.chunk_type}: {c.qualified_name}  ({c.file_path})",
        ))

console.print(f"\nSkipped files: {[(s.file_path, s.reason) for s in result.skipped]}\n")

# ---- assertions ----
failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


actual = {(c.file_path, c.chunk_type, c.qualified_name) for c in result.chunks}
check("chunk set matches expected", actual == EXPECTED,
      f"missing={EXPECTED - actual} extra={actual - EXPECTED}")
check("exactly 12 chunks", len(result.chunks) == 12, f"got {len(result.chunks)}")
check("chunk_ids unique", len({c.chunk_id for c in result.chunks}) == len(result.chunks))
check("broken file skipped", [s.file_path for s in result.skipped] == ["shop/broken_syntax.py"],
      f"got {result.skipped}")

# function/method content must equal the exact file lines it claims
ok_ranges = True
for c in result.chunks:
    if c.chunk_type == "class":
        continue
    file_lines = (REPO / c.file_path).read_text(encoding="utf-8").split("\n")
    if "\n".join(file_lines[c.start_line - 1:c.end_line]) != c.content:
        ok_ranges = False
        print("   range mismatch:", c.chunk_id)
check("function/method line ranges match source exactly", ok_ranges)

acq = by_qn.get(("shop/db.py", "ConnectionPool.acquire"))
check("method metadata (class_name, lines 19-26)",
      acq is not None and acq.class_name == "ConnectionPool"
      and (acq.start_line, acq.end_line) == (19, 26))

pool = by_qn.get(("shop/db.py", "ConnectionPool"))
check("class chunk keeps attributes, drops method bodies",
      pool is not None and "max_size = 5" in pool.content and "def acquire" not in pool.content
      and (pool.start_line, pool.end_line) == (10, 29))

norm = by_qn.get(("shop/orders.py", "OrderService.normalize_id"))
check("decorator included in span (starts at line 16)",
      norm is not None and norm.start_line == 16
      and norm.content.lstrip().startswith("@staticmethod"))

fa = by_qn.get(("shop/orders.py", "fetch_orders_async"))
check("async function captured",
      fa is not None and fa.chunk_type == "function" and fa.content.startswith("async def"))

ids_a = [c.chunk_id for c in ingest_repo(REPO).chunks]
check("deterministic across runs", ids_a == [c.chunk_id for c in result.chunks])

print("\nMODULE 2: ALL CHECKS PASSED" if not failures else f"\nMODULE 2: FAILED -> {failures}")
sys.exit(1 if failures else 0)