"""Index a repository so the agent can search it.

    python -m app.cli.index_repo --path /repos/sample_repo --name sample_repo

--name is what you pass as the repo when submitting jobs; the patch step expects
the checkout at <AUTODEV_REPOS_DIR>/<name>.
"""
import argparse
import re
import sys
from pathlib import Path

from app.db.chunk_store import init_schema, init_vector_index, replace_repo_chunks
from app.db.connection import get_connection
from app.ingestion.ast_chunker import ingest_repo
from app.search.dense_search import embed_missing_chunks
from app.search.embedder import Embedder

_REPO_NAME = re.compile(r"^[A-Za-z0-9._-]+$")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="index_repo", description="Parse, chunk and embed a repository")
    parser.add_argument("--path", required=True, help="repository directory to index")
    parser.add_argument("--name", required=True, help="repo name used when submitting jobs")
    args = parser.parse_args(argv)

    path = Path(args.path)
    if not path.is_dir():
        print(f"Not a directory: {path}")
        return 2
    if not _REPO_NAME.match(args.name) or args.name in (".", ".."):
        print(f"Invalid --name {args.name!r}: use letters, digits, '.', '_' or '-'")
        return 2
    if path.resolve().name != args.name:
        print(f"Note: patches expect the checkout at <AUTODEV_REPOS_DIR>/{args.name}, "
              f"but the folder is named '{path.resolve().name}'.")

    result = ingest_repo(path)
    if not result.chunks:
        print("No Python functions or classes found; nothing to index.")
        return 1

    conn = get_connection()
    try:
        init_schema(conn)
        init_vector_index(conn)
        stored = replace_repo_chunks(conn, args.name, result.chunks)
        print(f"Stored {stored} chunks; {len(result.skipped)} file(s) skipped.")
        for s in result.skipped:
            print(f"  skipped {s.file_path}: {s.reason}")
        print("Embedding chunks (the first run downloads the model)...")
        embedded = embed_missing_chunks(conn, args.name, Embedder())
        print(f"Embedded {embedded} chunks. Repo '{args.name}' is ready.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())