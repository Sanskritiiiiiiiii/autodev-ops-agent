import argparse
import json
import os
import sys
import urllib.error
from pathlib import Path

from rich.console import Console

from app.cli.render import EventRenderer, exit_code_for
from app.cli.sse_client import stream_job, submit_job


def watch(api_url: str, job_id: str, console: Console, api_key: str | None = None) -> int:
    renderer = EventRenderer(console)
    code = 4  # stays 4 if the stream ends without a terminal event
    with console.status("Waiting for the agent...", spinner="dots"):
        for ev in stream_job(api_url, job_id, api_key=api_key):
            data = json.loads(ev.data)
            renderer.render(ev.event, data)
            final = exit_code_for(ev.event, data)
            if final is not None:
                code = final
    return code


def main(argv: list[str] | None = None, environ=None) -> int:
    parser = argparse.ArgumentParser(prog="triage_cli", description="AutoDev-Ops Agent CLI")
    parser.add_argument("--api", default="http://127.0.0.1:8000", help="API base URL")
    sub = parser.add_subparsers(dest="command", required=True)
    p_submit = sub.add_parser("submit", help="submit a log and watch the agent live")
    p_submit.add_argument("--repo", required=True, help="indexed repo name, e.g. sample_repo")
    p_submit.add_argument("--log-file", required=True, help="file containing the stack trace / log")
    p_watch = sub.add_parser("watch", help="watch (or replay) an existing job")
    p_watch.add_argument("job_id")
    args = parser.parse_args(argv)

    env = os.environ if environ is None else environ
    api_key = (env.get("AUTODEV_API_KEY") or "").strip() or None  # environment only, never a flag

    console = Console()
    try:
        if args.command == "submit":
            log = Path(args.log_file).read_text(encoding="utf-8")
            job_id = submit_job(args.api, args.repo, log, api_key=api_key)
            console.print(f"Submitted job {job_id}")
        else:
            job_id = args.job_id
        return watch(args.api, job_id, console, api_key)
    except FileNotFoundError as exc:
        console.print(f"File not found: {exc.filename}", style="red")
        return 4
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            console.print("Authentication failed: set AUTODEV_API_KEY to the server's API key.", style="red")
        else:
            console.print(f"HTTP {exc.code}: {exc.reason}", style="red")
        return 4
    except (OSError, ConnectionError) as exc:
        console.print(f"Cannot reach the API at {args.api}: {exc}", style="red")
        return 4
    except KeyboardInterrupt:
        console.print("Interrupted.", style="yellow")
        return 4


if __name__ == "__main__":
    sys.exit(main())