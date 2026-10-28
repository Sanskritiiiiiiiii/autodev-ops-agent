from rich.console import Console, Group
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text


def exit_code_for(event_type: str, data: dict) -> int | None:
    """0 verified fix, 1 no verified fix, 3 job failed, 4 stream problem; None = not terminal."""
    if event_type == "job_completed":
        result = data.get("result") or {}
        return 0 if result.get("evaluation_status") == "passed" else 1
    if event_type == "job_failed":
        return 3
    if event_type in ("timeout", "stream_error"):
        return 4
    return None


class EventRenderer:
    def __init__(self, console: Console):
        self.console = console

    def render(self, event_type: str, data: dict) -> None:
        c = self.console
        if event_type == "job_started":
            c.print(Text(f"> Job started (attempt {data.get('attempt', '?')})", style="bold cyan"))
        elif event_type == "step":
            self._step(data)
        elif event_type == "job_retrying":
            c.print(Text(f"! Attempt {data.get('attempt')} failed, will retry: {data.get('error')}",
                         style="yellow"))
        elif event_type == "job_completed":
            self._completed(data.get("result"))
        elif event_type == "job_failed":
            c.print(Panel(Text(str(data.get("error", "unknown error"))),
                          title="Job failed", border_style="red"))
        elif event_type == "timeout":
            c.print(Text("Stream timed out waiting for new events.", style="yellow"))
        elif event_type == "stream_error":
            c.print(Text(f"Stream error: {data.get('error')}", style="red"))

    def _step(self, d: dict) -> None:
        c, node = self.console, d.get("node")
        if node == "analyze_log":
            body = Text.assemble(("Hypothesis\n", "bold"), d["hypothesis"] + "\n\n",
                                 ("Search query  ", "bold"), d["search_query"])
            c.print(Panel(body, title="1. Analyze log", border_style="blue"))
        elif node == "retrieve_code":
            table = Table(title="2. Retrieved code")
            for col in ("Chunk", "Type", "Location"):
                table.add_column(col)
            for ch in d["chunks"]:
                table.add_row(ch["qualified_name"], ch["chunk_type"],
                              f"{ch['file_path']}:{ch['start_line']}-{ch['end_line']}")
            c.print(table)
        elif node == "draft_fix":
            code = d.get("new_code") or "(the model returned no code)"
            body = Group(
                Text.assemble(("Hypothesis\n", "bold"), d.get("hypothesis", "")),
                Text.assemble(("Target  ", "bold"), str(d.get("chunk_id"))),
                Syntax(code, "python", word_wrap=True),
            )
            c.print(Panel(body, title=f"3. Draft fix (attempt {d.get('attempt')})", border_style="magenta"))
        elif node == "verify":
            if d.get("evaluation_status") == "passed":
                c.print(Text("4. Verification passed", style="bold green"))
            else:
                c.print(Text.assemble(("4. Verification failed: ", "bold red"), d.get("feedback") or ""))
        elif node == "output_diff":
            c.print(Panel(Syntax(d["diff"], "diff", word_wrap=True),
                          title="5. Proposed patch (git diff preview)", border_style="green"))

    def _completed(self, result: dict | None) -> None:
        c = self.console
        if not result:
            c.print(Text("Job completed (no result stored).", style="bold"))
            return
        if result.get("evaluation_status") == "passed":
            c.print(Panel(Text(f"Verified fix found in {result['attempts']} attempt(s).", style="bold green"),
                          title="Result", border_style="green"))
        else:
            c.print(Panel(Text.assemble(
                (f"No verified fix after {result.get('attempts')} attempt(s).\n", "bold red"),
                str(result.get("verification_feedback") or "")),
                title="Result", border_style="red"))
        if result.get("patch_path"):
            c.print(Text.assemble(("Patch file: ", "bold"), result["patch_path"]))
            c.print(Text.assemble(("Apply it with: ", "bold"), f"git apply {result['patch_path']}"))
        elif result.get("patch_error"):
            c.print(Text(f"Patch could not be generated: {result['patch_error']}", style="yellow"))