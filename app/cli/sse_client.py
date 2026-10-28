import http.client
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

STREAM_END_TYPES = ("job_completed", "job_failed", "timeout", "stream_error")


@dataclass
class SseEvent:
    id: str | None
    event: str
    data: str


def submit_job(api_url: str, repo_name: str, log: str, timeout: float = 10.0) -> str:
    body = json.dumps({"repo_name": repo_name, "log": log}).encode()
    req = urllib.request.Request(
        f"{api_url}/api/v1/triage/submit", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())["job_id"]


def iter_sse(url: str, last_event_id: str | None = None, timeout: float = 60.0):
    """Yield SseEvent objects from one SSE connection until the server closes it."""
    headers = {"Accept": "text/event-stream"}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        ev_id, ev_type, data_lines = None, "message", []
        for raw in resp:
            line = raw.decode("utf-8").rstrip("\r\n")
            if not line:  # blank line = dispatch
                if data_lines:
                    yield SseEvent(ev_id, ev_type, "\n".join(data_lines))
                ev_type, data_lines = "message", []
                continue
            if line.startswith(":"):  # comment / ping
                continue
            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if field == "event":
                ev_type = value
            elif field == "data":
                data_lines.append(value)
            elif field == "id":
                ev_id = value


def stream_job(api_url: str, job_id: str, max_reconnects: int = 3,
               retry_delay: float = 1.0, read_timeout: float = 60.0):
    """Yield a job's events; reconnect with Last-Event-ID if the connection drops.
    Stops after a terminal event. HTTP errors (404, 400) are raised, not retried."""
    url = f"{api_url}/api/v1/triage/stream/{job_id}"
    last_id: str | None = None
    failures = 0
    while True:
        try:
            for ev in iter_sse(url, last_id, read_timeout):
                failures = 0
                if ev.id:
                    last_id = ev.id
                yield ev
                if ev.event in STREAM_END_TYPES:
                    return
        except urllib.error.HTTPError:
            raise
        except (OSError, http.client.HTTPException):
            pass  # dropped connection or timeout: fall through and reconnect
        failures += 1
        if failures > max_reconnects:
            raise ConnectionError("event stream dropped and the reconnect limit was reached")
        time.sleep(retry_delay)