import json
import urllib.error
import urllib.request

DEFAULT_API = "https://api.github.com"


class GitHubError(Exception):
    def __init__(self, status: int, message: str):
        text = f"GitHub API error {status}: {message}" if status else f"network error: {message}"
        super().__init__(text)
        self.status = status
        self.message = message


class GitHubClient:
    """Minimal GitHub REST client over urllib. The token is only ever sent as a header."""

    def __init__(self, token: str, api_base: str = DEFAULT_API, timeout: float = 20.0):
        self._token = token
        self.api_base = api_base.rstrip("/")
        self.timeout = timeout

    def request(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(self.api_base + path, data=data, method=method, headers={
            "Authorization": f"Bearer {self._token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "autodev-ops-agent",
            "Content-Type": "application/json",
        })
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            try:
                message = json.loads(raw).get("message", exc.reason)
            except (ValueError, AttributeError):
                message = exc.reason
            raise GitHubError(exc.code, str(message)) from None
        except (urllib.error.URLError, OSError) as exc:
            raise GitHubError(0, str(getattr(exc, "reason", exc))) from None