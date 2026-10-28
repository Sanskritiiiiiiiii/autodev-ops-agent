import hmac
import os

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

MIN_KEY_LENGTH = 16
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def configured_key(environ=None) -> str:
    env = os.environ if environ is None else environ
    return (env.get("AUTODEV_API_KEY") or "").strip()


def check_api_key_config(environ=None) -> bool:
    """Validate the setting at startup. Returns True if authentication is enabled."""
    key = configured_key(environ)
    if not key:
        return False
    if len(key) < MIN_KEY_LENGTH:
        raise RuntimeError(f"AUTODEV_API_KEY must be at least {MIN_KEY_LENGTH} characters")
    return True


async def require_api_key(provided: str | None = Security(_api_key_header)) -> None:
    expected = configured_key()
    if not expected:
        return  # authentication disabled (local development)
    if not provided or not hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8")):
        raise HTTPException(status_code=401, detail="invalid or missing API key",
                            headers={"WWW-Authenticate": "ApiKey"})