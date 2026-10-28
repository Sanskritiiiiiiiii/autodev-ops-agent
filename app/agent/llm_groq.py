import json
import os

from app.agent.prompts import (
    ANALYZE_SYSTEM, ANALYZE_TOOL, FIX_SYSTEM,
    build_analyze_prompt, build_fix_prompt, build_fix_tool,
)
from app.agent.tools import FIXABLE_TYPES


class LLMOutputError(Exception):
    """The model did not return a usable tool call."""


def _strip_fences(code: str) -> str:
    s = code.strip("\n")
    if s.lstrip().startswith("```"):
        lines = s.split("\n")[1:]  # drop the opening fence (and optional language tag)
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines)
    return s


def _tool_args(resp, expected_name: str) -> dict:
    message = resp.choices[0].message
    calls = getattr(message, "tool_calls", None) or []
    if not calls:
        raise LLMOutputError("model returned no tool call")
    call = calls[0]
    if call.function.name != expected_name:
        raise LLMOutputError(f"unexpected tool '{call.function.name}', wanted '{expected_name}'")
    try:
        args = json.loads(call.function.arguments)
    except (json.JSONDecodeError, TypeError) as exc:
        raise LLMOutputError(f"tool arguments are not valid JSON: {exc}") from exc
    if not isinstance(args, dict):
        raise LLMOutputError("tool arguments are not a JSON object")
    return args


def _nonempty(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


class GroqLLM:
    """LLM protocol implementation over an OpenAI-compatible chat client."""

    def __init__(self, client, model: str, max_tokens: int = 3000):
        self.client = client
        self.model = model
        self.max_tokens = max_tokens

    def _call(self, system: str, user: str, tool: dict) -> dict:
        name = tool["function"]["name"]
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": user}],
            tools=[tool],
            tool_choice={"type": "function", "function": {"name": name}},
            temperature=0,
            max_tokens=self.max_tokens,
        )
        return _tool_args(resp, name)

    def analyze(self, log: str) -> dict:
        args = self._call(ANALYZE_SYSTEM, build_analyze_prompt(log), ANALYZE_TOOL)
        if not (_nonempty(args.get("hypothesis")) and _nonempty(args.get("search_query"))):
            raise LLMOutputError("analysis is missing 'hypothesis' or 'search_query'")
        return {"hypothesis": args["hypothesis"].strip(),
                "search_query": args["search_query"].strip()}

    def draft_fix(self, log: str, hypothesis: str, chunks: list[dict],
                  feedback: str | None) -> dict:
        empty = {"hypothesis": hypothesis, "chunk_id": None, "new_code": None}
        fixable_ids = [c["chunk_id"] for c in chunks if c["chunk_type"] in FIXABLE_TYPES]
        if not fixable_ids:
            return empty  # nothing replaceable; Verify will reject and the loop ends
        try:
            args = self._call(FIX_SYSTEM, build_fix_prompt(log, hypothesis, chunks, feedback),
                              build_fix_tool(fixable_ids))
        except LLMOutputError:
            return empty  # Verify rejects it with feedback and the loop retries
        return {
            "hypothesis": args["hypothesis"].strip() if _nonempty(args.get("hypothesis")) else hypothesis,
            "chunk_id": args["chunk_id"] if isinstance(args.get("chunk_id"), str) else None,
            "new_code": _strip_fences(args["new_code"]) if _nonempty(args.get("new_code")) else None,
        }


def make_groq_llm() -> GroqLLM:
    api_key = os.environ.get("GROQ_API_KEY")
    model = os.environ.get("GROQ_MODEL")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY is not set")
    if not model:
        raise RuntimeError("GROQ_MODEL is not set (choose a tool-calling model from the Groq docs)")
    from groq import Groq  # imported lazily so offline tests don't need the package

    return GroqLLM(Groq(api_key=api_key), model)