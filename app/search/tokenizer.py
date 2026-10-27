import re

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_CAMEL = re.compile(r"[A-Z]+(?=[A-Z][a-z])|[A-Z]?[a-z]+|[A-Z]+|[0-9]+")


def tokenize(text: str) -> list[str]:
    tokens: list[str] = []
    for ident in _IDENT.findall(text):
        whole = ident.lower()
        tokens.append(whole)
        parts: list[str] = []
        for piece in ident.split("_"):
            if piece:
                parts.extend(m.lower() for m in _CAMEL.findall(piece))
        if parts != [whole]:
            tokens.extend(parts)
    return tokens