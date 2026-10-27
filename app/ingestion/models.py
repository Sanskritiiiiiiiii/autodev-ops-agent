from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class CodeChunk:
    chunk_id: str            # "<file_path>::<qualified_name>@<start_line>"
    file_path: str           # POSIX-style, relative to repo root
    chunk_type: str          # "function" | "method" | "class"
    name: str
    qualified_name: str      # "func" or "Class.method"
    class_name: str | None
    start_line: int          # 1-based, inclusive (includes decorators)
    end_line: int            # 1-based, inclusive
    content: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SkippedFile:
    file_path: str
    reason: str


@dataclass
class IngestResult:
    chunks: list[CodeChunk]
    skipped: list[SkippedFile]