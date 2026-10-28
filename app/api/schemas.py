from pydantic import BaseModel, Field


class TriageSubmitRequest(BaseModel):
    repo_name: str = Field(min_length=1, max_length=200)
    log: str = Field(min_length=1, max_length=200_000)  # raw stack trace / log text


class TriageSubmitResponse(BaseModel):
    job_id: str
    status: str