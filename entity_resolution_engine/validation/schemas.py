from __future__ import annotations

from typing import List, Literal

from pydantic import BaseModel, Field


class ValidationResult(BaseModel):
    decision: Literal["MATCH", "NO_MATCH", "REVIEW"]
    confidence: float = Field(ge=0.0, le=1.0)
    reasons: List[str] = Field(default_factory=list)
    risk_flags: List[str] = Field(default_factory=list)
