import re
import unicodedata
from typing import Optional

from rapidfuzz import fuzz


PUNCT_PATTERN = re.compile(r"[^\w\s]")
ALIAS_PATTERNS = [
    (re.compile(r"\bfc\b"), "football club"),
]


def normalize_name(name: Optional[str]) -> str:
    if not name:
        return ""
    text = unicodedata.normalize("NFKD", name)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower().strip()
    text = PUNCT_PATTERN.sub(" ", text)
    text = re.sub(r"\s+", " ", text)
    for pattern, replacement in ALIAS_PATTERNS:
        if pattern.search(text):
            text = pattern.sub(replacement, text)
    text = re.sub(r"\s+", " ", text)
    return text


def token_sort_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.token_sort_ratio(a, b) / 100.0


def simple_ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return fuzz.ratio(a, b) / 100.0
