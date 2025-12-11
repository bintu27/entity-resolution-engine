import re
from typing import Optional, Tuple

SEASON_REGEXES = [
    re.compile(r"(?P<start>\d{4})\s*[-/]\s*(?P<end>\d{2,4})"),
    re.compile(r"(?P<year>\d{4})"),
]


def normalize_season(season_name: str) -> Tuple[Optional[int], Optional[int]]:
    if not season_name:
        return None, None
    for regex in SEASON_REGEXES:
        match = regex.search(season_name)
        if match:
            if "year" in match.groupdict():
                year = int(match.group("year"))
                return year, year + 1
            start = int(match.group("start"))
            raw_end = match.group("end")
            end = int(raw_end) if len(raw_end) == 4 else int(f"{str(start)[:2]}{raw_end}")
            if end < start:
                end = start + 1
            return start, end
    return None, None
