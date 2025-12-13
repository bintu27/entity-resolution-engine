import re
from typing import Optional, Tuple

SEASON_REGEXES = [
    re.compile(r"(?P<start>\d{2,4})\s*[-/]\s*(?P<end>\d{2,4})"),
    re.compile(r"(?P<year>\d{4})"),
]


def _expand_year(fragment: str, reference_start: Optional[int] = None) -> int:
    if len(fragment) == 4:
        return int(fragment)
    value = int(fragment)
    if reference_start is not None:
        return int(f"{str(reference_start)[:2]}{fragment}")
    # assume seasons described with two digits are modern (2000s) unless >30
    return 2000 + value if value <= 30 else 1900 + value


def normalize_season(season_name: str) -> Tuple[Optional[int], Optional[int]]:
    if not season_name:
        return None, None
    for regex in SEASON_REGEXES:
        match = regex.search(season_name)
        if match:
            if "year" in match.groupdict():
                year = int(match.group("year"))
                return year, year + 1
            start_str = match.group("start")
            start = _expand_year(start_str)
            raw_end = match.group("end")
            end = _expand_year(raw_end, reference_start=start)
            if end < start:
                end = start + 1
            return start, end
    return None, None
