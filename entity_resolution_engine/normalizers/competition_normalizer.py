import re
from pathlib import Path
import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "normalization.yml"
with CONFIG_PATH.open() as f:
    CONFIG = yaml.safe_load(f)

SPONSORS = [phrase.lower() for phrase in CONFIG.get("competition_sponsors", [])]


def normalize_competition(name: str) -> str:
    if not name:
        return ""
    lowered = name.lower()
    for sponsor in SPONSORS:
        lowered = lowered.replace(sponsor, "")
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered
