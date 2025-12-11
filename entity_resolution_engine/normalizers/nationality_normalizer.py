import yaml
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "normalization.yml"
with CONFIG_PATH.open() as f:
    CONFIG = yaml.safe_load(f)

COUNTRY_MAP = {k.lower(): v for k, v in CONFIG.get("countries", {}).items()}


def normalize_country(value: str) -> str:
    if not value:
        return ""
    normalized = COUNTRY_MAP.get(value.lower(), value)
    return normalized
