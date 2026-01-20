import argparse
import json
from pathlib import Path

from openapi_spec_validator import validate_spec


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True)
    args = parser.parse_args()

    spec = json.loads(Path(args.spec).read_text())
    validate_spec(spec)


if __name__ == "__main__":
    main()
