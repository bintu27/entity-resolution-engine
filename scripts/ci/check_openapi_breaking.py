import argparse
import json
from pathlib import Path


def _load_schema(path: Path) -> dict:
    return json.loads(path.read_text())


def _collect_paths(schema: dict) -> dict:
    paths = {}
    for path, methods in schema.get("paths", {}).items():
        normalized = {}
        for method, payload in methods.items():
            normalized[method.lower()] = payload
        paths[path] = normalized
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--current", required=True)
    args = parser.parse_args()

    base_schema = _load_schema(Path(args.base))
    current_schema = _load_schema(Path(args.current))

    base_paths = _collect_paths(base_schema)
    current_paths = _collect_paths(current_schema)

    missing_paths = sorted(set(base_paths) - set(current_paths))
    if missing_paths:
        raise SystemExit(f"Breaking change: missing paths {missing_paths}")

    missing_methods = []
    for path, methods in base_paths.items():
        current_methods = current_paths.get(path, {})
        for method in methods:
            if method not in current_methods:
                missing_methods.append(f"{method.upper()} {path}")

    if missing_methods:
        raise SystemExit(f"Breaking change: missing methods {missing_methods}")


if __name__ == "__main__":
    main()
