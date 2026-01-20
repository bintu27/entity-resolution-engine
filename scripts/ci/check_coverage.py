import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True)
    parser.add_argument("--line-min", type=float, required=True)
    parser.add_argument("--branch-min", type=float, required=True)
    args = parser.parse_args()

    report_path = Path(args.report)
    data = json.loads(report_path.read_text())
    totals = data.get("totals", {})

    line_rate = totals.get("percent_covered", 0.0)
    branch_rate = totals.get("percent_covered_branches", 0.0)

    failures = []
    if line_rate < args.line_min:
        failures.append(f"line coverage {line_rate:.2f}% < {args.line_min:.2f}%")
    if branch_rate < args.branch_min:
        failures.append(f"branch coverage {branch_rate:.2f}% < {args.branch_min:.2f}%")

    if failures:
        raise SystemExit("Coverage gate failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
