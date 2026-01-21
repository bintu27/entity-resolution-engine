import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True)
    parser.add_argument("--line-min", type=float)
    parser.add_argument("--branch-min", type=float)
    parser.add_argument("--baseline")
    args = parser.parse_args()

    report_path = Path(args.report)
    data = json.loads(report_path.read_text())
    totals = data.get("totals", {})

    line_rate = totals.get("percent_covered", 0.0)
    branch_rate = totals.get("percent_covered_branches", 0.0)

    if args.baseline:
        baseline_data = json.loads(Path(args.baseline).read_text())
        line_min = baseline_data.get("line_min", 0.0)
        branch_min = baseline_data.get("branch_min", 0.0)
    else:
        line_min = args.line_min if args.line_min is not None else 80.0
        branch_min = args.branch_min if args.branch_min is not None else 70.0

    failures = []
    if line_rate < line_min:
        failures.append(f"line coverage {line_rate:.2f}% < {line_min:.2f}%")
    if branch_rate < branch_min:
        failures.append(f"branch coverage {branch_rate:.2f}% < {branch_min:.2f}%")

    if failures:
        raise SystemExit("Coverage gate failed: " + "; ".join(failures))


if __name__ == "__main__":
    main()
