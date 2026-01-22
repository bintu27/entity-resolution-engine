import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", required=True)
    parser.add_argument("--baseline")
    args = parser.parse_args()

    report_path = Path(args.report)
    data = json.loads(report_path.read_text())
    issue_count = len(data.get("results", [])) if isinstance(data, dict) else 0

    if args.baseline:
        baseline_data = json.loads(Path(args.baseline).read_text())
        max_issues = baseline_data.get("max_issues", 0)
    else:
        max_issues = 0

    if issue_count > max_issues:
        raise SystemExit(
            f"Security gate failed: {issue_count} bandit issues > {max_issues} allowed"
        )


if __name__ == "__main__":
    main()
