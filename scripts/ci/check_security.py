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
    vuln_count = 0
    if isinstance(data, dict):
        if "vulnerabilities" in data:
            vuln_count = len(data.get("vulnerabilities", []))
        elif "dependencies" in data:
            for dependency in data.get("dependencies", []):
                vuln_count += len(dependency.get("vulns", []))
    elif isinstance(data, list):
        for dependency in data:
            vuln_count += len(dependency.get("vulns", []))

    if args.baseline:
        baseline_data = json.loads(Path(args.baseline).read_text())
        max_vulns = baseline_data.get("max_vulnerabilities", 0)
    else:
        max_vulns = 0

    if vuln_count > max_vulns:
        raise SystemExit(
            f"Security gate failed: {vuln_count} vulnerabilities > {max_vulns} allowed"
        )


if __name__ == "__main__":
    main()
