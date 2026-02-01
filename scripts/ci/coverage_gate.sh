#!/usr/bin/env bash
set -euo pipefail

python -m coverage json -o reports/coverage.json
if [ -f ".ci/coverage_baseline.json" ]; then
  python scripts/ci/check_coverage.py --report reports/coverage.json --baseline .ci/coverage_baseline.json
else
  python scripts/ci/check_coverage.py --report reports/coverage.json --line-min 80 --branch-min 70
fi
