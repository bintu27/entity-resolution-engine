#!/usr/bin/env bash
set -euo pipefail

python -m coverage json -o reports/coverage.json
python scripts/ci/check_coverage.py --report reports/coverage.json --line-min 80 --branch-min 70
