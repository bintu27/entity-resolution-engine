#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

python scripts/ci/generate_openapi.py --output reports/openapi-current.json
python scripts/ci/validate_openapi.py --spec reports/openapi-current.json
