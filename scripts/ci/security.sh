#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

pip-audit -r requirements.txt -r requirements-dev.txt -f json -o reports/pip-audit.json
bandit -r entity_resolution_engine -q -f json -o reports/bandit.json
