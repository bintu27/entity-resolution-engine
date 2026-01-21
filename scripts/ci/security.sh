#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

pip-audit -r requirements.txt -r requirements-dev.txt -f json -o reports/pip-audit.json || true
if [ -f ".ci/pip_audit_baseline.json" ]; then
  python scripts/ci/check_security.py --report reports/pip-audit.json --baseline .ci/pip_audit_baseline.json
else
  python scripts/ci/check_security.py --report reports/pip-audit.json
fi
bandit -r entity_resolution_engine -q -f json -o reports/bandit.json
