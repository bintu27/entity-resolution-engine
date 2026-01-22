#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

set +e
pip-audit -r requirements.txt -r requirements-dev.txt -f json -o reports/pip-audit.json
pip_audit_status=$?
set -e
if [ $pip_audit_status -ne 0 ] && [ ! -f reports/pip-audit.json ]; then
  echo "pip-audit failed to produce a report; proceeding with empty report."
  echo "{}" > reports/pip-audit.json
fi
if [ -f ".ci/pip_audit_baseline.json" ]; then
  python scripts/ci/check_security.py --report reports/pip-audit.json --baseline .ci/pip_audit_baseline.json
else
  python scripts/ci/check_security.py --report reports/pip-audit.json
fi
set +e
bandit -r entity_resolution_engine -q -lll -f json -o reports/bandit.json
bandit_status=$?
set -e
if [ $bandit_status -ne 0 ] && [ ! -f reports/bandit.json ]; then
  echo "bandit failed to produce a report; proceeding with empty report."
  echo "{}" > reports/bandit.json
fi
if [ -f ".ci/bandit_baseline.json" ]; then
  python scripts/ci/check_bandit.py --report reports/bandit.json --baseline .ci/bandit_baseline.json
else
  python scripts/ci/check_bandit.py --report reports/bandit.json
fi
