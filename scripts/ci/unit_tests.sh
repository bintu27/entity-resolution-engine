#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

pytest -m "not contract" \
  --junitxml=reports/junit.xml \
  --cov=entity_resolution_engine \
  --cov-branch \
  --cov-report=term-missing \
  --cov-report=xml:reports/coverage.xml \
  --cov-report=html:reports/coverage_html
