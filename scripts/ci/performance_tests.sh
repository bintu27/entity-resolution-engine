#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

pytest -m performance --junitxml=reports/performance-junit.xml
