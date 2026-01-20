#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

pytest -m contract --junitxml=reports/contract-junit.xml
