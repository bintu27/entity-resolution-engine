#!/usr/bin/env bash
set -euo pipefail

mkdir -p reports

PYTHON_BIN="${PYTHON_BIN:-python}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  PYTHON_BIN="python3"
fi

"$PYTHON_BIN" -m pytest tests/test_llm_validation_config.py tests/test_llm_split_behavior.py \
  --junitxml=reports/llm-split-junit.xml
