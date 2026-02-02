# Entity Resolution Engine

A modular Python-based Entity Resolution Engine that merges records from two independent data sources (SourceAlpha and SourceBeta) into a unified golden database (UnifiedEntityStore). It ships with synthetic sports data, normalization helpers, fuzzy matchers, merge logic, lineage, and a small FastAPI for lookups.

## Architecture
- **SourceAlpha** and **SourceBeta**: standalone Postgres databases with slightly different schemas for teams, players, competitions, seasons, and matches.
- **UnifiedEntityStore (UES)**: canonical database that stores unified entities plus lineage back to both sources.
- **Pipeline**: loaders → normalizers → matchers → mergers → UES writer.
- **API**: FastAPI exposing health, mapping trigger, and player lookup/lineage endpoints.

## Quickstart
1. Copy environment template:
   ```bash
   cp .env.example .env
   ```
2. Start Postgres containers:
   ```bash
   make up
   ```
3. Seed SourceAlpha and SourceBeta with synthetic data:
   ```bash
   make seed
   ```
4. Run the mapping pipeline to populate UES:
   ```bash
   make map
   ```
5. Serve the API (after mapping):
   ```bash
   make api
   ```

### One-command local run
If you just want everything brought up in one shot, run:
```bash
make dev
```
This target ensures `.env` exists (copying from `.env.example` if needed), starts the databases, seeds them, runs the mapper, and finally launches the FastAPI server. Use `Ctrl+C` to stop the API and `make clean` to tear everything down.

## Run Locally (step-by-step)
### Prerequisites
- Docker with Compose v2+ (ships with recent Docker Desktop installs)
- Python 3.11+ plus `pip`
- `make` (preinstalled on macOS/Linux; install via Xcode CLT, build-essentials, etc.)

### Detailed workflow
1. **Create a virtual environment (optional but recommended)**  
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. **Install Python dependencies**  
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. **Prepare environment variables**  
   ```bash
   cp .env.example .env  # safe to rerun; overwrites nothing if .env already exists
   ```
   Update `.env` if you need to override default ports or API host values.
4. **Start the three Postgres services**  
   ```bash
   make up
   ```
   This launches the SourceAlpha, SourceBeta, and UnifiedEntityStore databases on ports 5433–5435. View logs anytime with `docker-compose logs -f`.
5. **Seed the source databases with synthetic data**  
   ```bash
   make seed
   ```
6. **Run the entity-resolution pipeline**  
   ```bash
   make map
   ```
   The mapper reads both sources, performs normalization/matching, and writes to UES.
7. **Bring up the FastAPI service**  
   ```bash
   make api
   ```
   The server listens on `http://localhost:8000` by default (`FASTAPI_HOST/FASTAPI_PORT` come from `.env`). Keep this terminal open; hit `Ctrl+C` to stop.
8. **Exercise the API** (in a different terminal)
   ```bash
   curl http://localhost:8000/health
   curl -X POST http://localhost:8000/mapping/run
   curl http://localhost:8000/lookup/player/by-alpha/1
   ```
9. **Tear down when finished**
   ```bash
   make clean
   ```
   This stops and removes the Postgres containers/volumes; rerun the steps above to start fresh.

## Inspecting data volumes
Once the containers are running, you can count rows directly in each database via `psql` inside the respective container.

1. **SourceAlpha tables**
   ```bash
   docker-compose exec source_alpha_db psql -U postgres -d source_alpha_db \
     -c "SELECT COUNT(*) AS players FROM players;"
   docker-compose exec source_alpha_db psql -U postgres -d source_alpha_db \
     -c "SELECT COUNT(*) AS matches FROM matches;"
   ```
2. **SourceBeta tables**
   ```bash
   docker-compose exec source_beta_db psql -U postgres -d source_beta_db \
     -c "SELECT COUNT(*) AS players FROM players;"
   docker-compose exec source_beta_db psql -U postgres -d source_beta_db \
     -c "SELECT COUNT(*) AS matches FROM matches;"
   ```
3. **Unified (master) tables**
   ```bash
   docker-compose exec ues_db psql -U postgres -d ues_db \
     -c "SELECT COUNT(*) AS unified_players FROM ues_players;"
   docker-compose exec ues_db psql -U postgres -d ues_db \
     -c "SELECT COUNT(*) AS unified_lineage FROM source_lineage;"
   ```

Swap in any other table name from the schemas (`entity_resolution_engine/db/*.sql`) if you need different counts. You can also open an interactive session (`docker-compose exec <service> psql -U postgres -d <db>`) and run `\dt` to list every table.

## Synthetic data volume
- `make seed` now loads thousands of rows so you can stress-test the matcher/merger (≈40 Alpha teams, 50 Beta teams, 2k–2.3k players, 800–900 matches, ~20k lineup rows).
- Data builders share vocab in `entity_resolution_engine/synthetic/base_entities.py` so Alpha/Beta overlap is partial—names are mutated, some entities exist in only one source, and schema quirks stay intact.
- Adjust the dataset size by tweaking the constants at the top of `entity_resolution_engine/synthetic/generate_alpha_data.py` and `entity_resolution_engine/synthetic/generate_beta_data.py` (team counts, player counts, matches, etc.). Rerun `make seed` (and `make map`) after any change.
- Each generator deletes the target tables before reloading, so multiple runs keep the database consistent.

## Docker Compose
`docker-compose.yml` spins up three Postgres 16 services:
- `source_alpha_db` on port 5433
- `source_beta_db` on port 5434
- `ues_db` on port 5435

## Data Model Highlights
- Synthetic data includes noisy variations in player names, competition naming, seasons, and match dates.
- Normalizers standardize names, countries, seasons, and competitions.
- Matchers use RapidFuzz and heuristic scores to connect Alpha/Beta entities with confidence thresholds.
- UES IDs are deterministic (`UES*` + hash) and lineage tracks all source IDs.

## API Examples
After running `make api` (default `http://localhost:8000` unless you override `FASTAPI_PORT` in `.env`):
- Health: `curl http://localhost:8000/health`
- Trigger mapping (returns a `run_id`): `curl -X POST http://localhost:8000/mapping/run`
- Get player by UES ID: `curl http://localhost:8000/ues/player/UESP-<hash>`
- Lookup by SourceAlpha ID: `curl http://localhost:8000/lookup/player/by-alpha/1`
- Lookup by SourceBeta ID: `curl http://localhost:8000/lookup/player/by-beta/10`
- Fetch lineage: `curl http://localhost:8000/ues/player/UESP-<hash>/lineage`

## LLM validation (gray-zone only)
LLM validation is **opt-in** and only invoked for gray-zone or conflicting matches. It does **not** replace the RapidFuzz/heuristic matchers or the deterministic merge logic. The matcher still drives candidate generation and confidence; the LLM only adds a second opinion for ambiguous cases.

Configure it in `entity_resolution_engine/config/llm_validation.yml`:
- `enabled`: toggle validation on/off.
- `gray_zone`: per-entity low/high thresholds.
- `max_calls_per_entity_type_per_run`: cap LLM calls per entity type.
- `circuit_breaker`: rolling window + max failure/invalid JSON rates.
- `fallback_mode_when_llm_unhealthy`: `auto_approve` (default) or `review`.

Environment variables:
- `INTERNAL_API_KEY` (protects internal endpoints)
- `LLM_PROVIDER`, `LLM_MODEL`, `LLM_API_KEY`, `LLM_API_URL`

Safety guardrails:
- Temperature is pinned to `0`.
- Strict JSON response schema with one retry on invalid JSON.
- Circuit breaker + call caps ensure predictable behavior.
- If the LLM is unavailable, gray-zone matches follow the configured fallback (auto-approve by default).

## Review queue workflow
Review items are stored in `llm_match_reviews` and exposed via internal endpoints:
- `GET /validation/reviews` (filter by `status`, `entity_type`, `run_id`, etc.)
- `POST /validation/reviews/{id}/approve`
- `POST /validation/reviews/{id}/reject`

Approving/rejecting only updates the review queue status; it **does not** retroactively merge or update UES entities. To apply reviewed outcomes, re-run the pipeline with the desired behavior (e.g., by resolving conflicts upstream or exporting decisions).

Example calls (replace `$INTERNAL_API_KEY` and `$RUN_ID`):
```bash
curl -H "X-Internal-API-Key: $INTERNAL_API_KEY" \
  "http://localhost:8000/validation/reviews?status=PENDING&run_id=$RUN_ID"

curl -H "X-Internal-API-Key: $INTERNAL_API_KEY" \
  -X POST "http://localhost:8000/validation/reviews/123/approve"
```

## Monitoring + anomaly detection
Each pipeline run emits metrics and anomalies for QA:
- Stored tables: `pipeline_run_metrics`, `llm_match_reviews`, `anomaly_events`, `anomaly_triage_reports`.
- Anomaly detection uses z-scores across historical runs (lookback window) to flag rate drift.
- Triage reports summarize likely causes and suggested actions.

Internal endpoints (protected by `X-Internal-API-Key`):
- `GET /monitoring/anomalies?run_id=...`
- `POST /monitoring/triage` (generates a triage report)
- `GET /monitoring/report?run_id=...` (full quality snapshot)
- `GET /monitoring/summary?run_id=...` (aggregated metrics + review queue counts)
- `GET /monitoring/gates?run_id=...` (quality gate result)

Each mapping run returns a `run_id` (from `/mapping/run` or the CLI) that ties together metrics, review items, anomalies, and gate results.

## Quality gates
Quality gates turn run metrics into a PASS/FAIL decision to reduce manual QA overhead. Configure them in `entity_resolution_engine/config/quality_gates.yml`:
- `max_llm_review_rate`
- `max_gray_zone_rate`
- `max_llm_error_rate`
- `fail_on_high_severity_anomalies`

Gate results are stored in `quality_gate_results` and exposed via `/monitoring/gates`.

## How to run locally
Use Docker Compose for the databases and a local Python environment for the API/pipeline.
- Databases run on ports 5433–5435 via `docker-compose.yml`.
- The FastAPI server defaults to `http://localhost:8000` (override via `.env`).

Quickstart:
```bash
cp .env.example .env
make up
make seed
make map
make api
```

## Testing strategy
Run unit + contract tests locally with:
```bash
pytest
```

CI scripts in `scripts/ci` cover formatting, linting, type checks, OpenAPI contracts, and performance benchmarks. Use `make ci` to run the full suite.

## CI Quality Gates (local)
The CI pipeline runs the same gates locally via scripts in `scripts/ci`. To execute the full set:
```bash
make ci
```

Individual gates can be run as needed:
```bash
bash scripts/ci/format_check.sh
bash scripts/ci/lint.sh
bash scripts/ci/type_check.sh
bash scripts/ci/build.sh
bash scripts/ci/openapi_contract.sh
bash scripts/ci/contract_tests.sh
bash scripts/ci/unit_tests.sh
bash scripts/ci/coverage_gate.sh
bash scripts/ci/security.sh
bash scripts/ci/performance_tests.sh
```
