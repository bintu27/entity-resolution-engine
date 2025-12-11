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
After running `make api` (default `http://localhost:8080`):
- Health: `curl http://localhost:8080/health`
- Trigger mapping: `curl -X POST http://localhost:8080/mapping/run`
- Get player by UES ID: `curl http://localhost:8080/ues/player/UESP-<hash>`
- Lookup by SourceAlpha ID: `curl http://localhost:8080/lookup/player/by-alpha/1`
- Lookup by SourceBeta ID: `curl http://localhost:8080/lookup/player/by-beta/10`
- Fetch lineage: `curl http://localhost:8080/ues/player/UESP-<hash>/lineage`

## Tests
Basic unit tests cover season normalization, name similarity, deterministic UES IDs, and a positive player match scenario:
```bash
pytest
```
