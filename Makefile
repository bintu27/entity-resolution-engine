-include .env

.PHONY: up seed map api clean bootstrap dev ci .env

.env:
	@if [ ! -f .env ]; then cp .env.example .env; fi

up:
	docker-compose up -d

seed:
	python -m entity_resolution_engine.synthetic.generate_alpha_data
	python -m entity_resolution_engine.synthetic.generate_beta_data

map:
	python -m entity_resolution_engine.cli.run_mapping

api:
	uvicorn entity_resolution_engine.api.main:app --host $(FASTAPI_HOST) --port $(FASTAPI_PORT)

clean:
	docker-compose down -v

bootstrap: .env
	$(MAKE) up
	$(MAKE) seed
	$(MAKE) map

dev: bootstrap
	$(MAKE) api

ci:
	bash scripts/ci/install_deps.sh
	bash scripts/ci/format_check.sh
	bash scripts/ci/lint.sh
	bash scripts/ci/type_check.sh
	bash scripts/ci/build.sh
	bash scripts/ci/openapi_contract.sh
	bash scripts/ci/contract_tests.sh
	bash scripts/ci/llm_split_gate.sh
	bash scripts/ci/unit_tests.sh
	bash scripts/ci/coverage_gate.sh
	bash scripts/ci/security.sh
	bash scripts/ci/performance_tests.sh
