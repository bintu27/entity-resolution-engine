include .env

.PHONY: up seed map api clean

up:
docker-compose up -d

seed:
python -m entity_resolution_engine.synthetic.generate_alpha_data
python -m entity_resolution_engine.synthetic.generate_beta_data

map:
python -m entity_resolution_engine.cli.run_mapping

api:
uvicorn entity_resolution_engine.api.main:app --host $$FASTAPI_HOST --port $$FASTAPI_PORT

clean:
docker-compose down -v
