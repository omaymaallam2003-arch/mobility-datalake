COMPOSE ?= docker compose

.PHONY: up run sql test down clean logs sample

up:            ## Build + démarre MinIO et Dagster (UI: http://localhost:3000)
	$(COMPOSE) up -d --build

run:           ## Lance le pipeline bronze -> silver -> gold
	$(COMPOSE) exec dagster dagster job execute -m pipeline.definitions -j medallion_pipeline

sql:           ## Interroge la couche Gold en SQL (DuckDB)
	$(COMPOSE) exec dagster python scripts/query_gold.py

test:          ## Tests unitaires (dans le conteneur)
	$(COMPOSE) exec dagster pytest -q

logs:
	$(COMPOSE) logs -f dagster

down:
	$(COMPOSE) down

clean:         ## Arrête tout et supprime les volumes (données incluses)
	$(COMPOSE) down -v

sample:        ## Télécharge localement un échantillon SODA (sans Docker)
	python scripts/download_sample.py --start 2023-01-01 --end 2023-01-03 --out data/raw
