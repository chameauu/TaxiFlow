SHELL := /bin/bash
COMPOSE := docker compose -f compose.yaml
SPARK_PACKAGES := io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262
SPARK_SUBMIT := /opt/spark/bin/spark-submit --master "local[4]" --driver-memory 4g --conf spark.driver.maxResultSize=1g --conf spark.sql.shuffle.partitions=16 --packages "$(SPARK_PACKAGES)" --conf spark.jars.ivy=/tmp/.ivy2
RUNTIME_PROFILES := --profile core --profile stream --profile orchestration --profile bi
ALL_PROFILES := $(RUNTIME_PROFILES) --profile batch
PROFILES := $(ALL_PROFILES)

.PHONY: help env up up-all down reset logs ps images pull sync lock seed batch batch-report test test-contract test-integration lint format compose-check verify

help:
	@printf '%s\n' \
		'make env           Create a local .env with generated credentials' \
		'make up            Start core and stream profiles with healthchecks' \
		'make up-all        Start core, stream, orchestration, and bi' \
		'make down          Stop services without deleting volumes' \
		'make reset         Stop services and delete named volumes' \
		'make logs          Tail logs for all profiles' \
		'make ps            Show service health for all profiles' \
		'make images        List every image, including the batch profile' \
		'make pull          Pull every image, including Spark' \
		'make sync          Install the locked environment' \
		'make seed          Download and verify TLC source files' \
		'make batch         Run one TLC month into Delta bronze' \
		'make batch-report  Read back bronze counts and sums' \
		'make lock          Regenerate uv.lock' \
		'make test          Run unit tests' \
		'make test-contract Run Compose and config contract tests' \
		'make test-integration Run tests against the running stack' \
		'make lint          Run ruff' \
		'make format        Format with ruff' \
		'make compose-check Validate compose.yaml' \
		'make verify        Run lint, unit tests, contract tests, and Compose validation'

env:
	@if [ -f .env ]; then \
		echo ".env already exists"; \
	else \
		umask 077; \
		MINIO_PASSWORD=$$(python3 -c 'import secrets; print(secrets.token_hex(16))'); \
		POSTGRES_PASSWORD=$$(python3 -c 'import secrets; print(secrets.token_hex(16))'); \
		AIRFLOW_PASSWORD=$$(python3 -c 'import secrets; print(secrets.token_hex(16))'); \
		SUPERSET_PASSWORD=$$(python3 -c 'import secrets; print(secrets.token_hex(16))'); \
		AIRFLOW_FERNET_KEY=$$(python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())'); \
		AIRFLOW_SECRET=$$(python3 -c 'import secrets; print(secrets.token_hex(32))'); \
		SUPERSET_SECRET=$$(python3 -c 'import secrets; print(secrets.token_hex(32))'); \
		sed \
			-e "s|^MINIO_ROOT_PASSWORD=.*|MINIO_ROOT_PASSWORD=$$MINIO_PASSWORD|" \
			-e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$$POSTGRES_PASSWORD|" \
			-e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$$AIRFLOW_FERNET_KEY|" \
			-e "s|^AIRFLOW_WEBSERVER_SECRET_KEY=.*|AIRFLOW_WEBSERVER_SECRET_KEY=$$AIRFLOW_SECRET|" \
			-e "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=$$AIRFLOW_PASSWORD|" \
			-e "s|^SUPERSET_SECRET_KEY=.*|SUPERSET_SECRET_KEY=$$SUPERSET_SECRET|" \
			-e "s|^SUPERSET_ADMIN_PASSWORD=.*|SUPERSET_ADMIN_PASSWORD=$$SUPERSET_PASSWORD|" \
			.env.example > .env; \
		echo "created .env with generated local credentials"; \
	fi

up: env
	$(COMPOSE) --profile core --profile stream up -d --wait --wait-timeout 420 minio postgres
	$(COMPOSE) --profile core --profile stream up -d minio-init
	$(COMPOSE) --profile core --profile stream wait minio-init
	$(COMPOSE) --profile core --profile stream up -d --wait --wait-timeout 420 --no-deps zookeeper kafka
	$(COMPOSE) --profile core --profile stream up -d --wait --wait-timeout 420 --no-deps hive-metastore schema-registry
	$(COMPOSE) --profile core --profile stream up -d --wait --wait-timeout 420 --no-deps trino debezium

up-all: env
	$(COMPOSE) $(RUNTIME_PROFILES) up -d --wait --wait-timeout 600 minio postgres
	$(COMPOSE) $(RUNTIME_PROFILES) up -d minio-init airflow-init superset-init
	$(COMPOSE) $(RUNTIME_PROFILES) wait minio-init airflow-init superset-init
	$(COMPOSE) $(RUNTIME_PROFILES) up -d --wait --wait-timeout 600 --no-deps zookeeper kafka
	$(COMPOSE) $(RUNTIME_PROFILES) up -d --wait --wait-timeout 600 --no-deps hive-metastore schema-registry
	$(COMPOSE) $(RUNTIME_PROFILES) up -d --wait --wait-timeout 600 --no-deps trino debezium airflow superset

down:
	$(COMPOSE) $(PROFILES) down

reset:
	$(COMPOSE) $(PROFILES) down -v --remove-orphans

logs:
	$(COMPOSE) $(PROFILES) logs -f --tail=100

ps:
	$(COMPOSE) $(PROFILES) ps

images: env
	$(COMPOSE) $(ALL_PROFILES) config --images | sort -u

pull: env
	$(COMPOSE) $(ALL_PROFILES) pull --ignore-buildable

sync:
	uv sync --frozen

lock:
	uv lock

seed: env
	uv run taxiflow tlc download --manifest config/tlc_manifest.json

batch: env
	$(COMPOSE) --profile core up -d --wait minio
	$(COMPOSE) --profile core --profile batch run --rm --no-deps spark $(SPARK_SUBMIT) /opt/taxiflow/src/taxiflow/jobs/batch_bronze.py --root /opt/taxiflow --manifest /opt/taxiflow/config/tlc_manifest.json --service yellow --year 2024 --month 1 --write-partitions 8

batch-report: env
	$(COMPOSE) --profile core --profile batch run --rm --no-deps spark $(SPARK_SUBMIT) /opt/taxiflow/src/taxiflow/jobs/report_bronze.py

test:
	uv run pytest -q -m "not integration and not contract"

test-contract:
	uv run pytest -q -m contract

test-integration:
	TAXIFLOW_REQUIRE_STACK=1 uv run pytest -q -m integration

lint:
	uv run ruff check .

format:
	uv run ruff format .

compose-check: env
	$(COMPOSE) config -q

verify: lint test test-contract compose-check
