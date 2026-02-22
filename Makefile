.PHONY: docs

ifdef UV
    PIP := uv pip
else
    PIP := pip3
endif

install-dev:
	$(PIP) install -e ".[dev,slack,dlt,lsp]" ./examples/custom_materializations

install-doc:
	$(PIP) install -r ./docs/requirements.txt

install-pre-commit:
	prek install

style:
	prek run --all-files

doc-test:
	python -m pytest --doctest-modules sqlmesh/core sqlmesh/utils

package:
	$(PIP) install build && python3 -m build

publish: package
	$(PIP) install twine && python3 -m twine upload dist/*

package-tests:
	$(PIP) install build && cp pyproject.toml tests/sqlmesh_pyproject.toml && python3 -m build tests/

publish-tests: package-tests
	$(PIP) install twine && python3 -m twine upload -r tobiko-private tests/dist/*

docs-serve:
	mkdocs serve

api-docs:
	python pdoc/cli.py -o docs/_readthedocs/html/

api-docs-serve:
	python pdoc/cli.py

clean-build:
	rm -rf build/ && rm -rf dist/ && rm -rf *.egg-info

clear-caches:
	find . -type d -name ".cache" -exec rm -rf {} + 2>/dev/null && echo "Successfully removed all .cache directories"

dev-publish: clean-build publish

jupyter-example:
	jupyter lab tests/slows/jupyter/example_outputs.ipynb

engine-up: engine-clickhouse-up engine-mssql-up engine-mysql-up engine-postgres-up engine-spark-up engine-trino-up

engine-down: engine-clickhouse-down engine-mssql-down engine-mysql-down engine-postgres-down engine-spark-down engine-trino-down

fast-test:
	pytest -n auto -m "fast and not cicdonly" --junitxml=test-results/junit-fast-test.xml && pytest -m "isolated" && pytest -m "registry_isolation" && pytest -m "dialect_isolated"

slow-test:
	pytest -n auto -m "(fast or slow) and not cicdonly" && pytest -m "isolated" && pytest -m "registry_isolation" && pytest -m "dialect_isolated"

cicd-test:
	pytest -n auto -m "fast or slow" --junitxml=test-results/junit-cicd.xml && pytest -m "isolated" && pytest -m "registry_isolation" && pytest -m "dialect_isolated"

core-fast-test:
	pytest -n auto -m "fast and not web and not github and not jupyter"

core-slow-test:
	pytest -n auto -m "(fast or slow) and not web and not github and not jupyter"

engine-slow-test:
	pytest -n auto -m "(fast or slow) and engine"

engine-docker-test:
	pytest -n auto -m "docker and engine"

engine-remote-test:
	pytest -n auto -m "remote and engine"

engine-test:
	pytest -n auto -m "engine"

github-test:
	pytest -n auto -m "github"

jupyter-test:
	pytest -n auto -m "jupyter"

web-test:
	pytest -n auto -m "web"

guard-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable $* not set"; \
		exit 1; \
	fi

engine-%-install:
	$(PIP) install -e ".[dev,slack,lsp,${*}]" ./examples/custom_materializations

engine-docker-%-up:
	docker compose -f ./tests/core/engine_adapter/integration/docker/compose.${*}.yaml up -d
	./.circleci/wait-for-db.sh ${*}

engine-%-up: engine-%-install engine-docker-%-up
	@echo "Engine '${*}' is up and running"

engine-%-down:
	docker compose -f ./tests/core/engine_adapter/integration/docker/compose.${*}.yaml down -v

##################
# Docker Engines #
##################

clickhouse-test: engine-clickhouse-up
	pytest -n auto -m "clickhouse" --reruns 3 --junitxml=test-results/junit-clickhouse.xml

duckdb-test: engine-duckdb-install
	pytest -n auto -m "duckdb" --reruns 3 --junitxml=test-results/junit-duckdb.xml

mssql-test: engine-mssql-up
	pytest -n auto -m "mssql" --reruns 3 --junitxml=test-results/junit-mssql.xml

mysql-test: engine-mysql-up
	pytest -n auto -m "mysql" --reruns 3 --junitxml=test-results/junit-mysql.xml

postgres-test: engine-postgres-up
	pytest -n auto -m "postgres" --reruns 3 --junitxml=test-results/junit-postgres.xml

spark-test: engine-spark-up
	pytest -n auto -m "spark" --reruns 3 --junitxml=test-results/junit-spark.xml && pytest -n auto -m "pyspark" --reruns 3 --junitxml=test-results/junit-pyspark.xml

trino-test: engine-trino-up
	pytest -n auto -m "trino" --reruns 3 --junitxml=test-results/junit-trino.xml

risingwave-test: engine-risingwave-up
	pytest -n auto -m "risingwave" --reruns 3 --junitxml=test-results/junit-risingwave.xml

#################
# Cloud Engines #
#################

snowflake-test: guard-SNOWFLAKE_ACCOUNT guard-SNOWFLAKE_WAREHOUSE guard-SNOWFLAKE_DATABASE guard-SNOWFLAKE_USER engine-snowflake-install
	pytest -n auto -m "snowflake" --reruns 3 --junitxml=test-results/junit-snowflake.xml

bigquery-test: guard-BIGQUERY_KEYFILE engine-bigquery-install
	$(PIP) install -e ".[bigframes]"
	pytest -n auto -m "bigquery" --reruns 3 --junitxml=test-results/junit-bigquery.xml

databricks-test: guard-DATABRICKS_CATALOG guard-DATABRICKS_SERVER_HOSTNAME guard-DATABRICKS_HTTP_PATH guard-DATABRICKS_CONNECT_VERSION engine-databricks-install
	$(PIP) install 'databricks-connect==${DATABRICKS_CONNECT_VERSION}'
	pytest -n auto -m "databricks" --reruns 3 --junitxml=test-results/junit-databricks.xml

redshift-test: guard-REDSHIFT_HOST guard-REDSHIFT_USER guard-REDSHIFT_PASSWORD guard-REDSHIFT_DATABASE engine-redshift-install
	pytest -n auto -m "redshift" --reruns 3 --junitxml=test-results/junit-redshift.xml

clickhouse-cloud-test: guard-CLICKHOUSE_CLOUD_HOST guard-CLICKHOUSE_CLOUD_USERNAME guard-CLICKHOUSE_CLOUD_PASSWORD engine-clickhouse-install
	pytest -n 1 -m "clickhouse_cloud" --reruns 3 --junitxml=test-results/junit-clickhouse-cloud.xml

athena-test: guard-AWS_ACCESS_KEY_ID guard-AWS_SECRET_ACCESS_KEY guard-ATHENA_S3_WAREHOUSE_LOCATION engine-athena-install
	pytest -n auto -m "athena" --reruns 3 --junitxml=test-results/junit-athena.xml

fabric-test: guard-FABRIC_HOST guard-FABRIC_CLIENT_ID guard-FABRIC_CLIENT_SECRET guard-FABRIC_DATABASE engine-fabric-install
	pytest -n auto -m "fabric" --reruns 3 --junitxml=test-results/junit-fabric.xml

gcp-postgres-test: guard-GCP_POSTGRES_INSTANCE_CONNECTION_STRING guard-GCP_POSTGRES_USER guard-GCP_POSTGRES_PASSWORD guard-GCP_POSTGRES_KEYFILE_JSON engine-gcppostgres-install
	pytest -n auto -m "gcp_postgres" --reruns 3 --junitxml=test-results/junit-gcp-postgres.xml

benchmark-ci:
	python benchmarks/lsp_render_model_bench.py --debug-single-value
