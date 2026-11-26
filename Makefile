# Local commands (README, "Run it"). CI runs ruff and pytest directly (.github/workflows/ci.yml).

# Spark needs Java 17; Java 23 is also installed on the owner's laptop and must not be used.
JAVA17 := $(shell /usr/libexec/java_home -v 17 2>/dev/null)
PY := .venv/bin/python
DEMO_DB := chronos_demo
DAYS ?= 30

.PHONY: setup db demo dashboard console test truth airflow

# A virtual environment with the pipeline, the test tools and the Streamlit pages.
setup:
	python3.11 -m venv .venv
	.venv/bin/pip install -q -e ".[dev,dashboard]"

# Only the Chronos PostgreSQL (port 5433). Airflow is optional: see `make airflow`.
db:
	docker compose up -d chronos-db

# Thirty business dates through the whole pipeline, with the spec's defects and one stray file
# that DataSentinel blocks (jobs/demo.py). About 10 minutes; starts from scratch each time.
# `make demo DAYS=90` reaches March 2017, where Olist's late deliveries begin.
demo:
	JAVA_HOME=$(JAVA17) $(PY) -m jobs.demo --fresh --days $(DAYS)

# The dashboards read PostgreSQL only.
dashboard:
	CHRONOS_DB_NAME=$(DEMO_DB) .venv/bin/streamlit run dashboard/app.py

console:
	CHRONOS_DB_NAME=$(DEMO_DB) .venv/bin/streamlit run sentinel/console/app.py --server.port 8502

# Lint and every test. CI runs the same, without the Olist files (146 tests, about 40 min);
# with data/raw/olist present the real-slice truth test runs too (160, about 55 min).
test:
	.venv/bin/ruff check .
	JAVA_HOME=$(JAVA17) CHRONOS_REQUIRE_POSTGRES=1 .venv/bin/pytest -q

# The truth test on the real Jan-Feb 2017 slice, with the spec's defects on (about 30 minutes).
# Needs data/raw/olist and the chronos-db container; missing either fails instead of skipping.
# Record each pass in the current phase's notes ("Truth test, last passed").
truth:
	JAVA_HOME=$(JAVA17) CHRONOS_REQUIRE_OLIST=1 CHRONOS_REQUIRE_POSTGRES=1 \
		.venv/bin/pytest tests/truth -k olist -q -p no:cacheprovider

# Airflow 3.4 on http://localhost:8080 (user and password: airflow), with the Chronos DAGs.
airflow:
	docker compose up -d --build
