# Chronos

A batch lakehouse that replays real Olist marketplace orders as a late, messy event feed, builds a
star schema from it, and publishes only audited releases. The full design is in
[docs/spec.md](docs/spec.md); progress notes per phase are in `docs/phase-N-notes.md`.

Status: Phase 0 (foundations). A complete README comes in Phase 5.

## Data

The data is the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce),
published on Kaggle under CC BY-NC-SA 4.0 (non-commercial, share-alike). Credit: Olist. The CSV
files go in `data/raw/olist/` and are never committed.

### Differences between the Kaggle download and the spec's first numbers

The spec was first measured on a GitHub mirror. Rerunning both profiling scripts on the Kaggle
download (22 Nov 2026) matched every count except the reviews file: Kaggle has 99,224 rows, the
mirror 100,000. That moves the review-based numbers slightly (for example 547 orders with several
reviews instead of 555, and late orders average 2.27 stars instead of 2.26). The spec now carries
Kaggle's numbers; the full list is in [docs/decisions.md](docs/decisions.md).

## Quick start (local)

Needs Python 3.11, Java 17 and Docker.

```bash
python3.11 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check . && pytest          # includes the Iceberg smoke test
docker compose up -d            # Chronos PostgreSQL on :5433, Airflow on http://localhost:8080
```

See [docs/phase-0-notes.md](docs/phase-0-notes.md) for what each piece is and how to check it.
