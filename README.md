# async-db-example

A working example of async Python with PostgreSQL and OpenSearch, wired together in a FastAPI app.

**Stack:** Python 3.13 · FastAPI · psycopg v3 · SQLModel · SQLAlchemy 2 (async) · opensearch-py

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- Docker

## Quickstart

```bash
# Start dependencies
docker run -d --name async-db-postgres \
  -e POSTGRES_USER=demo -e POSTGRES_PASSWORD=demo -e POSTGRES_DB=demodb \
  -p 5432:5432 postgres:latest

docker run -d --name async-opensearch \
  -e "discovery.type=single-node" -e "DISABLE_SECURITY_PLUGIN=true" \
  -p 9200:9200 opensearchproject/opensearch:2

# Install and run
uv sync
uv run uvicorn api:app --port 8000 --reload
```

API docs: http://localhost:8000/docs

## Endpoints

| Method | Path | Description |
|---|---|---|
| POST | `/teams` | Create a team |
| GET | `/teams` | List teams |
| POST | `/heroes` | Create a hero |
| POST | `/heroes/bulk` | Bulk-create heroes |
| GET | `/heroes` | List heroes |
| GET | `/heroes/count` | Count indexed heroes (OpenSearch) |
| GET | `/heroes/{id}` | Get a hero |
| PATCH | `/heroes/{id}` | Update a hero |
| DELETE | `/heroes/{id}` | Delete a hero |
| GET | `/search` | Full-text search with optional `q`, `team_id`, `min_age`, `max_age` |
| GET | `/stats` | Avg age per team (OpenSearch aggregation) |

## Tests

```bash
uv run pytest tests/ -v
```

No running infrastructure required — the test suite mocks all external dependencies.
