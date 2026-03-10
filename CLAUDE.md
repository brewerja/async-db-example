# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync

# Run the OpenSearch standalone example
uv run python opensearch_example.py

# Start the FastAPI server (with auto-reload)
uv run uvicorn api:app --port 8000 --reload

# Type check
uv run ty check <file>.py
```

## Infrastructure

Both a Postgres and OpenSearch instance are required. Start them with Docker:

```bash
docker run -d --name async-db-postgres \
  -e POSTGRES_USER=demo -e POSTGRES_PASSWORD=demo -e POSTGRES_DB=demodb \
  -p 5432:5432 postgres:latest

docker run -d --name async-opensearch \
  -e "discovery.type=single-node" -e "DISABLE_SECURITY_PLUGIN=true" \
  -p 9200:9200 opensearchproject/opensearch:2
```

Connection strings are hardcoded in each file (`DB_URL`, `OS_HOST`).

## Architecture

Two entry points, sharing the same Postgres schema (`team` and `hero` tables):

- **`opensearch_example.py`** — standalone async script demonstrating the full opensearch-py async client (bulk indexing, get, term/range queries, aggregations, update, count). Uses a `movies` index unrelated to the API.
- **`api.py`** — FastAPI app combining both: Postgres is the source of truth, OpenSearch is kept in sync on every hero write.

### Key patterns in `api.py`

- **Lifespan**: `create_all` (Postgres) and index creation (OpenSearch) run on startup; connections are disposed on shutdown
- **Session dependency**: `get_session()` yields one `AsyncSession` per request via `async_sessionmaker`
- **OS sync**: every `POST`/`PATCH`/`DELETE /heroes` call writes to Postgres first, then calls `_os_index()`/`_os_delete()`
- **Search**: `GET /search?q=` runs a `multi_match` over `name` and `secret_name` fields in OpenSearch — heroes only appear in search results if they were created through the API

### Typing notes

- `async_sessionmaker[AsyncSession]` (not `sessionmaker`) is required for the async engine — `sessionmaker` triggers a type error with `AsyncEngine`
- OpenSearch query parameters like `refresh` must be passed via `params={"refresh": "wait_for"}`, not as direct kwargs — the method signatures only expose `body`, `index`, `params`, and `headers`
- `col()` from sqlmodel is needed in join conditions (e.g. `col(Hero.team_id) == Team.id`) so type checkers treat `==` as a SQL clause rather than a Python `bool`
- `.scalars().all()` returns `Sequence[T]`, not `list[T]`
