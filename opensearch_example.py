"""
Async opensearch-py example.

Stack:
  - opensearch-py 3.x (AsyncOpenSearch client)
  - aiohttp transport (installed via opensearch-py[async])
"""

import asyncio
from typing import Any

from opensearchpy import AsyncOpenSearch

INDEX = "movies"

client: AsyncOpenSearch = AsyncOpenSearch(
    hosts=[{"host": "localhost", "port": 9200}],
    use_ssl=False,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def recreate_index() -> None:
    if await client.indices.exists(index=INDEX):
        await client.indices.delete(index=INDEX)

    await client.indices.create(
        index=INDEX,
        body={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {
                "properties": {
                    "title":    {"type": "text"},
                    "director": {"type": "keyword"},
                    "year":     {"type": "integer"},
                    "rating":   {"type": "float"},
                }
            },
        },
    )
    print(f"  Index '{INDEX}' created.")


async def bulk_index(docs: list[dict[str, Any]]) -> None:
    """Index a list of dicts using the bulk API."""
    body: list[dict[str, Any]] = []
    for doc in docs:
        body.append({"index": {"_index": INDEX, "_id": doc["id"]}})
        body.append(doc)

    resp = await client.bulk(body=body, params={"refresh": "wait_for"})
    errors = [item for item in resp["items"] if "error" in item.get("index", {})]
    if errors:
        raise RuntimeError(f"Bulk errors: {errors}")
    print(f"  Indexed {len(docs)} documents.")


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

async def main() -> None:
    print("\n=== Cluster info ===")
    info = await client.info()
    print(f"  OpenSearch {info['version']['number']} — cluster: {info['cluster_name']}")

    print("\n=== Creating index ===")
    await recreate_index()

    # --- Bulk index ---
    print("\n=== Bulk indexing ===")
    movies: list[dict[str, Any]] = [
        {"id": 1, "title": "The Godfather",          "director": "Francis Ford Coppola", "year": 1972, "rating": 9.2},
        {"id": 2, "title": "The Dark Knight",        "director": "Christopher Nolan",    "year": 2008, "rating": 9.0},
        {"id": 3, "title": "Pulp Fiction",           "director": "Quentin Tarantino",    "year": 1994, "rating": 8.9},
        {"id": 4, "title": "Interstellar",           "director": "Christopher Nolan",    "year": 2014, "rating": 8.7},
        {"id": 5, "title": "Inception",              "director": "Christopher Nolan",    "year": 2010, "rating": 8.8},
        {"id": 6, "title": "Goodfellas",             "director": "Martin Scorsese",      "year": 1990, "rating": 8.7},
        {"id": 7, "title": "The Shawshank Redemption","director": "Frank Darabont",      "year": 1994, "rating": 9.3},
    ]
    await bulk_index(movies)

    # --- Get by ID ---
    print("\n=== Get by ID ===")
    doc = await client.get(index=INDEX, id=1)
    src = doc["_source"]
    print(f"  [{doc['_id']}] {src['title']} ({src['year']}) — rating {src['rating']}")

    # --- Full-text search ---
    print("\n=== Full-text search: 'godfather' ===")
    resp = await client.search(
        index=INDEX,
        body={"query": {"match": {"title": "godfather"}}},
    )
    for hit in resp["hits"]["hits"]:
        s = hit["_source"]
        print(f"  score={hit['_score']:.3f}  {s['title']} ({s['year']})")

    # --- Term filter (keyword field) ---
    print("\n=== Filter: director = Christopher Nolan ===")
    resp = await client.search(
        index=INDEX,
        body={
            "query": {"term": {"director": "Christopher Nolan"}},
            "sort": [{"year": "asc"}],
        },
    )
    for hit in resp["hits"]["hits"]:
        s = hit["_source"]
        print(f"  {s['title']} ({s['year']}) — rating {s['rating']}")

    # --- Range + sort ---
    print("\n=== Range: rating >= 9.0, sorted by rating desc ===")
    resp = await client.search(
        index=INDEX,
        body={
            "query": {"range": {"rating": {"gte": 9.0}}},
            "sort": [{"rating": "desc"}],
        },
    )
    for hit in resp["hits"]["hits"]:
        s = hit["_source"]
        print(f"  {s['rating']}  {s['title']}")

    # --- Aggregation ---
    print("\n=== Aggregation: avg rating per director ===")
    resp = await client.search(
        index=INDEX,
        body={
            "size": 0,
            "aggs": {
                "by_director": {
                    "terms": {"field": "director"},
                    "aggs": {"avg_rating": {"avg": {"field": "rating"}}},
                }
            },
        },
    )
    for bucket in resp["aggregations"]["by_director"]["buckets"]:
        print(f"  {bucket['key']:30s} avg={bucket['avg_rating']['value']:.2f}  ({bucket['doc_count']} films)")

    # --- Update a document ---
    print("\n=== Update: bump Inception rating to 9.0 ===")
    await client.update(
        index=INDEX,
        id=5,
        body={"doc": {"rating": 9.0}},
        params={"refresh": "wait_for"},
    )
    updated = await client.get(index=INDEX, id=5)
    print(f"  New rating: {updated['_source']['rating']}")

    # --- Delete a document ---
    print("\n=== Delete: remove Pulp Fiction ===")
    await client.delete(index=INDEX, id=3, params={"refresh": "wait_for"})
    count = await client.count(index=INDEX)
    print(f"  Documents remaining: {count['count']}")

    await client.close()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
