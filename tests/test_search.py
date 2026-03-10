"""
Tests for GET /search and GET /stats — both are OpenSearch-only (no DB session).
"""


def _os_hit(id: str, name: str, secret_name: str, age: int, team_id: int, score: float = 1.0):
    return {
        "_id": id,
        "_score": score,
        "_source": {"name": name, "secret_name": secret_name, "age": age, "team_id": team_id},
    }


async def test_search_text(client, mock_os):
    mock_os.search.return_value = {
        "hits": {"hits": [_os_hit("1", "Spider-Man", "Peter Parker", 24, 1, score=1.5)]}
    }

    resp = await client.get("/search?q=peter")

    assert resp.status_code == 200
    results = resp.json()
    assert len(results) == 1
    assert results[0]["name"] == "Spider-Man"
    assert results[0]["score"] == 1.5

    body = mock_os.search.call_args.kwargs["body"]
    must = body["query"]["bool"]["must"]
    assert must[0]["multi_match"]["query"] == "peter"


async def test_search_no_query_returns_all(client, mock_os):
    mock_os.search.return_value = {"hits": {"hits": []}}

    resp = await client.get("/search")

    assert resp.status_code == 200
    body = mock_os.search.call_args.kwargs["body"]
    assert body["query"]["bool"]["must"] == [{"match_all": {}}]


async def test_search_team_filter(client, mock_os):
    mock_os.search.return_value = {"hits": {"hits": []}}

    await client.get("/search?team_id=1")

    body = mock_os.search.call_args.kwargs["body"]
    filters = body["query"]["bool"]["filter"]
    assert {"term": {"team_id": 1}} in filters


async def test_search_age_range(client, mock_os):
    mock_os.search.return_value = {"hits": {"hits": []}}

    await client.get("/search?min_age=20&max_age=30")

    body = mock_os.search.call_args.kwargs["body"]
    filters = body["query"]["bool"]["filter"]
    assert {"range": {"age": {"gte": 20, "lte": 30}}} in filters


async def test_search_min_age_only(client, mock_os):
    mock_os.search.return_value = {"hits": {"hits": []}}

    await client.get("/search?min_age=18")

    body = mock_os.search.call_args.kwargs["body"]
    filters = body["query"]["bool"]["filter"]
    assert {"range": {"age": {"gte": 18}}} in filters


async def test_search_combined_filters(client, mock_os):
    mock_os.search.return_value = {"hits": {"hits": []}}

    await client.get("/search?q=parker&team_id=1&min_age=20&max_age=30")

    body = mock_os.search.call_args.kwargs["body"]
    bool_query = body["query"]["bool"]
    assert bool_query["must"][0]["multi_match"]["query"] == "parker"
    assert {"term": {"team_id": 1}} in bool_query["filter"]
    assert {"range": {"age": {"gte": 20, "lte": 30}}} in bool_query["filter"]


async def test_stats(client, mock_os):
    mock_os.search.return_value = {
        "aggregations": {
            "by_team": {
                "buckets": [
                    {"key": 1, "doc_count": 2, "avg_age": {"value": 36.0}},
                    {"key": 3, "doc_count": 1, "avg_age": {"value": 200.0}},
                ]
            }
        }
    }

    resp = await client.get("/stats")

    assert resp.status_code == 200
    stats = resp.json()
    assert len(stats) == 2
    assert stats[0] == {"team_id": 1, "hero_count": 2, "avg_age": 36.0}
    assert stats[1] == {"team_id": 3, "hero_count": 1, "avg_age": 200.0}

    body = mock_os.search.call_args.kwargs["body"]
    assert "aggs" in body
    assert body["size"] == 0
