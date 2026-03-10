import pytest
from unittest.mock import call

from api import Hero
from tests.conftest import scalars

SPIDER_MAN = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=24, team_id=1)
IRON_MAN   = Hero(id=2, name="Iron Man",   secret_name="Tony Stark",   age=48, team_id=1)


async def test_create_hero(client, mock_session, mock_os):
    mock_session.refresh.side_effect = lambda obj: setattr(obj, "id", 1)

    resp = await client.post(
        "/heroes",
        json={"name": "Spider-Man", "secret_name": "Peter Parker", "age": 24, "team_id": 1},
    )

    assert resp.status_code == 201
    assert resp.json()["name"] == "Spider-Man"
    mock_session.add.assert_called_once()
    mock_session.commit.assert_awaited_once()
    mock_os.index.assert_awaited_once()


async def test_bulk_create_heroes(client, mock_session, mock_os):
    # Simulate flush() setting IDs on the hero objects
    captured: list[Hero] = []

    def capture(heroes):
        captured.extend(heroes)

    async def set_ids():
        for i, h in enumerate(captured, 1):
            h.id = i

    mock_session.add_all.side_effect = capture
    mock_session.flush.side_effect = set_ids

    resp = await client.post(
        "/heroes/bulk",
        json=[
            {"name": "Spider-Man", "secret_name": "Peter Parker", "age": 24, "team_id": 1},
            {"name": "Iron Man",   "secret_name": "Tony Stark",   "age": 48, "team_id": 1},
        ],
    )

    assert resp.status_code == 201
    assert len(resp.json()) == 2
    mock_session.add_all.assert_called_once()
    mock_session.flush.assert_awaited_once()
    mock_session.commit.assert_awaited_once()
    mock_os.bulk.assert_awaited_once()


async def test_list_heroes(client, mock_session):
    mock_session.execute.return_value = scalars(SPIDER_MAN, IRON_MAN)

    resp = await client.get("/heroes")

    assert resp.status_code == 200
    assert len(resp.json()) == 2


async def test_get_hero(client, mock_session):
    mock_session.get.return_value = SPIDER_MAN

    resp = await client.get("/heroes/1")

    assert resp.status_code == 200
    assert resp.json()["name"] == "Spider-Man"
    mock_session.get.assert_awaited_once_with(Hero, 1)


async def test_get_hero_not_found(client, mock_session):
    mock_session.get.return_value = None

    resp = await client.get("/heroes/999")

    assert resp.status_code == 404


async def test_count_heroes(client, mock_os):
    mock_os.count.return_value = {"count": 42}

    resp = await client.get("/heroes/count")

    assert resp.status_code == 200
    assert resp.json() == {"count": 42}
    mock_os.count.assert_awaited_once()


async def test_update_hero(client, mock_session, mock_os):
    hero = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=24, team_id=1)
    mock_session.get.return_value = hero
    mock_session.refresh.side_effect = lambda obj: None

    resp = await client.patch("/heroes/1", json={"age": 25})

    assert resp.status_code == 200
    assert resp.json()["age"] == 25
    mock_session.commit.assert_awaited_once()
    mock_os.index.assert_awaited_once()


async def test_update_hero_not_found(client, mock_session):
    mock_session.get.return_value = None

    resp = await client.patch("/heroes/999", json={"age": 25})

    assert resp.status_code == 404


async def test_update_hero_partial(client, mock_session, mock_os):
    """PATCH only modifies fields that are explicitly sent."""
    hero = Hero(id=1, name="Spider-Man", secret_name="Peter Parker", age=24, team_id=1)
    mock_session.get.return_value = hero
    mock_session.refresh.side_effect = lambda obj: None

    await client.patch("/heroes/1", json={"age": 30})

    assert hero.name == "Spider-Man"  # untouched
    assert hero.age == 30             # updated


async def test_delete_hero(client, mock_session, mock_os):
    mock_session.get.return_value = SPIDER_MAN

    resp = await client.delete("/heroes/1")

    assert resp.status_code == 204
    mock_session.delete.assert_awaited_once_with(SPIDER_MAN)
    mock_session.commit.assert_awaited_once()
    mock_os.delete.assert_awaited_once()


async def test_delete_hero_not_found(client, mock_session):
    mock_session.get.return_value = None

    resp = await client.delete("/heroes/999")

    assert resp.status_code == 404
