from api import Team
from tests.conftest import scalars


async def test_create_team(client, mock_session):
    mock_session.refresh.side_effect = lambda obj: setattr(obj, "id", 1)

    resp = await client.post("/teams", json={"name": "X-Men", "city": "Westchester"})

    assert resp.status_code == 201
    assert resp.json()["name"] == "X-Men"
    mock_session.add.assert_called_once()
    mock_session.commit.assert_awaited_once()


async def test_list_teams(client, mock_session):
    mock_session.execute.return_value = scalars(
        Team(id=1, name="Avengers", city="New York"),
        Team(id=2, name="X-Men", city="Westchester"),
    )

    resp = await client.get("/teams")

    assert resp.status_code == 200
    names = [t["name"] for t in resp.json()]
    assert names == ["Avengers", "X-Men"]
