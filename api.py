"""
FastAPI app combining async psycopg v3 + SQLModel (Postgres) with opensearch-py (OpenSearch).

Endpoints:
  POST   /teams                    create a team
  GET    /teams                    list all teams

  POST   /heroes                   create a hero (written to Postgres + indexed in OpenSearch)
  POST   /heroes/bulk              bulk-create heroes (add_all + flush + OS bulk)
  GET    /heroes                   list all heroes
  GET    /heroes/count             count indexed heroes via OpenSearch
  GET    /heroes/{id}              get one hero
  PATCH  /heroes/{id}              update a hero (synced to OpenSearch)
  DELETE /heroes/{id}              delete a hero (removed from OpenSearch)

  GET    /search?q=&team_id=&min_age=&max_age=    search heroes via OpenSearch
  GET    /stats                    avg age per team via OpenSearch aggregation
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import Body, Depends, FastAPI, HTTPException
from opensearchpy import AsyncOpenSearch
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlmodel import Field, SQLModel, select

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_URL = "postgresql+psycopg://demo:demo@localhost:5432/demodb"
OS_HOST = {"host": "localhost", "port": 9200}
HERO_INDEX = "heroes"

# ---------------------------------------------------------------------------
# Database models
# ---------------------------------------------------------------------------


class Team(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    city: str


class Hero(SQLModel, table=True):
    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    secret_name: str
    age: int | None = None
    team_id: int | None = Field(default=None, foreign_key="team.id")


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class TeamCreate(SQLModel):
    name: str
    city: str


class HeroCreate(SQLModel):
    name: str
    secret_name: str
    age: int | None = None
    team_id: int | None = None


class HeroUpdate(SQLModel):
    name: str | None = None
    secret_name: str | None = None
    age: int | None = None
    team_id: int | None = None


class SearchHit(SQLModel):
    id: str
    score: float
    name: str
    secret_name: str
    age: int | None
    team_id: int | None


class TeamStat(SQLModel):
    team_id: int
    hero_count: int
    avg_age: float | None


class HeroCount(SQLModel):
    count: int


# ---------------------------------------------------------------------------
# Infrastructure — engine + OpenSearch client
# ---------------------------------------------------------------------------

engine: AsyncEngine = create_async_engine(DB_URL, echo=True)

session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine, expire_on_commit=False
)

os_client: AsyncOpenSearch = AsyncOpenSearch(hosts=[OS_HOST], use_ssl=False)


async def init_os_index() -> None:
    if not await os_client.indices.exists(index=HERO_INDEX):
        await os_client.indices.create(
            index=HERO_INDEX,
            body={
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {
                    "properties": {
                        "name": {"type": "text"},
                        "secret_name": {"type": "text"},
                        "age": {"type": "integer"},
                        "team_id": {"type": "integer"},
                    }
                },
            },
        )


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    await init_os_index()
    yield
    await engine.dispose()
    await os_client.close()


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Heroes API", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


# ---------------------------------------------------------------------------
# OpenSearch helpers
# ---------------------------------------------------------------------------


def _hero_doc(hero: Hero) -> dict[str, Any]:
    return {
        "name": hero.name,
        "secret_name": hero.secret_name,
        "age": hero.age,
        "team_id": hero.team_id,
    }


async def _os_index(hero: Hero) -> None:
    await os_client.index(
        index=HERO_INDEX,
        id=hero.id,
        body=_hero_doc(hero),
        params={"refresh": "wait_for"},
    )


async def _os_delete(hero_id: int) -> None:
    await os_client.delete(
        index=HERO_INDEX,
        id=hero_id,
        params={"refresh": "wait_for", "ignore": 404},
    )


async def _os_bulk_index(heroes: list[Hero]) -> None:
    body: list[dict[str, Any]] = []
    for hero in heroes:
        body.append({"index": {"_index": HERO_INDEX, "_id": hero.id}})
        body.append(_hero_doc(hero))
    resp = await os_client.bulk(body=body, params={"refresh": "wait_for"})
    errors = [item for item in resp["items"] if "error" in item.get("index", {})]
    if errors:
        raise RuntimeError(f"OpenSearch bulk errors: {errors}")


# ---------------------------------------------------------------------------
# Team routes
# ---------------------------------------------------------------------------


@app.post("/teams", response_model=Team, status_code=201)
async def create_team(
    data: TeamCreate,
    session: AsyncSession = Depends(get_session),
) -> Team:
    team = Team.model_validate(data)
    session.add(team)
    await session.commit()
    await session.refresh(team)
    return team


@app.get("/teams", response_model=list[Team])
async def list_teams(session: AsyncSession = Depends(get_session)) -> list[Team]:
    result = await session.execute(select(Team).order_by(Team.name))
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Hero routes
# ---------------------------------------------------------------------------


@app.post("/heroes", response_model=Hero, status_code=201)
async def create_hero(
    data: HeroCreate,
    session: AsyncSession = Depends(get_session),
) -> Hero:
    hero = Hero.model_validate(data)
    session.add(hero)
    await session.commit()
    await session.refresh(hero)
    await _os_index(hero)
    return hero


@app.post("/heroes/bulk", response_model=list[Hero], status_code=201)
async def bulk_create_heroes(
    data: Annotated[list[HeroCreate], Body()],
    session: AsyncSession = Depends(get_session),
) -> list[Hero]:
    heroes = [Hero.model_validate(d) for d in data]
    session.add_all(heroes)
    await session.flush()   # populate auto-generated IDs before bulk index
    await session.commit()
    await _os_bulk_index(heroes)
    return heroes


@app.get("/heroes", response_model=list[Hero])
async def list_heroes(session: AsyncSession = Depends(get_session)) -> list[Hero]:
    result = await session.execute(select(Hero).order_by(Hero.name))
    return list(result.scalars().all())


@app.get("/heroes/count", response_model=HeroCount)
async def count_heroes() -> HeroCount:
    resp = await os_client.count(index=HERO_INDEX)
    return HeroCount(count=resp["count"])


@app.get("/heroes/{hero_id}", response_model=Hero)
async def get_hero(
    hero_id: int,
    session: AsyncSession = Depends(get_session),
) -> Hero:
    hero = await session.get(Hero, hero_id)
    if not hero:
        raise HTTPException(status_code=404, detail="Hero not found")
    return hero


@app.patch("/heroes/{hero_id}", response_model=Hero)
async def update_hero(
    hero_id: int,
    data: HeroUpdate,
    session: AsyncSession = Depends(get_session),
) -> Hero:
    hero = await session.get(Hero, hero_id)
    if not hero:
        raise HTTPException(status_code=404, detail="Hero not found")

    for field, value in data.model_dump(exclude_unset=True).items():
        setattr(hero, field, value)

    session.add(hero)
    await session.commit()
    await session.refresh(hero)
    await _os_index(hero)
    return hero


@app.delete("/heroes/{hero_id}", status_code=204)
async def delete_hero(
    hero_id: int,
    session: AsyncSession = Depends(get_session),
) -> None:
    hero = await session.get(Hero, hero_id)
    if not hero:
        raise HTTPException(status_code=404, detail="Hero not found")
    await session.delete(hero)
    await session.commit()
    await _os_delete(hero_id)


# ---------------------------------------------------------------------------
# Search route
# ---------------------------------------------------------------------------


@app.get("/search", response_model=list[SearchHit])
async def search_heroes(
    q: str | None = None,
    team_id: int | None = None,
    min_age: int | None = None,
    max_age: int | None = None,
) -> list[SearchHit]:
    must: list[dict[str, Any]] = (
        [{"multi_match": {"query": q, "fields": ["name", "secret_name"]}}]
        if q
        else [{"match_all": {}}]
    )
    filters: list[dict[str, Any]] = []
    if team_id is not None:
        filters.append({"term": {"team_id": team_id}})
    if min_age is not None or max_age is not None:
        age_range: dict[str, int] = {}
        if min_age is not None:
            age_range["gte"] = min_age
        if max_age is not None:
            age_range["lte"] = max_age
        filters.append({"range": {"age": age_range}})

    resp = await os_client.search(
        index=HERO_INDEX,
        body={"query": {"bool": {"must": must, "filter": filters}}},
    )
    return [
        SearchHit(id=hit["_id"], score=hit["_score"], **hit["_source"])
        for hit in resp["hits"]["hits"]
    ]


@app.get("/stats", response_model=list[TeamStat])
async def hero_stats() -> list[TeamStat]:
    resp = await os_client.search(
        index=HERO_INDEX,
        body={
            "size": 0,
            "aggs": {
                "by_team": {
                    "terms": {"field": "team_id"},
                    "aggs": {"avg_age": {"avg": {"field": "age"}}},
                }
            },
        },
    )
    return [
        TeamStat(
            team_id=bucket["key"],
            hero_count=bucket["doc_count"],
            avg_age=bucket["avg_age"]["value"],
        )
        for bucket in resp["aggregations"]["by_team"]["buckets"]
    ]
