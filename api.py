"""
FastAPI app combining async psycopg v3 + SQLModel (Postgres) with opensearch-py (OpenSearch).

Endpoints:
  POST   /teams              create a team
  GET    /teams              list all teams

  POST   /heroes             create a hero (written to Postgres + indexed in OpenSearch)
  GET    /heroes             list all heroes
  GET    /heroes/{id}        get one hero
  PATCH  /heroes/{id}        update a hero (synced to OpenSearch)
  DELETE /heroes/{id}        delete a hero (removed from OpenSearch)

  GET    /search?q=          full-text search over heroes via OpenSearch
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
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


@app.get("/heroes", response_model=list[Hero])
async def list_heroes(session: AsyncSession = Depends(get_session)) -> list[Hero]:
    result = await session.execute(select(Hero).order_by(Hero.name))
    return list(result.scalars().all())


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
async def search_heroes(q: str) -> list[SearchHit]:
    resp = await os_client.search(
        index=HERO_INDEX,
        body={
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["name", "secret_name"],
                }
            }
        },
    )
    return [
        SearchHit(
            id=hit["_id"],
            score=hit["_score"],
            **hit["_source"],
        )
        for hit in resp["hits"]["hits"]
    ]
