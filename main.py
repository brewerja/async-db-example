"""
Async psycopg v3 + SQLModel + SQLAlchemy example.

Stack:
  - psycopg v3 (async driver)
  - SQLAlchemy 2.x async engine
  - SQLModel for model definitions
"""

import asyncio

from collections.abc import Sequence

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlmodel import Field, SQLModel, col, select

DATABASE_URL = "postgresql+psycopg://demo:demo@localhost:5432/demodb"

# ---------------------------------------------------------------------------
# Models
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
# Engine & session factory
#
# async_sessionmaker is the SQLAlchemy 2.0 async counterpart to sessionmaker.
# It is parameterized by the session class so callers know the context manager
# yields an AsyncSession (rather than the base Session type).
# ---------------------------------------------------------------------------

engine: AsyncEngine = create_async_engine(DATABASE_URL, echo=True)

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    engine,
    expire_on_commit=False,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def create_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)


async def drop_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

async def main() -> None:
    print("\n=== Creating tables ===")
    await drop_tables()   # start fresh each run
    await create_tables()

    # --- Insert ---
    print("\n=== Inserting rows ===")
    async with AsyncSessionLocal() as session:
        avengers = Team(name="Avengers", city="New York")
        guardians = Team(name="Guardians", city="Space")
        session.add_all([avengers, guardians])
        await session.flush()   # get auto-generated IDs before commit

        heroes: list[Hero] = [
            Hero(name="Spider-Man",   secret_name="Peter Parker",  age=24, team_id=avengers.id),
            Hero(name="Iron Man",     secret_name="Tony Stark",    age=48, team_id=avengers.id),
            Hero(name="Star-Lord",    secret_name="Peter Quill",   age=38, team_id=guardians.id),
            Hero(name="Groot",        secret_name="I am Groot",            team_id=guardians.id),
        ]
        session.add_all(heroes)
        await session.commit()
        print(f"  Inserted {len(heroes)} heroes across 2 teams.")

    # --- Query all heroes ---
    print("\n=== All heroes ===")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Hero).order_by(Hero.name))
        for hero in result.scalars().all():
            print(f"  [{hero.id}] {hero.name!r:20s} age={hero.age!s:>4}  team_id={hero.team_id}")

    # --- Filtered join query ---
    # col() wraps a SQLModel field as a column expression so type checkers
    # understand that == produces a SQL clause, not a Python bool.
    print("\n=== Avengers only ===")
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Hero, Team)
            .join(Team, col(Hero.team_id) == Team.id)
            .where(Team.name == "Avengers")
            .order_by(Hero.name)
        )
        for hero, team in result.all():
            print(f"  {hero.name} ({team.city})")

    # --- Update ---
    print("\n=== Update Groot's age ===")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Hero).where(Hero.name == "Groot"))
        groot = result.scalars().one()
        groot.age = 1000
        session.add(groot)
        await session.commit()
        await session.refresh(groot)
        print(f"  Groot's age is now {groot.age}")

    # --- Delete ---
    print("\n=== Delete Star-Lord ===")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Hero).where(Hero.name == "Star-Lord"))
        star_lord = result.scalars().one()
        await session.delete(star_lord)
        await session.commit()
        print("  Star-Lord deleted.")

    # --- Final count ---
    print("\n=== Remaining heroes ===")
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Hero).order_by(Hero.name))
        remaining: Sequence[Hero] = result.scalars().all()
        for hero in remaining:
            print(f"  {hero.name}")
        print(f"\n  Total: {len(remaining)} heroes remaining.")

    await engine.dispose()
    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
