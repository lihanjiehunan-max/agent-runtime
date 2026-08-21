from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def create_runtime_engine(database_url: str, *, echo: bool = False) -> AsyncEngine:
    url = make_url(database_url)
    if url.drivername != "postgresql+asyncpg":
        raise ValueError(
            "runtime metadata requires a postgresql+asyncpg database URL; "
            "SQLite fallback is unsupported"
        )
    return create_async_engine(url, echo=echo, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


__all__ = ["create_runtime_engine", "create_session_factory"]
