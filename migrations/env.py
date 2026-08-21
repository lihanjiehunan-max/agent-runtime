import asyncio
import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from packages.runtime_persistence.database import create_runtime_engine
from packages.runtime_persistence.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    database_url = os.getenv("RUNTIME_DATABASE_URL") or config.get_main_option(
        "sqlalchemy.url"
    )
    if not database_url:
        raise RuntimeError("RUNTIME_DATABASE_URL is required for offline migrations")
    context.configure(
        url=database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _run_migrations(connection: Connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def _run_async_migrations(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        await connection.run_sync(_run_migrations)
    await engine.dispose()


def run_migrations_online() -> None:
    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        if not isinstance(supplied_connection, Connection):
            raise TypeError("Alembic connection attribute must be a SQLAlchemy Connection")
        _run_migrations(supplied_connection)
        return

    database_url = os.getenv("RUNTIME_DATABASE_URL") or config.get_main_option(
        "sqlalchemy.url"
    )
    if not database_url:
        raise RuntimeError("RUNTIME_DATABASE_URL is required for online migrations")
    asyncio.run(_run_async_migrations(create_runtime_engine(database_url)))


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
