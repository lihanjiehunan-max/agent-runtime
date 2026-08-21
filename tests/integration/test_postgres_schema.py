import asyncio
import io
import os
import uuid
from collections.abc import AsyncIterator
from contextlib import redirect_stdout, suppress
from dataclasses import dataclass

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import MetaData, UniqueConstraint, inspect, text
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from packages.runtime_persistence.database import create_runtime_engine
from packages.runtime_persistence.models import Base

EXPECTED_TABLES = {
    "agent_package",
    "runtime_session",
    "runtime_execution",
    "runtime_event",
    "trace_projection",
}
EXPECTED_COLUMNS = {
    "agent_package": {
        "tenant_id",
        "agent_id",
        "version",
        "digest",
        "schema_version",
        "runtime_type",
        "sdk_version",
        "package_uri",
        "status",
        "manifest",
        "checksum_evidence",
        "created_at",
        "updated_at",
    },
    "runtime_session": {
        "tenant_id",
        "session_id",
        "thread_id",
        "user_id",
        "agent_id",
        "package_version",
        "package_digest",
        "status",
        "revision",
        "execution_epoch",
        "active_execution_id",
        "last_checkpoint_id",
        "last_event_sequence",
        "created_at",
        "updated_at",
    },
    "runtime_execution": {
        "tenant_id",
        "execution_id",
        "session_id",
        "user_id",
        "actor_id",
        "worker_id",
        "trace_id",
        "execution_epoch",
        "mode",
        "status",
        "request_input",
        "input_tokens",
        "output_tokens",
        "model_calls",
        "tool_calls",
        "error",
        "result_ref",
        "created_at",
        "started_at",
        "completed_at",
    },
    "runtime_event": {
        "tenant_id",
        "event_id",
        "sequence",
        "occurred_at",
        "trace_id",
        "span_id",
        "parent_span_id",
        "session_id",
        "execution_id",
        "agent_id",
        "package_version",
        "package_digest",
        "runtime_type",
        "worker_id",
        "sdk_version",
        "event_type",
        "phase",
        "duration_ms",
        "payload",
        "payload_ref",
    },
    "trace_projection": {
        "tenant_id",
        "trace_id",
        "session_id",
        "execution_id",
        "agent_id",
        "package_version",
        "package_digest",
        "model_ref",
        "tool_versions",
        "status",
        "summary",
        "event_index",
        "created_at",
        "updated_at",
    },
}


@dataclass(frozen=True)
class _InspectedTable:
    columns: dict[str, bool]
    primary_key: tuple[str, ...]
    unique_constraints: set[tuple[str, ...]]
    foreign_keys: set[tuple[tuple[str, ...], str, tuple[str, ...]]]


def _unique_column_sets(metadata: MetaData, table_name: str) -> set[tuple[str, ...]]:
    table = metadata.tables[table_name]
    return {
        tuple(column.name for column in constraint.columns)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    }


def _foreign_key_column_sets(
    metadata: MetaData, table_name: str
) -> set[tuple[tuple[str, ...], str, tuple[str, ...]]]:
    table = metadata.tables[table_name]
    return {
        (
            tuple(element.parent.name for element in constraint.elements),
            constraint.referred_table.name,
            tuple(element.column.name for element in constraint.elements),
        )
        for constraint in table.foreign_key_constraints
    }


def test_runtime_metadata_declares_exact_required_tables_and_tenant_scope() -> None:
    metadata = Base.metadata

    assert set(metadata.tables) == EXPECTED_TABLES
    for table_name in EXPECTED_TABLES:
        tenant_column = metadata.tables[table_name].c.tenant_id
        assert tenant_column.nullable is False
        assert tenant_column.primary_key is True


def test_package_identity_and_event_sequence_are_unique_within_tenant() -> None:
    metadata = Base.metadata

    package_uniques = _unique_column_sets(metadata, "agent_package")
    assert ("tenant_id", "agent_id", "version") in package_uniques
    assert ("tenant_id", "digest") in package_uniques

    event_uniques = _unique_column_sets(metadata, "runtime_event")
    assert ("tenant_id", "session_id", "sequence") in event_uniques


def test_runtime_metadata_uses_tenant_preserving_foreign_keys() -> None:
    metadata = Base.metadata

    assert (
        ("tenant_id", "agent_id", "package_version", "package_digest"),
        "agent_package",
        ("tenant_id", "agent_id", "version", "digest"),
    ) in _foreign_key_column_sets(metadata, "runtime_session")
    assert (
        ("tenant_id", "session_id", "active_execution_id"),
        "runtime_execution",
        ("tenant_id", "session_id", "execution_id"),
    ) in _foreign_key_column_sets(metadata, "runtime_session")
    assert (
        ("tenant_id", "session_id"),
        "runtime_session",
        ("tenant_id", "session_id"),
    ) in _foreign_key_column_sets(metadata, "runtime_execution")
    assert (
        ("tenant_id", "session_id", "execution_id"),
        "runtime_execution",
        ("tenant_id", "session_id", "execution_id"),
    ) in _foreign_key_column_sets(metadata, "runtime_event")
    assert (
        ("tenant_id", "session_id", "execution_id"),
        "runtime_execution",
        ("tenant_id", "session_id", "execution_id"),
    ) in _foreign_key_column_sets(metadata, "trace_projection")


def test_runtime_engine_rejects_sqlite_fallback() -> None:
    with pytest.raises(ValueError, match="SQLite fallback is unsupported"):
        create_runtime_engine("sqlite+aiosqlite:///:memory:")


def _postgres_url() -> str:
    database_url = os.getenv("RUNTIME_TEST_DATABASE_URL")
    if database_url is None:
        pytest.skip(
            "PostgreSQL integration dependency unavailable: set "
            "RUNTIME_TEST_DATABASE_URL=postgresql+asyncpg://..."
        )
    url = make_url(database_url)
    if url.drivername != "postgresql+asyncpg":
        pytest.fail("RUNTIME_TEST_DATABASE_URL must use postgresql+asyncpg; SQLite is unsupported")
    return database_url


def _alembic_config(connection: Connection) -> Config:
    config = Config()
    config.set_main_option("script_location", "migrations")
    config.attributes["connection"] = connection
    return config


def _upgrade(connection: Connection) -> None:
    command.upgrade(_alembic_config(connection), "head")


def _inspect_migrated_schema(
    connection: Connection, schema_name: str
) -> tuple[set[str], dict[str, _InspectedTable]]:
    inspector = inspect(connection)
    table_names = set(inspector.get_table_names(schema=schema_name))
    tables: dict[str, _InspectedTable] = {}
    for table_name in EXPECTED_TABLES:
        columns = {
            column["name"]: column["nullable"]
            for column in inspector.get_columns(table_name, schema=schema_name)
        }
        primary_key = tuple(
            inspector.get_pk_constraint(table_name, schema=schema_name)[
                "constrained_columns"
            ]
        )
        unique_constraints = {
            tuple(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(
                table_name, schema=schema_name
            )
        }
        foreign_keys = {
            (
                tuple(foreign_key["constrained_columns"]),
                foreign_key["referred_table"],
                tuple(foreign_key["referred_columns"]),
            )
            for foreign_key in inspector.get_foreign_keys(
                table_name, schema=schema_name
            )
        }
        tables[table_name] = _InspectedTable(
            columns=columns,
            primary_key=primary_key,
            unique_constraints=unique_constraints,
            foreign_keys=foreign_keys,
        )
    return table_names, tables


def test_offline_migration_uses_naming_convention_once() -> None:
    config = Config()
    config.set_main_option("script_location", "migrations")
    config.set_main_option(
        "sqlalchemy.url", "postgresql+asyncpg://runtime:runtime@localhost/runtime"
    )
    output = io.StringIO()

    with redirect_stdout(output):
        command.upgrade(config, "head", sql=True)

    migration_sql = output.getvalue()
    assert "CONSTRAINT ck_runtime_session_runtime_session_status" in migration_sql
    assert "ck_runtime_session_ck_runtime_session" not in migration_sql
    assert (
        "FOREIGN KEY(tenant_id, session_id, active_execution_id) "
        "REFERENCES runtime_execution (tenant_id, session_id, execution_id)"
        in migration_sql
    )


async def _temporary_schema(connection: AsyncConnection) -> AsyncIterator[str]:
    schema_name = f"runtime_task3_{uuid.uuid4().hex}"
    await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    await connection.execute(text(f'SET search_path TO "{schema_name}"'))
    await connection.commit()
    try:
        yield schema_name
    finally:
        await connection.rollback()
        await connection.execute(text("SET search_path TO public"))
        await connection.execute(text(f'DROP SCHEMA "{schema_name}" CASCADE'))
        await connection.commit()


async def _assert_migration_schema(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            schema_iterator = _temporary_schema(connection)
            schema_name = await anext(schema_iterator)
            try:
                await connection.run_sync(_upgrade)
                actual_tables, inspected_tables = await connection.run_sync(
                    lambda sync_connection: _inspect_migrated_schema(
                        sync_connection, schema_name
                    )
                )
                assert actual_tables >= EXPECTED_TABLES
                assert "alembic_version" in actual_tables
                for table_name, expected_columns in EXPECTED_COLUMNS.items():
                    inspected_table = inspected_tables[table_name]
                    assert set(inspected_table.columns) == expected_columns
                    assert inspected_table.columns["tenant_id"] is False
                    assert "tenant_id" in inspected_table.primary_key
                    for local_columns, _, remote_columns in inspected_table.foreign_keys:
                        assert local_columns[0] == "tenant_id"
                        assert remote_columns[0] == "tenant_id"

                package_uniques = inspected_tables[
                    "agent_package"
                ].unique_constraints
                assert ("tenant_id", "agent_id", "version") in package_uniques
                assert ("tenant_id", "digest") in package_uniques
                assert (
                    "tenant_id",
                    "session_id",
                    "sequence",
                ) in inspected_tables["runtime_event"].unique_constraints
                assert (
                    ("tenant_id", "session_id", "active_execution_id"),
                    "runtime_execution",
                    ("tenant_id", "session_id", "execution_id"),
                ) in inspected_tables["runtime_session"].foreign_keys
            finally:
                with suppress(StopAsyncIteration):
                    await anext(schema_iterator)
    finally:
        await engine.dispose()


def test_alembic_upgrade_creates_runtime_metadata_in_postgresql() -> None:
    asyncio.run(_assert_migration_schema(_postgres_url()))
