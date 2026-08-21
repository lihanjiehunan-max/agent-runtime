from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

IDENTIFIER_LENGTH = 254
DIGEST_LENGTH = 71

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class AgentPackageRow(Base):
    __tablename__ = "agent_package"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "agent_id", "version", name="uq_agent_package_version"
        ),
        UniqueConstraint("tenant_id", "digest", name="uq_agent_package_digest"),
        UniqueConstraint(
            "tenant_id",
            "agent_id",
            "version",
            "digest",
            name="uq_agent_package_identity",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    agent_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    version: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    digest: Mapped[str] = mapped_column(String(DIGEST_LENGTH), nullable=False)
    schema_version: Mapped[str] = mapped_column(
        String(64), nullable=False, server_default=text("'agent.package.v1'")
    )
    runtime_type: Mapped[str] = mapped_column(String(64), nullable=False)
    sdk_version: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    package_uri: Mapped[str] = mapped_column(String(2048), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=text("'active'")
    )
    manifest: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    checksum_evidence: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RuntimeSessionRow(Base):
    __tablename__ = "runtime_session"
    __table_args__ = (
        UniqueConstraint("tenant_id", "thread_id", name="uq_runtime_session_thread"),
        ForeignKeyConstraint(
            ["tenant_id", "agent_id", "package_version", "package_digest"],
            [
                "agent_package.tenant_id",
                "agent_package.agent_id",
                "agent_package.version",
                "agent_package.digest",
            ],
            name="fk_runtime_session_package",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id", "active_execution_id"],
            [
                "runtime_execution.tenant_id",
                "runtime_execution.session_id",
                "runtime_execution.execution_id",
            ],
            name="fk_runtime_session_active_execution",
            use_alter=True,
            deferrable=True,
            initially="DEFERRED",
        ),
        CheckConstraint(
            "status IN ('open', 'closed')", name="runtime_session_status"
        ),
        CheckConstraint("revision >= 0", name="runtime_session_revision"),
        CheckConstraint(
            "execution_epoch >= 0", name="runtime_session_execution_epoch"
        ),
        CheckConstraint(
            "last_event_sequence >= 0", name="runtime_session_last_event_sequence"
        ),
    )

    tenant_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    user_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    agent_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    package_version: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    package_digest: Mapped[str] = mapped_column(String(DIGEST_LENGTH), nullable=False)
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, server_default=text("'open'")
    )
    revision: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    execution_epoch: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    active_execution_id: Mapped[str | None] = mapped_column(
        String(IDENTIFIER_LENGTH), nullable=True
    )
    last_checkpoint_id: Mapped[str | None] = mapped_column(
        String(IDENTIFIER_LENGTH), nullable=True
    )
    last_event_sequence: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RuntimeExecutionRow(Base):
    __tablename__ = "runtime_execution"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "session_id",
            "execution_id",
            name="uq_runtime_execution_session_identity",
        ),
        UniqueConstraint("tenant_id", "trace_id", name="uq_runtime_execution_trace"),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["runtime_session.tenant_id", "runtime_session.session_id"],
            name="fk_runtime_execution_session",
        ),
        CheckConstraint(
            "execution_epoch >= 0", name="runtime_execution_execution_epoch"
        ),
        CheckConstraint("input_tokens >= 0", name="runtime_execution_input_tokens"),
        CheckConstraint("output_tokens >= 0", name="runtime_execution_output_tokens"),
        CheckConstraint("model_calls >= 0", name="runtime_execution_model_calls"),
        CheckConstraint("tool_calls >= 0", name="runtime_execution_tool_calls"),
    )

    tenant_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    execution_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    user_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    worker_id: Mapped[str | None] = mapped_column(String(IDENTIFIER_LENGTH), nullable=True)
    trace_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    execution_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mode: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    request_input: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_tokens: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    output_tokens: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    model_calls: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    tool_calls: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    result_ref: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class RuntimeEventRow(Base):
    __tablename__ = "runtime_event"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "session_id", "sequence", name="uq_runtime_event_sequence"
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id", "execution_id"],
            [
                "runtime_execution.tenant_id",
                "runtime_execution.session_id",
                "runtime_execution.execution_id",
            ],
            name="fk_runtime_event_execution",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "agent_id", "package_version", "package_digest"],
            [
                "agent_package.tenant_id",
                "agent_package.agent_id",
                "agent_package.version",
                "agent_package.digest",
            ],
            name="fk_runtime_event_package",
        ),
        CheckConstraint("sequence > 0", name="runtime_event_sequence"),
        CheckConstraint("duration_ms >= 0", name="runtime_event_duration"),
        Index("ix_runtime_event_execution_sequence", "tenant_id", "execution_id", "sequence"),
    )

    tenant_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    span_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    parent_span_id: Mapped[str | None] = mapped_column(
        String(IDENTIFIER_LENGTH), nullable=True
    )
    session_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    execution_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    agent_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    package_version: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    package_digest: Mapped[str] = mapped_column(String(DIGEST_LENGTH), nullable=False)
    runtime_type: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    sdk_version: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    event_type: Mapped[str] = mapped_column(String(254), nullable=False)
    phase: Mapped[str] = mapped_column(String(64), nullable=False)
    duration_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    payload_ref: Mapped[str | None] = mapped_column(String(2048), nullable=True)


class TraceProjectionRow(Base):
    __tablename__ = "trace_projection"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "execution_id", name="uq_trace_projection_execution"
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id", "execution_id"],
            [
                "runtime_execution.tenant_id",
                "runtime_execution.session_id",
                "runtime_execution.execution_id",
            ],
            name="fk_trace_projection_execution",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "agent_id", "package_version", "package_digest"],
            [
                "agent_package.tenant_id",
                "agent_package.agent_id",
                "agent_package.version",
                "agent_package.digest",
            ],
            name="fk_trace_projection_package",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    trace_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    execution_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    agent_id: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    package_version: Mapped[str] = mapped_column(String(IDENTIFIER_LENGTH), nullable=False)
    package_digest: Mapped[str] = mapped_column(String(DIGEST_LENGTH), nullable=False)
    model_ref: Mapped[str | None] = mapped_column(String(254), nullable=True)
    tool_versions: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    summary: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    event_index: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


__all__ = [
    "AgentPackageRow",
    "Base",
    "RuntimeEventRow",
    "RuntimeExecutionRow",
    "RuntimeSessionRow",
    "TraceProjectionRow",
]
