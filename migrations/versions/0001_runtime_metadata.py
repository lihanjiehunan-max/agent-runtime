"""Create tenant-scoped runtime metadata tables.

Revision ID: 0001_runtime_metadata
Revises:
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_runtime_metadata"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "agent_package",
        sa.Column("tenant_id", sa.String(length=254), nullable=False),
        sa.Column("agent_id", sa.String(length=254), nullable=False),
        sa.Column("version", sa.String(length=254), nullable=False),
        sa.Column("digest", sa.String(length=71), nullable=False),
        sa.Column(
            "schema_version",
            sa.String(length=64),
            server_default=sa.text("'agent.package.v1'"),
            nullable=False,
        ),
        sa.Column("runtime_type", sa.String(length=64), nullable=False),
        sa.Column("sdk_version", sa.String(length=254), nullable=False),
        sa.Column("package_uri", sa.String(length=2048), nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            server_default=sa.text("'active'"),
            nullable=False,
        ),
        sa.Column(
            "manifest",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "checksum_evidence",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "agent_id", "version", name="pk_agent_package"
        ),
        sa.UniqueConstraint(
            "tenant_id", "agent_id", "version", name="uq_agent_package_version"
        ),
        sa.UniqueConstraint(
            "tenant_id", "digest", name="uq_agent_package_digest"
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_id",
            "version",
            "digest",
            name="uq_agent_package_identity",
        ),
    )

    op.create_table(
        "runtime_session",
        sa.Column("tenant_id", sa.String(length=254), nullable=False),
        sa.Column("session_id", sa.String(length=254), nullable=False),
        sa.Column("thread_id", sa.String(length=254), nullable=False),
        sa.Column("user_id", sa.String(length=254), nullable=False),
        sa.Column("agent_id", sa.String(length=254), nullable=False),
        sa.Column("package_version", sa.String(length=254), nullable=False),
        sa.Column("package_digest", sa.String(length=71), nullable=False),
        sa.Column(
            "status",
            sa.String(length=32),
            server_default=sa.text("'open'"),
            nullable=False,
        ),
        sa.Column(
            "revision", sa.BigInteger(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "execution_epoch",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("active_execution_id", sa.String(length=254), nullable=True),
        sa.Column("last_checkpoint_id", sa.String(length=254), nullable=True),
        sa.Column(
            "last_event_sequence",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('open', 'closed')", name="runtime_session_status"
        ),
        sa.CheckConstraint(
            "revision >= 0", name="runtime_session_revision"
        ),
        sa.CheckConstraint(
            "execution_epoch >= 0",
            name="runtime_session_execution_epoch",
        ),
        sa.CheckConstraint(
            "last_event_sequence >= 0",
            name="runtime_session_last_event_sequence",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_id", "package_version", "package_digest"],
            [
                "agent_package.tenant_id",
                "agent_package.agent_id",
                "agent_package.version",
                "agent_package.digest",
            ],
            name="fk_runtime_session_package",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id", "session_id", name="pk_runtime_session"
        ),
        sa.UniqueConstraint(
            "tenant_id", "thread_id", name="uq_runtime_session_thread"
        ),
    )

    op.create_table(
        "runtime_execution",
        sa.Column("tenant_id", sa.String(length=254), nullable=False),
        sa.Column("execution_id", sa.String(length=254), nullable=False),
        sa.Column("session_id", sa.String(length=254), nullable=False),
        sa.Column("user_id", sa.String(length=254), nullable=False),
        sa.Column("actor_id", sa.String(length=254), nullable=False),
        sa.Column("worker_id", sa.String(length=254), nullable=True),
        sa.Column("trace_id", sa.String(length=254), nullable=False),
        sa.Column("execution_epoch", sa.BigInteger(), nullable=False),
        sa.Column("mode", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("request_input", sa.Text(), nullable=True),
        sa.Column(
            "input_tokens", sa.BigInteger(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "output_tokens", sa.BigInteger(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "model_calls", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column(
            "tool_calls", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("result_ref", sa.String(length=2048), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "execution_epoch >= 0",
            name="runtime_execution_execution_epoch",
        ),
        sa.CheckConstraint(
            "input_tokens >= 0",
            name="runtime_execution_input_tokens",
        ),
        sa.CheckConstraint(
            "output_tokens >= 0",
            name="runtime_execution_output_tokens",
        ),
        sa.CheckConstraint(
            "model_calls >= 0",
            name="runtime_execution_model_calls",
        ),
        sa.CheckConstraint(
            "tool_calls >= 0", name="runtime_execution_tool_calls"
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["runtime_session.tenant_id", "runtime_session.session_id"],
            name="fk_runtime_execution_session",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id", "execution_id", name="pk_runtime_execution"
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "session_id",
            "execution_id",
            name="uq_runtime_execution_session_identity",
        ),
        sa.UniqueConstraint(
            "tenant_id", "trace_id", name="uq_runtime_execution_trace"
        ),
    )

    op.create_foreign_key(
        "fk_runtime_session_active_execution",
        "runtime_session",
        "runtime_execution",
        ["tenant_id", "session_id", "active_execution_id"],
        ["tenant_id", "session_id", "execution_id"],
        deferrable=True,
        initially="DEFERRED",
    )

    op.create_table(
        "runtime_event",
        sa.Column("tenant_id", sa.String(length=254), nullable=False),
        sa.Column("event_id", sa.String(length=254), nullable=False),
        sa.Column("sequence", sa.BigInteger(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trace_id", sa.String(length=254), nullable=False),
        sa.Column("span_id", sa.String(length=254), nullable=False),
        sa.Column("parent_span_id", sa.String(length=254), nullable=True),
        sa.Column("session_id", sa.String(length=254), nullable=False),
        sa.Column("execution_id", sa.String(length=254), nullable=False),
        sa.Column("agent_id", sa.String(length=254), nullable=False),
        sa.Column("package_version", sa.String(length=254), nullable=False),
        sa.Column("package_digest", sa.String(length=71), nullable=False),
        sa.Column("runtime_type", sa.String(length=64), nullable=False),
        sa.Column("worker_id", sa.String(length=254), nullable=False),
        sa.Column("sdk_version", sa.String(length=254), nullable=False),
        sa.Column("event_type", sa.String(length=254), nullable=False),
        sa.Column("phase", sa.String(length=64), nullable=False),
        sa.Column("duration_ms", sa.Float(), nullable=True),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("payload_ref", sa.String(length=2048), nullable=True),
        sa.CheckConstraint(
            "sequence > 0", name="runtime_event_sequence"
        ),
        sa.CheckConstraint(
            "duration_ms >= 0", name="runtime_event_duration"
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "session_id", "execution_id"],
            [
                "runtime_execution.tenant_id",
                "runtime_execution.session_id",
                "runtime_execution.execution_id",
            ],
            name="fk_runtime_event_execution",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_id", "package_version", "package_digest"],
            [
                "agent_package.tenant_id",
                "agent_package.agent_id",
                "agent_package.version",
                "agent_package.digest",
            ],
            name="fk_runtime_event_package",
        ),
        sa.PrimaryKeyConstraint("tenant_id", "event_id", name="pk_runtime_event"),
        sa.UniqueConstraint(
            "tenant_id", "session_id", "sequence", name="uq_runtime_event_sequence"
        ),
    )
    op.create_index(
        "ix_runtime_event_execution_sequence",
        "runtime_event",
        ["tenant_id", "execution_id", "sequence"],
        unique=False,
    )

    op.create_table(
        "trace_projection",
        sa.Column("tenant_id", sa.String(length=254), nullable=False),
        sa.Column("trace_id", sa.String(length=254), nullable=False),
        sa.Column("session_id", sa.String(length=254), nullable=False),
        sa.Column("execution_id", sa.String(length=254), nullable=False),
        sa.Column("agent_id", sa.String(length=254), nullable=False),
        sa.Column("package_version", sa.String(length=254), nullable=False),
        sa.Column("package_digest", sa.String(length=71), nullable=False),
        sa.Column("model_ref", sa.String(length=254), nullable=True),
        sa.Column(
            "tool_versions",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column(
            "summary",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id", "session_id", "execution_id"],
            [
                "runtime_execution.tenant_id",
                "runtime_execution.session_id",
                "runtime_execution.execution_id",
            ],
            name="fk_trace_projection_execution",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_id", "package_version", "package_digest"],
            [
                "agent_package.tenant_id",
                "agent_package.agent_id",
                "agent_package.version",
                "agent_package.digest",
            ],
            name="fk_trace_projection_package",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id", "trace_id", name="pk_trace_projection"
        ),
        sa.UniqueConstraint(
            "tenant_id", "execution_id", name="uq_trace_projection_execution"
        ),
    )


def downgrade() -> None:
    op.drop_table("trace_projection")
    op.drop_index("ix_runtime_event_execution_sequence", table_name="runtime_event")
    op.drop_table("runtime_event")
    op.drop_constraint(
        "fk_runtime_session_active_execution",
        "runtime_session",
        type_="foreignkey",
    )
    op.drop_table("runtime_execution")
    op.drop_table("runtime_session")
    op.drop_table("agent_package")
