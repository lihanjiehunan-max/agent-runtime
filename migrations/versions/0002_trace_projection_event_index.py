"""Persist trace event identity metadata for projection CAS guards.

Revision ID: 0002_trace_projection_event_index
Revises: 0001_runtime_metadata
Create Date: 2026-08-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_trace_projection_event_index"
down_revision: str | None = "0001_runtime_metadata"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "trace_projection",
        sa.Column(
            "event_index",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("trace_projection", "event_index")
