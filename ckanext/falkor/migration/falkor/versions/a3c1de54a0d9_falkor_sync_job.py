"""falkor_sync_job

Revision ID: a3c1de54a0d9
Revises: 376615bb5319
Create Date: 2024-10-22 14:44:44.219739

"""
from alembic import op
from ckan.model import meta
from uuid import uuid4
from enum import Enum

import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a3c1de54a0d9'
down_revision = '376615bb5319'
branch_labels = None
depends_on = None


class FalkorSyncJobStatus(Enum):
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


def upgrade():
    op.create_table(
        "falkor_sync_job",
        meta.MetaData(),
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            default=uuid4
        ),
        sa.Column(
            "status",
            sa.Enum(FalkorSyncJobStatus),
            nullable=False,
        ),
        sa.Column(
            "is_latest",
            sa.Boolean,
            nullable=False
        ),
        sa.Column(
            "start",
            sa.DateTime,
            nullable=False
        ),
        sa.Column(
            "end",
            sa.DateTime,
            nullable=True,
            default=None
        )
    )


def downgrade():
    op.drop_table(
        "falkor_sync_job"
    )
    op.execute("DROP TYPE IF EXISTS falkorsyncjobstatus;")
