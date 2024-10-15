"""INIT

Revision ID: 376615bb5319
Revises:
Create Date: 2024-10-07 13:20:12.171995

"""
from alembic import op
from ckan.model import meta
from enum import Enum

import sqlalchemy as sa
import logging
import uuid

log = logging.getLogger(__name__)

# revision identifiers, used by Alembic.
revision = '376615bb5319'
down_revision = None
branch_labels = None
depends_on = None


class FalkorEventObjectType(Enum):
    PACKAGE = 'package'
    RESOURCE = 'resource'


class FalkorEventStatus(Enum):
    PENDING = 'pending'
    FAILED = 'failed'
    SYNCED = 'synced'


class FalkorEventType(Enum):
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"


def upgrade():
    op.create_table(
        "falkor_event",
        meta.MetaData(),
        sa.Column(
            "id",
            sa.dialects.postgresql.UUID(as_uuid=True),
            primary_key=True,
            nullable=False,
            default=uuid.uuid4
        ),
        sa.Column("object_id", sa.dialects.postgresql.UUID(
            as_uuid=True), nullable=False),
        sa.Column(
            "object_type",
            sa.Enum(FalkorEventObjectType),
            nullable=False
        ),
        sa.Column("event_type", sa.Enum(FalkorEventType), nullable=False),
        sa.Column("user_id", sa.TEXT, nullable=False, default="guest"),
        sa.Column(
            "status",
            sa.Enum(FalkorEventStatus),
            default=FalkorEventStatus.PENDING
        ),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("synced_at", sa.DateTime, nullable=True)
    )


def downgrade():
    op.drop_table(
        "falkor_event"
    )
    op.execute('DROP TYPE IF EXISTS falkoreventobjecttype;')
    op.execute('DROP TYPE IF EXISTS falkoreventtype;')
    op.execute('DROP TYPE IF EXISTS falkoreventstatus;')
