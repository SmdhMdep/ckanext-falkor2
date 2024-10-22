import logging
import sqlalchemy as sa

from enum import Enum
from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional
from sqlalchemy.ext.declarative import declarative_base
from ckan.model import meta

Base = declarative_base(metadata=meta.metadata)

log = logging.getLogger(__name__)


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


class FalkorEvent(Base):
    __tablename__ = "falkor_event"

    id = sa.Column(
        sa.dialects.postgresql.UUID(as_uuid=True),
        primary_key=True,
        nullable=False,
        default=uuid4
    )
    object_id = sa.Column(sa.dialects.postgresql.UUID(
        as_uuid=True), nullable=False)
    object_type = sa.Column(sa.Enum(FalkorEventObjectType), nullable=False)
    event_type = sa.Column(sa.Enum(FalkorEventType), nullable=False)
    user_id = sa.Column(sa.TEXT, nullable=False, default="guest")
    status = sa.Column(
        sa.Enum(FalkorEventStatus),
        default=FalkorEventStatus.PENDING
    )
    created_at = sa.Column(sa.DateTime, nullable=False)
    synced_at = sa.Column(sa.DateTime, nullable=True)


def new_falkor_event(
    id: UUID,
    object_id: UUID,
    object_type: FalkorEventObjectType,
    event_type: FalkorEventType,
    user_id: str,
    created_at: sa.DateTime,
    status: FalkorEventStatus = FalkorEventStatus.PENDING,
    synced_at: Optional[sa.DateTime] = None
) -> FalkorEvent:
    return FalkorEvent(
        id=id,
        object_id=object_id,
        object_type=object_type,
        event_type=event_type,
        user_id=user_id,
        status=status,
        created_at=created_at,
        synced_at=synced_at
    )


def insert_pending_event(
    session: sa.orm.Session,
    event_id: UUID,
    object_id: UUID,
    object_type: FalkorEventObjectType,
    event_type: FalkorEventType,
    user_id: str,
    created_at: datetime,
):
    session.add(
        new_falkor_event(
            id=event_id,
            object_id=object_id,
            object_type=object_type,
            event_type=event_type,
            user_id=user_id,
            created_at=created_at,
        )
    )


class FalkorConfig(Base):
    __tablename__ = "falkor_config"

    initialised = sa.Column(sa.Boolean, nullable=False, primary_key=True)


def get_falkor_config(session: sa.orm.Session) -> FalkorConfig:
    return session.query(FalkorConfig).first()


def validate_falkor_config(session: sa.orm.Session):
    row_count = session.query(FalkorConfig).count()
    if row_count != 1:
        raise Exception(
            f"falkor_config should have exactly 1 row. Has {row_count}"
        )
