import sqlalchemy as sa
import ckan.model as model

from enum import Enum
from uuid import UUID, uuid4
from typing import Optional
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base(metadata=model.meta.metadata)


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
