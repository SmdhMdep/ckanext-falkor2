import uuid
import sqlalchemy as sa
import ckan.model as model

from enum import Enum
from typing import Union, Optional
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
        sa.dialects.postgresql.UUID,
        primary_key=True,
        nullable=False,
        default=uuid.uuid4
    )
    object_id = sa.Column(sa.dialects.postgresql.UUID, nullable=False)
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
    id: uuid.UUID,
    object_id: uuid.UUID,
    object_type: FalkorEventObjectType,
    user_id: Union[uuid.UUID, str],
    created_at: sa.DateTime,
    status: FalkorEventStatus = FalkorEventStatus.PENDING,
    synced_at: Optional[sa.DateTime] = None
) -> FalkorEvent:
    return FalkorEvent(
        id=id,
        object_id=object_id,
        object_type=object_type,
        user_id=user_id,
        status=status,
        created_at=created_at,
        synced_at=synced_at
    )
