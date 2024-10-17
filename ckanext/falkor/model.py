import logging
import sqlalchemy as sa
import ckan.model as model

from enum import Enum
from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional, Union
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base(metadata=model.meta.metadata)

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
    sequence = sa.Column(sa.INTEGER, nullable=False)
    created_at = sa.Column(sa.DateTime, nullable=False)
    synced_at = sa.Column(sa.DateTime, nullable=True)


def new_falkor_event(
    id: UUID,
    object_id: UUID,
    object_type: FalkorEventObjectType,
    event_type: FalkorEventType,
    user_id: str,
    sequence: sa.INTEGER,
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
        sequence=sequence,
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
    sequence: int,
    created_at: datetime,
):
    session.add(
        new_falkor_event(
            id=event_id,
            object_id=object_id,
            object_type=object_type,
            event_type=event_type,
            user_id=user_id,
            sequence=sequence,
            created_at=created_at,
        )
    )


class FalkorObjectEventSequence(Base):
    __tablename__ = "falkor_object_event_sequence"

    id = sa.Column(
        sa.dialects.postgresql.UUID(as_uuid=True),
        primary_key=True,
        nullable=False,
    )
    sequence = sa.Column(sa.INTEGER, nullable=False)


def new_falkor_object_event_sequence(object_id: UUID, sequence: sa.Integer = 0) -> FalkorObjectEventSequence:
    return FalkorObjectEventSequence(id=object_id, sequence=sequence)


def get_sequence_number(session: sa.orm.Session, object_id: UUID):
    object_event_sequence: Union[FalkorObjectEventSequence, None] = session.query(
        FalkorObjectEventSequence).get(object_id)

    if object_event_sequence is None:
        object_event_sequence = new_falkor_object_event_sequence(
            object_id=object_id)
        session.add(object_event_sequence)

    object_event_sequence.sequence += 1
    return object_event_sequence.sequence
