import logging
import sqlalchemy as sa

from enum import Enum
from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional, List
from sqlalchemy.ext.declarative import declarative_base
from ckan.model import meta, Package, Resource

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


def get_pending_events(session: sa.orm.Session) -> List[FalkorEvent]:
    return session.query(FalkorEvent).filter(FalkorEvent.status == FalkorEventStatus.PENDING).all()


def get_packages_without_create_events(session: sa.orm.Session) -> List[Package]:
    distinct_package_creates = session.query(
        FalkorEvent
    ).filter(
        FalkorEvent.object_type == FalkorEventObjectType.PACKAGE
    ).filter(
        FalkorEvent.event_type == FalkorEventType.CREATE
    ).subquery()

    return session.query(
        Package
    ).outerjoin(
        distinct_package_creates,
        Package.id == sa.cast(
            distinct_package_creates.c.object_id, sa.TEXT)
    ).filter(
        sa.cast(distinct_package_creates.c.object_id, sa.TEXT) == None
    ).all()


def get_resources_without_create_events(session: sa.orm.Session) -> List[Resource]:
    distinct_resource_creates = session.query(
        FalkorEvent
    ).filter(
        FalkorEvent.object_type == FalkorEventObjectType.RESOURCE
    ).filter(
        FalkorEvent.event_type == FalkorEventType.CREATE
    ).subquery()

    return session.query(
        Resource
    ).outerjoin(
        distinct_resource_creates,
        Resource.id == sa.cast(
            distinct_resource_creates.c.object_id, sa.TEXT)
    ).filter(
        sa.cast(distinct_resource_creates.c.object_id, sa.TEXT) == None
    ).all()


class FalkorSyncJobStatus(Enum):
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class FalkorSyncJob(Base):
    __tablename__ = "falkor_sync_job"

    id = sa.Column(
        sa.dialects.postgresql.UUID(as_uuid=True),
        primary_key=True,
        nullable=False,
        default=uuid4
    )
    status = sa.Column(
        sa.Enum(FalkorSyncJobStatus),
        nullable=False,
    )
    is_latest = sa.Column(
        sa.Boolean,
        nullable=False
    )
    start = sa.Column(
        sa.DateTime,
        nullable=False
    )
    end = sa.Column(
        sa.DateTime,
        nullable=True,
        default=None
    )


def new_falkor_sync_job(
    id: UUID = uuid4(),
    status: FalkorSyncJobStatus = FalkorSyncJobStatus.RUNNING,
    is_latest: bool = True,
    start: datetime = datetime.now(),
    end: Optional[datetime] = None
) -> FalkorSyncJob:
    return FalkorSyncJob(
        id=id,
        status=status,
        is_latest=is_latest,
        start=start,
        end=end
    )


def insert_new_falkor_sync_job(session: sa.orm.Session, job: FalkorSyncJob):
    running_job = session.query(FalkorSyncJob).filter(
        FalkorSyncJob.status == FalkorSyncJobStatus.RUNNING
    ).first()

    if running_job is not None:
        raise Exception(f"Falkor sync job is already running. ID: {running_job.id}")

    session.query(FalkorSyncJob).filter(
        FalkorSyncJob.is_latest == True
    ).update({FalkorSyncJob.is_latest: False})
    session.add(job)
    session.commit()
