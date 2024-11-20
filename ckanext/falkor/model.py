import logging
import sqlalchemy as sa
import ckan.model as ckan_model

from enum import Enum
from uuid import UUID, uuid4
from datetime import datetime
from typing import Optional, List
from sqlalchemy.ext.declarative import declarative_base
from ckan.model import meta, Resource, Package, Group
from ckan.lib.dictization import table_dictize

Base = declarative_base(metadata=meta.metadata)

log = logging.getLogger(__name__)


TOOLKIT_CONTEXT = {
    "model": ckan_model,
    "ignore_auth": True,
    "defer_commit": True
}


class FalkorEventResourceType(Enum):
    DEFAULT = 'default'
    STREAM = 'stream'


class FalkorEventStatus(str, Enum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    FAILED = 'failed'
    SYNCED = 'synced'


class FalkorEventType(str, Enum):
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
    org_id = sa.Column(sa.dialects.postgresql.UUID, nullable=False)
    org_name = sa.Column(sa.TEXT, nullable=False)
    package_id = sa.Column(sa.dialects.postgresql.UUID, nullable=False)
    package_name = sa.Column(sa.TEXT, nullable=False)
    resource_id = sa.Column(sa.dialects.postgresql.UUID, nullable=False)
    resource_name = sa.Column(sa.TEXT, nullable=False)
    resource_type = sa.Column(
        sa.Enum(FalkorEventResourceType),
        nullable=False,
        default=FalkorEventResourceType.DEFAULT
    )
    event_type = sa.Column(sa.Enum(FalkorEventType), nullable=False)
    user_id = sa.Column(sa.TEXT, nullable=False, default="guest")
    user_email = sa.Column(sa.TEXT, nullable=False, default="guest")
    status = sa.Column(sa.Enum(FalkorEventStatus),
                       default=FalkorEventStatus.PENDING)
    created_at = sa.Column(sa.DateTime, nullable=False)
    synced_at = sa.Column(sa.DateTime, nullable=True)


def get_pending_events(session: sa.orm.Session) -> List[FalkorEvent]:
    return session.query(FalkorEvent).filter(FalkorEvent.status == FalkorEventStatus.PENDING).all()


def get_failed_events(
    session: sa.orm.Session,
) -> List[FalkorEvent]:
    return session.query(FalkorEvent).filter(FalkorEvent.status == FalkorEventStatus.FAILED).order_by(FalkorEvent.created_at.desc()).all()


def get_events(status: Optional[FalkorEventStatus] = None) -> FalkorEvent:
    session = meta.create_local_session()
    try:
        query = session.query(FalkorEvent).order_by(FalkorEvent.created_at.desc())
        if status is not None:
            query = query.filter(FalkorEvent.status == status)
        return query.all()
    finally:
        session.close()


def get_event(event_id: str) -> FalkorEvent:
    session = meta.create_local_session()
    try:
        return session.query(FalkorEvent).get(event_id)
    finally:
        session.close()


def create_new_event(event_type: FalkorEventType, resource: dict, user: dict) -> FalkorEvent:
    package = table_dictize(Package.get(
        resource["package_id"]), TOOLKIT_CONTEXT)
    org = table_dictize(Group.get(package["owner_org"]), TOOLKIT_CONTEXT)

    event = FalkorEvent(
        org_id=org["id"],
        org_name=org["name"],
        package_id=package["id"],
        package_name=package["name"],
        resource_id=resource["id"],
        resource_name=resource["name"],
        user_id=user["id"],
        user_email=user["email"],
        event_type=event_type,
    )

    if event.event_type == FalkorEventType.CREATE:
        event.created_at = datetime.fromisoformat(resource["created"])
    else:
        event.created_at = datetime.now()

    if resource["resource_type"] == FalkorEventResourceType.STREAM.value:
        event.resource_type = FalkorEventResourceType.STREAM

    return event


def get_resources_without_create_events(session: sa.orm.Session) -> List[Resource]:
    distinct_resource_creates = session.query(
        FalkorEvent
    ).filter(
        FalkorEvent.event_type == FalkorEventType.CREATE
    ).subquery()

    return session.query(
        Resource
    ).outerjoin(
        distinct_resource_creates,
        Resource.id == sa.cast(
            distinct_resource_creates.c.resource_id, sa.TEXT)
    ).filter(
        sa.cast(distinct_resource_creates.c.resource_id, sa.TEXT) == None
    ).all()


def get_dictized_package(
    id: str
) -> Package:
    session = meta.create_local_session()
    try:
        return table_dictize(session.query(Package).get(id), TOOLKIT_CONTEXT)
    finally:
        session.close()


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
    id: UUID,
    start: datetime,
    status: FalkorSyncJobStatus = FalkorSyncJobStatus.RUNNING,
    is_latest: bool = True,
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
        raise Exception(
            f"Falkor sync job is already running. ID: {running_job.id}")

    session.query(FalkorSyncJob).filter(
        # Using "is True" doesn't seem to work here
        FalkorSyncJob.is_latest == True
    ).update({FalkorSyncJob.is_latest: False})
    session.add(job)


def get_sync_job_history(session: sa.orm.Session, limit: Optional[int] = None) -> List[FalkorSyncJob]:
    query = session.query(FalkorSyncJob).order_by(FalkorSyncJob.start.desc())
    if limit is not None:
        query = query.limit(limit)
    return query.all()
