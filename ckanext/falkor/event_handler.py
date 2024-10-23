import logging

from uuid import UUID, uuid4
from datetime import datetime
from typing import Union, Optional
from ckanext.falkor.model import (
    FalkorEvent,
    FalkorEventType,
    FalkorEventObjectType,
    insert_pending_event,
)

from ckan.model import meta, Package, Resource
from ckanext.falkor.client import Client
from ckan.model.domain_object import DomainObjectOperation

log = logging.getLogger(__name__)


def generate_event_id() -> UUID:
    return uuid4()


class EventHandler:
    falkor: Client

    def __init__(self, falkor: Client):
        self.falkor = falkor

    def handle_package_create(
            self,
            package_id: str,
            metadata_created: datetime,
            user_id: str,
            event: Optional[FalkorEvent] = None
    ):
        if event is None:
            session = meta.create_local_session()
            event = insert_pending_event(
                session,
                event_id=generate_event_id(),
                object_id=UUID(package_id),
                object_type=FalkorEventObjectType.PACKAGE,
                event_type=FalkorEventType.CREATE,
                user_id=user_id,
                created_at=metadata_created
            )
            session.commit()

    def handle_resource_create(
            self,
            resource_id: str,
            created_at: datetime,
            user_id: str,
            event: Optional[FalkorEvent] = None
    ):
        if event is None:
            session = meta.create_local_session()
            insert_pending_event(
                session,
                event_id=generate_event_id(),
                object_id=UUID(resource_id),
                object_type=FalkorEventObjectType.RESOURCE,
                event_type=FalkorEventType.CREATE,
                user_id=user_id,
                created_at=created_at
            )
            session.commit()

    def handle_resource_read(
            self,
            resource_id: UUID,
            package_id: UUID,
            user_id: str,
            created_at: datetime = datetime.now(),
            event: Optional[FalkorEvent] = None
    ):
        if event is None:
            session = meta.create_local_session()
            insert_pending_event(
                session,
                event_id=generate_event_id(),
                object_id=resource_id,
                object_type=FalkorEventObjectType.RESOURCE,
                event_type=FalkorEventType.READ,
                user_id=user_id,
                created_at=created_at
            )
            session.commit()

    def handle_resource_update(
            self,
            resource_id: str,
            created_at: datetime,
            user_id: str,
            event: Optional[FalkorEvent] = None
    ):
        if event is None:
            session = meta.create_local_session()
            insert_pending_event(
                session,
                event_id=generate_event_id(),
                object_id=UUID(resource_id),
                object_type=FalkorEventObjectType.RESOURCE,
                event_type=FalkorEventType.UPDATE,
                user_id=user_id,
                created_at=created_at
            )
            session.commit()

    def handle_resource_delete(
            self,
            resource_id: str,
            created_at: datetime,
            user_id: str,
            event: Optional[FalkorEvent] = None
    ):
        if event is None:
            session = meta.create_local_session()
            insert_pending_event(
                session,
                event_id=generate_event_id(),
                object_id=UUID(resource_id),
                object_type=FalkorEventObjectType.RESOURCE,
                event_type=FalkorEventType.DELETE,
                user_id=user_id,
                created_at=created_at
            )
            session.commit()


def handle_read_event(
        handler: EventHandler,
        resource: dict,
        user_id: str,
):
    handler.handle_resource_read(
        resource["id"],
        resource["package_id"],
        user_id
    )


def handle_modification_event(
        handler: EventHandler,
        entity: Union[Package, Resource],
        operation: DomainObjectOperation,
        user_id: str,
):
    if isinstance(entity, Package):
        # Currently Falkor does not track changes to packages.
        # We only use the create event to create the dataset
        # and ignore any further changes.
        if operation != DomainObjectOperation.new:
            return

        handler.handle_package_create(
            package_id=entity.id,
            metadata_created=entity.metadata_created,
            user_id=user_id
        )

    elif isinstance(entity, Resource):
        if operation == DomainObjectOperation.new:
            handler.handle_resource_create(
                resource_id=entity.id,
                created_at=entity.created,
                user_id=user_id
            )
        elif operation == DomainObjectOperation.changed:
            handler.handle_resource_update(
                resource_id=entity.id,
                created_at=entity.created,
                user_id=user_id
            )
        elif operation == DomainObjectOperation.deleted:
            handler.handle_resource_delete(
                resource_id=entity.id,
                created_at=entity.created,
                user_id=user_id
            )
