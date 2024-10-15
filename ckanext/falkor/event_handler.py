
import logging
import sqlalchemy as sa

from datetime import datetime
from uuid import UUID, uuid4
from ckanext.falkor.model import FalkorEventObjectType, FalkorEventType, new_falkor_event
from ckanext.falkor.client import Client
from ckan.model import meta

log = logging.getLogger(__name__)


def generate_event_id() -> UUID:
    return uuid4()


class EventHandler:
    falkor: Client
    engine: sa.engine.Engine

    def __init__(self, falkor: Client):
        self.falkor = falkor
        self.engine = meta.engine

    def handle_package_create(self, package: dict, user_id: str):
        log.info(package)
        self.__insert_pending_event(
            event_id=generate_event_id(),
            object_id=UUID(package["id"]),
            object_type=FalkorEventObjectType.PACKAGE,
            event_type=FalkorEventType.CREATE,
            user_id=user_id,
            created_at=package["metadata_created"]
        )

    def handle_resource_create(self, resource: dict, user_id: str):
        self.__insert_pending_event(
            event_id=generate_event_id(),
            object_id=UUID(resource["id"]),
            object_type=FalkorEventObjectType.RESOURCE,
            event_type=FalkorEventType.CREATE,
            user_id=user_id,
            created_at=resource["created"]
        )

    def handle_resource_read(
            self,
            resource_id: UUID,
            package_id: UUID,
            user_id: str,
            created_at: datetime = datetime.now()
    ):
        self.__insert_pending_event(
            event_id=generate_event_id(),
            object_id=resource_id,
            object_type=FalkorEventObjectType.RESOURCE,
            event_type=FalkorEventType.READ,
            user_id=user_id,
            created_at=created_at
        )

    def handle_resource_update(self, resource: dict, user_id: str):
        self.__insert_pending_event(
            event_id=generate_event_id(),
            object_id=UUID(resource["id"]),
            object_type=FalkorEventObjectType.RESOURCE,
            event_type=FalkorEventType.UPDATE,
            user_id=user_id,
            created_at=resource["created"]
        )

    def handle_resource_delete(self, resource: dict, user_id: str):
        self.__insert_pending_event(
            event_id=generate_event_id(),
            object_id=UUID(resource["id"]),
            object_type=FalkorEventObjectType.RESOURCE,
            event_type=FalkorEventType.DELETE,
            user_id=user_id,
            created_at=resource["created"]
        )

    def __insert_pending_event(
        self,
        event_id: UUID,
        object_id: UUID,
        object_type: FalkorEventObjectType,
        event_type: FalkorEventType,
        user_id: str,
        created_at: datetime,
    ):
        session = sa.orm.Session(bind=self.engine)
        try:
            event = new_falkor_event(
                id=event_id,
                object_id=object_id,
                object_type=object_type,
                event_type=event_type,
                user_id=user_id,
                created_at=created_at,
            )
            session.add(event)
            session.commit()
        except Exception as e:
            logging.critical(e, exc_info=True)
            session.rollback()
        finally:
            session.close()
