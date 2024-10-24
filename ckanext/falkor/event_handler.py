import logging
import sqlalchemy as sa

from datetime import datetime
from requests import HTTPError

from ckanext.falkor.model import (
    FalkorEvent,
    FalkorEventType,
    FalkorEventStatus,
    FalkorEventObjectType,
    get_package_create_event_for_resource
)
from ckanext.falkor.client import Client

from ckan.model import meta
from ckan.model.domain_object import DomainObjectOperation

log = logging.getLogger(__name__)

DomainObjectOperationToFalkorEventTypeMap = {
    DomainObjectOperation.new: FalkorEventType.CREATE,
    DomainObjectOperation.changed: FalkorEventType.UPDATE,
    DomainObjectOperation.deleted: FalkorEventType.DELETE
}


class EventHandler:
    falkor: Client

    def __init__(self, falkor: Client):
        self.falkor = falkor

    def handle(self, event: FalkorEvent):
        session: sa.orm.Session = meta.create_local_session()
        session.add(event)
        session.commit()
        try:
            # TODO: Clean up nesting.
            if event.object_type == FalkorEventObjectType.PACKAGE:
                # TODO: Is there a way to avoid setting PROCESSING in both branches?
                event.status = FalkorEventStatus.PROCESSING
                session.commit()

                self.falkor.dataset_create(event.object_id)

            elif event.object_type == FalkorEventObjectType.RESOURCE:
                package_create_event = get_package_create_event_for_resource(
                    session, event.object_id)

                # TODO: Add retry here in case resource was created shortly after
                # package and it is still processing.
                if package_create_event.status != FalkorEventStatus.SYNCED:
                    return

                package_id = str(package_create_event.object_id)

                try:
                    document = self.falkor.document_get(
                        package_id, str(event.object_id))
                except HTTPError as e:
                    if e.response.status_code == 404:
                        self.falkor.document_create(package_id, event)
                    else:
                        raise e

                event.status = FalkorEventStatus.PROCESSING
                session.commit()

            event.status = FalkorEventStatus.SYNCED
            event.synced_at = datetime.now()
            session.commit()
        except Exception as e:
            log.exception(e)
            log.debug(e.response.json())
            event.status = FalkorEventStatus.FAILED
            session.commit()
        finally:
            session.close()
