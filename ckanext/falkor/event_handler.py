import logging
import sqlalchemy as sa

from datetime import datetime
from typing import List

from ckanext.falkor.model import (
    FalkorEvent,
    FalkorEventType,
    FalkorEventStatus,
    FalkorEventResourceType,
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

    def handle_event(self, event: FalkorEvent):
        session: sa.orm.Session = meta.create_local_session()
        session.add(event)
        try:
            event.status = FalkorEventStatus.PROCESSING
            session.commit()

            document_event = {
                "id": str(event.id),
                "event_type": event.event_type,
                "user_id": event.user_id,
                "user_email": event.user_email,
                "created_at": event.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            }

            metadata = {
                "org_id": event.org_id,
                "org_name": event.org_name,
                "package_id": event.package_id,
                "package_name": event.package_name,
                "resource_id": event.resource_id,
                "resource_name": event.resource_name,
                "resource_type": event.resource_type.value
            }

            package_id = event.package_id
            resource_id = event.resource_id

            if event.resource_type == FalkorEventResourceType.STREAM:
                document_event["user_id"] = event.user_email

                metadata["org_id"] = event.org_name
                metadata["package_id"] = event.package_name
                metadata["resource_id"] = event.resource_name

                package_id = event.package_name
                resource_id = event.resource_name

            if not self.falkor.dataset_exists(package_id):
                self.falkor.dataset_create(package_id)

            if not self.falkor.document_exists(package_id, resource_id):
                self.falkor.document_create(
                    package_id,
                    resource_id,
                    [document_event],
                    metadata
                )
            else:
                document_events: List[dict] = self.falkor.document_get(
                    package_id,
                    resource_id
                )

                if document_event in document_events:
                    log.warning(
                        f"[Event ID: {event.id}] Already synced to Falkor")
                    event.status = FalkorEventStatus.SYNCED
                    event.synced_at = datetime.now()
                    session.commit()
                    return

                document_events.append(document_event)

                self.falkor.document_update(
                    package_id,
                    resource_id,
                    document_events
                )

            event.status = FalkorEventStatus.SYNCED
            event.synced_at = datetime.now()
            session.commit()
        except Exception as e:
            log.exception(
                f"[Event ID: {event.id}] {e}")
            event.status = FalkorEventStatus.FAILED
            session.commit()
            raise e
        finally:
            session.close()
