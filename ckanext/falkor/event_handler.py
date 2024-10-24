import logging
import sqlalchemy as sa

from datetime import datetime
from requests import HTTPError
from typing import List, Union

from ckanext.falkor.model import (
    FalkorEvent,
    FalkorEventType,
    FalkorEventStatus,
    FalkorEventObjectType,
    get_package_create_event_for_resource
)
from ckanext.falkor.client import Client

from ckan.model import meta, Package, Resource
from ckan.model.domain_object import DomainObjectOperation
import ckan.plugins.toolkit as toolkit

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

    def handle(self, event: FalkorEvent, entity: dict):
        session: sa.orm.Session = meta.create_local_session()
        session.add(event)
        session.commit()
        try:
            # TODO: Clean up nesting.
            if event.object_type == FalkorEventObjectType.PACKAGE:
                # TODO: Is there a way to avoid setting PROCESSING in both branches?
                event.status = FalkorEventStatus.PROCESSING
                session.commit()

                self.falkor.dataset_create(entity["id"])

            elif event.object_type == FalkorEventObjectType.RESOURCE:
                # TODO: Would it be better to check Falkor for the existence of the dataset instead?
                # Check Falkor for dataset, if not exists then fire create event if no create event already pending.
                package_create_event = get_package_create_event_for_resource(
                    session, entity["package_id"])

                # TODO: Add retry here in case resource was created shortly after
                # package and it is still processing.
                if package_create_event is None or package_create_event.status != FalkorEventStatus.SYNCED:
                    return

                event.status = FalkorEventStatus.PROCESSING
                session.commit()

                try:
                    document_events: List[dict] = self.falkor.document_get(
                        entity["package_id"],
                        entity["id"]
                    )

                    document_events.append({
                        "id": str(event.id),
                        "event_type": event.event_type,
                        "user_id": event.user_id,
                        "created_at": str(event.created_at),
                    })

                    self.falkor.document_update(
                        str(event.object_id),
                        entity["package_id"],
                        document_events
                    )
                except HTTPError as e:
                    if e.response.status_code == 404:
                        log.debug(entity)
                        package_info = toolkit.get_action("package_show")(
                            data_dict={"id": entity["package_id"]}
                        )
                        self.falkor.document_create(
                            entity["package_id"],
                            event,
                            package_info["organization"]["id"]
                        )
                    else:
                        raise e

            event.status = FalkorEventStatus.SYNCED
            event.synced_at = datetime.now()
            session.commit()
        except Exception as e:
            log.exception(e)
            event.status = FalkorEventStatus.FAILED
            session.commit()
        finally:
            session.close()
