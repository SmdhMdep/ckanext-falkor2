import logging
import sqlalchemy as sa

from datetime import datetime

from ckanext.falkor.model import (
    FalkorEvent,
    FalkorEventType,
    FalkorEventStatus,
)

from ckan.model import meta
from ckan.model.domain_object import DomainObjectOperation

log = logging.getLogger(__name__)

DomainObjectOperationToFalkorEventTypeMap = {
    DomainObjectOperation.new: FalkorEventType.CREATE,
    DomainObjectOperation.changed: FalkorEventType.UPDATE,
    DomainObjectOperation.deleted: FalkorEventType.DELETE
}


def handle_event(event: FalkorEvent):
    session: sa.orm.Session = meta.create_local_session()
    session.add(event)
    session.commit()
    try:
        event.status = FalkorEventStatus.PROCESSING
        session.commit()

        event.status = FalkorEventStatus.SYNCED
        event.synced_at = datetime.now()
        session.commit()
    except Exception as e:
        log.exception(e)
        session.rollback()
        event.status = FalkorEventStatus.FAILED
        session.commit()
    finally:
        session.close()
