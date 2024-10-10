import logging
import datetime
import uuid

from typing import Optional
from enum import Enum

import sqlalchemy as sa

from sqlalchemy.ext.declarative import declarative_base

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.model as model

from ckan.lib.dictization import table_dictize
from ckan.model.domain_object import DomainObjectOperation
from ckanext.falkor import falkor_client, auth

log = logging.getLogger(__name__)

Base = declarative_base(metadata=model.meta.metadata)


class FalkorEventObjectType(Enum):
    PACKAGE = 'package'
    RESOURCE = 'resource'


class FalkorEventStatus(Enum):
    PENDING = 'pending'
    FAILED = 'failed'
    SYNCED = 'synced'


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
    status = sa.Column(
        sa.Enum(FalkorEventStatus),
        default=FalkorEventStatus.PENDING
    )
    created_at = sa.Column(sa.DateTime, nullable=False)
    synced_at = sa.Column(sa.DateTime, nullable=False)


def new_falkor_event(
    id: uuid.UUID,
    object_id: uuid.UUID,
    object_type: FalkorEventObjectType,
    created_at: sa.DateTime,
    status: FalkorEventStatus = FalkorEventStatus.PENDING,
    synced_at: Optional[sa.DateTime] = None
) -> FalkorEvent:
    return FalkorEvent(
        id=id,
        object_id=object_id,
        object_type=object_type,
        status=status,
        created_at=created_at,
        synced_at=synced_at
    )


def get_config_value(config, key: str) -> str:
    value = config.get(key)
    if not value:
        raise Exception(f"{key} not present in configration")
    return value


class FalkorPlugin(plugins.SingletonPlugin):
    falkor: falkor_client.Falkor
    engine: sa.engine.Engine

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IActions)

    def get_actions(self):
        return {"hello_world": hello_world}

    # IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, "templates")
        toolkit.add_public_directory(config, "public")

    def configure(self, config):
        self.config = config
        endpoint = get_config_value(config, "ckanext.falkor.auth.endpoint")
        client_id = get_config_value(config, "ckanext.falkor.auth.client_id")
        client_secret = get_config_value(
            config, "ckanext.falkor.auth.client_secret")
        username = get_config_value(config, "ckanext.falkor.auth.username")
        password = get_config_value(config, "ckanext.falkor.auth.password")

        credentials = auth.Credentials(
            client_id, client_secret, username, password)
        auth_client = auth.Auth(
            credentials,
            endpoint,
        )

        tenant_id = get_config_value(config, "ckanext.falkor.tenant_id")
        core_api_url = get_config_value(config, "ckanext.falkor.core_api_url")
        admin_api_url = get_config_value(
            config, "ckanext.falkor.admin_api_url")
        self.audit_base_url = get_config_value(
            config, "ckanext.falkor.audit_base_url")

        self.falkor = falkor_client.Falkor(
            auth_client, tenant_id, core_api_url, admin_api_url
        )

        self.engine = model.meta.engine

    # IResourceController
    def before_show(self, resource_dict):
        self.handle_resource_read(
            resource_id=resource_dict["id"],
            package_id=resource_dict["package_id"]
        )
        self.get_helpers()

    # IDomainObjectNotification & #IResourceURLChange
    def notify(self, entity, operation=None):
        context = {"model": model, "ignore_auth": True, "defer_commit": True}
        if isinstance(entity, model.Resource):
            resource: model.Resource = entity
            if operation == DomainObjectOperation.new:
                package_info = toolkit.get_action("package_show")(
                    data_dict={"id": resource.package_id}
                )

                organisation_info = package_info["organization"]
                organisation_id = organisation_info["id"]

                self.falkor.document_create(resource, organisation_id)

            elif operation == DomainObjectOperation.changed:
                self.falkor.document_update(resource)

            elif operation == DomainObjectOperation.deleted:
                self.falkor.document_delete(resource)
            else:
                return

        elif isinstance(entity, model.Package):
            package = table_dictize(entity, context)

            if operation == DomainObjectOperation.new:
                package = table_dictize(entity, context)
                self.falkor.dataset_create(package)
            else:
                return

    def construct_falkor_url(self, resource):
        resource_id = resource["id"]
        resource_name = resource["name"]

        package_id = resource["package_id"]

        package_info = toolkit.get_action(
            "package_show")(data_dict={"id": package_id})
        package_name = package_info["name"]

        organisation_info = package_info["organization"]
        organisation_name = organisation_info["title"]

        url = f"{self.audit_base_url}{package_id}/{resource_id}"
        query = (
            f"?dataset_name={package_name}"
            f"&org_name={organisation_name}"
            f"&doc_name={resource_name}"
        )
        return url + query

    def get_helpers(self):
        return {"construct_falkor_url": self.construct_falkor_url}

    def handle_resource_read(self, resource_id: str, package_id: str):
        session = sa.orm.Session(bind=self.engine)
        try:
            event = new_falkor_event(
                id=uuid.uuid4(),
                object_id=resource_id,
                object_type="resource",
                status="pending",
                created_at=datetime.datetime.now(),
                synced_at=None,
            )
            session.add(event)
            session.commit()

            self.falkor.document_read(
                package_id=resource_id,
                resource_id=package_id
            )
            log.info(session.query(FalkorEvent).all())
        except:
            session.rollback()
        finally:
            session.close()


@toolkit.side_effect_free
def hello_world(context, data_dict: Optional[dict] = None) -> str:
    return {"message": f"Hello, {data_dict['name'] if 'name' in data_dict else 'World'}!"}


class EventHandler:
    falkor: falkor_client.Falkor
    engine: sa.engine.Engine

    def __init__(self, falkor: falkor_client.Falkor, engine: sa.engine.Engine):
        self.falkor = falkor
        self.engine = engine

    def handle_package_create(self):
        pass

    def handle_resource_create(self):
        pass

    def handle_resource_read(self):
        pass

    def handle_resource_update(self):
        pass

    def handle_resource_delete(self):
        pass

    def __insert_pending_event(
        self,
        event_id: uuid.UUID,
        object_id: uuid.UUID,
        object_type: FalkorEventObjectType,
        created_at: datetime.datetime
    ):
        session = sa.orm.Session(bind=self.engine)
        try:
            event = new_falkor_event(
                id=event_id,
                object_id=object_id,
                object_type=object_type,
                created_at=created_at,
            )
            session.add(event)
            session.commit()
        except Exception as e:
            logging.critical(e, exc_info=True)
            session.rollback()
        finally:
            session.close()
