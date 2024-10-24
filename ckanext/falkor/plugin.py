import logging
import re

from flask import request
from datetime import datetime
from ckan.lib import jobs

import sqlalchemy as sa
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.model as ckan_model
from ckan.model.domain_object import DomainObjectOperation

from ckanext.falkor import client, auth
from ckanext.falkor.model import (
    FalkorEvent,
    FalkorEventType,
    FalkorEventObjectType,
    FalkorSyncJobStatus,
    new_falkor_sync_job,
    get_pending_events,
    get_packages_without_create_events,
    get_resources_without_create_events,
    insert_new_falkor_sync_job
)
from ckanext.falkor.event_handler import (
    EventHandler,
    DomainObjectOperationToFalkorEventTypeMap
)

log = logging.getLogger(__name__)


def get_config_value(config, key: str) -> str:
    value = config.get(key)
    if not value:
        raise Exception(f"{key} not present in configration")
    return value


def get_user_id() -> str:
    user = toolkit.g.userobj
    return "guest" if not user else user.id


class FalkorPlugin(plugins.SingletonPlugin):
    falkor: client.Client
    event_handler: EventHandler

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)

    # IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, "templates")
        toolkit.add_public_directory(config, "public")

    def configure(self, config):
        # TODO: Check if plugins has been initialised before tracking events
        self.config = config
        endpoint = get_config_value(config, "ckanext.falkor.auth.endpoint")
        client_id = get_config_value(config, "ckanext.falkor.auth.client_id")
        client_secret = get_config_value(
            config,
            "ckanext.falkor.auth.client_secret"
        )
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

        self.falkor = client.Client(
            auth_client, tenant_id, core_api_url, admin_api_url
        )

        self.event_handler = EventHandler(self.falkor)

    def sync(self):
        session: sa.orm.Session = ckan_model.meta.create_local_session()
        job = new_falkor_sync_job()
        try:
            insert_new_falkor_sync_job(session, job)

            packages = get_packages_without_create_events(session)
            for package in packages:
                event = FalkorEvent(
                    object_id=package.id,
                    object_type=FalkorEventObjectType.PACKAGE,
                    event_type=FalkorEventType.CREATE,
                    user_id="sync_job",
                    created_at=package.metadata_created
                )
                jobs.enqueue(
                    self.event_handler.handle,
                    [event]
                )

            resources = get_resources_without_create_events(session)
            for resource in resources:
                event = FalkorEvent(
                    object_id=resource.id,
                    object_type=FalkorEventObjectType.RESOURCE,
                    event_type=FalkorEventType.CREATE,
                    user_id="sync_job",
                    created_at=resource.created
                )
                jobs.enqueue(
                    self.event_handler.handle,
                    [event]
                )

            pending_events = get_pending_events(session)
            for event in pending_events:
                jobs.enqueue(
                    self.event_handler.handle,
                    [event]
                )

            job.status = FalkorSyncJobStatus.FINISHED
        except Exception as e:
            log.exception(e, extra={"job_id": job.id})
            session.rollback()
            job.status = FalkorSyncJobStatus.FAILED
        finally:
            job.end = datetime.now()
            session.commit()
            session.close()

    # IResourceController

    def before_show(self, resource_dict):
        resource_id = resource_dict["id"]

        # TODO: See whether we should expand on this idea as we are currently
        # generating a lot of reads. For now use to reduce noise of READ events
        # during development.
        valid_url_pattern = re.compile(r'^.*?/dataset/[^/]+/resource/[^/]+/?$')

        log.debug(
            f"URL: {request.url}\nMatched: {valid_url_pattern.match(request.url)}")
        if not valid_url_pattern.match(request.url):
            return

        session = ckan_model.meta.create_local_session()
        resource = session.query(ckan_model.Resource).get(resource_id)

        event = FalkorEvent(
            object_id=resource_id,
            object_type=FalkorEventObjectType.RESOURCE,
            event_type=FalkorEventType.READ,
            user_id=get_user_id(),
            created_at=datetime.now()
        )

        jobs.enqueue(
            self.event_handler.handle,
            [event, resource]
        )

        self.get_helpers()

    def notify(
            self,
            entity,
            operation=None
    ):
        if operation is None:
            return

        event = FalkorEvent(
            object_id=entity.id,
            event_type=DomainObjectOperationToFalkorEventTypeMap[
                operation
            ],
            user_id=get_user_id(),
        )

        if isinstance(entity, ckan_model.Package):
            # Currently Falkor does not track changes to packages.
            # We only use the create event to create the dataset
            # and ignore any further changes.
            if event.event_type != FalkorEventType.CREATE:
                return

            event.object_type = FalkorEventObjectType.PACKAGE
            event.created_at = entity.metadata_created

        elif isinstance(entity, ckan_model.Resource):
            event.object_type = FalkorEventObjectType.RESOURCE
            if operation == DomainObjectOperation.new:
                event.created_at = entity.created
            elif operation == DomainObjectOperation.update:
                event.created_at = entity.last_modified
            else:
                event.created_at = datetime.now()
        else:
            return

        jobs.enqueue(
            self.event_handler.handle,
            args=[event, entity]
        )

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
