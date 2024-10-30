import logging
import re

from flask import request, Blueprint
from datetime import datetime
from ckan.lib import jobs

import sqlalchemy as sa
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.model as ckan_model
import ckan.lib.base as base
from ckan.model.domain_object import DomainObjectOperation
from ckan.lib.dictization import table_dictize

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
    insert_new_falkor_sync_job,
    get_dictized_entity,
    get_sync_job_history,
    get_failed_events
)
from ckanext.falkor.event_handler import (
    EventHandler,
    DomainObjectOperationToFalkorEventTypeMap
)

render = base.render

CONTEXT = {
    "model": ckan_model,
    "ignore_auth": True,
    "defer_commit": True
}

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
    blueprint: Blueprint

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.IBlueprint)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)

    # IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, "templates")
        toolkit.add_public_directory(config, "public")

        toolkit.add_ckan_admin_tab(
            config, "falkor_admin.admin_tab", "Falkor", icon="gavel")

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
        self.blueprint = Blueprint(u'falkor_admin', __name__)
        self.blueprint.add_url_rule(
            "/ckan-admin/falkor",
            view_func=self.admin_tab,
            methods=["GET"]
        )
        self.blueprint.add_url_rule(
            "/ckan-admin/falkor/sync",
            view_func=self.sync,
            methods=["POST"]
        )

    def get_blueprint(self):
        return self.blueprint

    def admin_tab(self):
        session: sa.orm.Session = ckan_model.meta.create_local_session()
        recent_job_limit = 10
        sync_jobs = get_sync_job_history(session, recent_job_limit)
        failed_events = get_failed_events(session)
        session.close()
        return render(
            "admin/base.html",
            extra_vars={
                "latest_job_run": sync_jobs[0].start if len(sync_jobs) else None,
                "sync_jobs": sync_jobs,
                "failed_events": failed_events
            }
        )

    def sync(self):
        # TODO: Verify user is sys admin
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
                    [event, table_dictize(package, CONTEXT)]
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
                    [event, table_dictize(resource, CONTEXT)]
                )

            pending_events = get_pending_events(session)
            for event in pending_events:
                entity = get_dictized_entity(
                    session,
                    CONTEXT,
                    str(event.object_id),
                    event.object_type
                )
                jobs.enqueue(
                    self.event_handler.handle,
                    [event, entity]
                )

            job.status = FalkorSyncJobStatus.FINISHED
            toolkit.h.flash_success("Sync job started")
        except Exception as e:
            log.exception(e, extra={"job_id": job.id})
            session.rollback()
            job.status = FalkorSyncJobStatus.FAILED
            toolkit.h.flash_error("There was an error starting the sync job")
        finally:
            job.end = datetime.now()
            session.commit()
            session.close()

        return toolkit.h.redirect_to(toolkit.h.url_for("falkor_admin.admin_tab"))

    # IResourceController

    def before_show(self, resource_dict):
        resource_id = resource_dict["id"]

        # TODO: See whether we should expand on this idea as we are currently
        # generating a lot of reads. For now use to reduce noise of READ events
        # during development.
        valid_url_pattern = re.compile(
            r'^.*?/dataset/[^/]+/resource/(?!new)[^/]+/?$')

        if not valid_url_pattern.match(request.url):
            return

        log.debug(resource_dict)

        event = FalkorEvent(
            object_id=resource_id,
            object_type=FalkorEventObjectType.RESOURCE,
            event_type=FalkorEventType.READ,
            user_id=get_user_id(),
            created_at=datetime.now()
        )

        jobs.enqueue(
            self.event_handler.handle,
            [event, resource_dict]
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
            args=[event, table_dictize(entity, CONTEXT)]
        )

    def construct_falkor_url(self, resource):
        resource_id = resource["id"]
        package_id = resource["package_id"]
        package_info = toolkit.get_action(
            "package_show")(data_dict={"id": package_id})
        org_id = package_info["organization"]["id"]

        return f"{self.audit_base_url}{org_id}/{package_id}/{resource_id}"

    def get_helpers(self):
        return {"construct_falkor_url": self.construct_falkor_url}
