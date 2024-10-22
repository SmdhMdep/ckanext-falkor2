import logging

import sqlalchemy as sa
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.model as ckan_model

from ckanext.falkor import client, auth, event_handler, model
from ckan.lib import jobs
from datetime import datetime

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
    engine: sa.engine.Engine
    event_handler: event_handler.EventHandler
    __initialised: bool

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

        self.event_handler = event_handler.EventHandler(self.falkor)
        self.sync()

    def sync(self):
        session: sa.orm.Session = ckan_model.meta.create_local_session()
        job = model.new_falkor_sync_job()
        try:
            model.insert_new_falkor_sync_job(session, job)

            packages = model.get_packages_without_create_events(session)
            for package in packages:
                self.event_handler.handle_package_create(
                    package_id=package.id,
                    metadata_created=package.metadata_created,
                    user_id="sync_job"
                )

            resources = model.get_resources_without_create_events(session)
            for resource in resources:
                self.event_handler.handle_resource_create(resource, "sync_job")

            pending_events = model.get_pending_events(session)
            for event in pending_events:
                if event.object_type == model.FalkorEventObjectType.PACKAGE \
                        and event.event_type == model.FalkorEventType.CREATE:
                    self.event_handler.handle_package_create(
                        package_id=event.object_id,
                        metadata_created=event.created_at,
                        user_id=event.user_id, event=event
                    )

            job.status = model.FalkorSyncJobStatus.FINISHED
        except Exception as e:
            log.exception(e, extra={"job_id": job.id})
            session.rollback()
            job.status = model.FalkorSyncJobStatus.FAILED
        finally:
            job.end = datetime.now()
            session.commit()
            session.close()

    # IResourceController

    def before_show(self, resource_dict):
        jobs.enqueue(
            event_handler.handle_read_event,
            [
                self.event_handler,
                resource_dict,
                get_user_id()
            ]
        )

        self.get_helpers()

    def notify(
            self,
            entity,
            operation=None
    ):
        if operation is None:
            return

        jobs.enqueue(
            event_handler.handle_modification_event,
            args=[
                self.event_handler,
                entity, operation, get_user_id()
            ]
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
