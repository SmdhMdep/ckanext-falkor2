import logging

import sqlalchemy as sa
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.falkor import client, auth, event_handler, model
from ckan.lib import jobs

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
        session = model.meta.Session
        model.validate_falkor_config(session)
        self.__initialised = model.get_falkor_config(
           session
        ).initialised

    @property
    def initialised(self):
        if not self.__initialised:
            # TODO: Can this be retrieved from redis?
            self.__initialised = model.get_falkor_config(
                model.meta.Session
            ).initialised
        return self.__initialised

    # IResourceController
    def before_show(self, resource_dict):
        if not self.initialised:
            return

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
        if not self.initialised:
            return

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
