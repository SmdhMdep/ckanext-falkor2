import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import logging

import ckan.model as model
from ckan.common import config as ckanconfig

from ckan.lib.dictization import table_dictize
from ckan.model.domain_object import DomainObjectOperation

from ckanext.falkor import falkor_client, auth

log = logging.getLogger(__name__)


def get_config_value(config, key: str) -> str:
    value = config.get(key)
    if not value:
        raise Exception(f"{key} not present in configration")
    return value


class FalkorPlugin(plugins.SingletonPlugin):
    falkor: falkor_client.Falkor

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
        self.config = config
        endpoint = get_config_value(config, "ckanext.falkor.auth.endpoint")
        client_id = get_config_value(config, "ckanext.falkor.auth.client_id")
        client_secret = get_config_value(config, "ckanext.falkor.auth.client_secret")
        username = get_config_value(config, "ckanext.falkor.auth.username")
        password = get_config_value(config, "ckanext.falkor.auth.password")

        credentials = auth.Credentials(client_id, client_secret, username, password)
        auth_client = auth.Auth(
            credentials,
            endpoint,
        )

        tenant_id = get_config_value(config, "ckanext.falkor.tenant_id")
        core_api_url = get_config_value(config, "ckanext.falkor.core_api_url")
        admin_api_url = get_config_value(config, "ckanext.falkor.admin_api_url")

        self.falkor = falkor_client.Falkor(
            auth_client, tenant_id, core_api_url, admin_api_url
        )

    # IResourceController
    def before_show(self, resource_dict):
        self.falkor.document_read(resource_dict)
        self.get_helpers()

    # IDomainObjectNotification & #IResourceURLChange
    def notify(self, entity, operation=None):
        context = {"model": model, "ignore_auth": True, "defer_commit": True}

        if isinstance(entity, model.Resource):
            if operation == DomainObjectOperation.new:
                resource = table_dictize(entity, context)
                self.falkor.document_create(resource)

            elif operation == DomainObjectOperation.changed:
                resource = table_dictize(entity, context)
                self.falkor.document_update(resource)

            elif operation == DomainObjectOperation.deleted:
                resource = table_dictize(entity, context)
                self.falkor.document_delete(resource)
            else:
                return

        elif isinstance(entity, model.Package):
            if operation == DomainObjectOperation.new:
                resource = table_dictize(entity, context)
                self.falkor.dataset_create(resource)
            else:
                return

    def construct_falkor_url(self, resource):
        log.info(resource)
        resource_id = resource["id"]
        package_id = resource["package_id"]

        package_info = toolkit.get_action("package_show")(data_dict={"id": package_id})
        organisation_id = package_info["organization"]["id"]

        return f"http://192.168.66.1:8686/{organisation_id}/{package_id}/{resource_id}"

    def get_helpers(self):
        return {"construct_falkor_url": self.construct_falkor_url}
