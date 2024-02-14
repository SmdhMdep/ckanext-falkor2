import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import logging

import ckan.model as model

from ckan.plugins.toolkit import config

import ckan.lib.jobs as jobs
from ckan.lib.dictization import table_dictize
from ckan.model.domain_object import DomainObjectOperation

from ckanext.falkor import tasks2
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
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IConfigurable, inherit=True)

    # IConfigurer
    def update_config(self, config_):
        toolkit.add_template_directory(config_, "templates")
        toolkit.add_public_directory(config_, "public")
        toolkit.add_resource("fanstatic", "falkor")

    def configure(self, config):
        self.config = config
        # config_keys = [
        # "ckanext.falkor.auth.endpoint",
        # "ckanext.falkor.auth.client_id",
        # "ckanext.falkor.auth.client_secret",
        # "ckanext.falkor.auth.username",
        # "ckanext.falkor.auth.password",
        # ]
        tenant_id = get_config_value(config, "ckanext.falkor.tenant_id")
        core_api_url = get_config_value(config, "ckanext.falkor.core_api_url")
        admin_api_url = get_config_value(config, "ckanext.falkor.admin_api_url")

        credentials = auth.Credentials("falkor", "secret", "testuser", "password")
        endpoint = "http://192.168.66.1:38080/realms/byzgen-falkor/protocol/openid-connect/token"
        auth_client = auth.Auth(credentials, endpoint)

        self.falkor = falkor_client.Falkor(
            auth_client, tenant_id, core_api_url, admin_api_url
        )

    # IResourceController
    def before_show(self, resource_dict):
        context = {
            "model": model,
            "session": model.Session,
            "user": toolkit.g.user,
            "user_obj": toolkit.g.userobj,
        }

        # self.falkor.document_read(context, resource_dict)

    # IDomainObjectNotification & #IResourceURLChange
    def notify(self, entity, operation=None):
        context = {"model": model, "ignore_auth": True, "defer_commit": True}

        if isinstance(entity, model.Resource):
            if operation == DomainObjectOperation.new:
                topic = "resource/create"
                resource = table_dictize(entity, context)
                self.falkor.document_create(resource)

            elif operation == DomainObjectOperation.changed:
                topic = "resource/update"
                resource = table_dictize(entity, context)
                self.falkor.document_update(resource)

            elif operation == DomainObjectOperation.deleted:
                topic = "resource/delete"
                resource = table_dictize(entity, context)
                self.falkor.document_delete(resource)
            else:
                return

        elif isinstance(entity, model.Package):
            if operation == DomainObjectOperation.new:
                topic = "dataset/create"
                resource = table_dictize(entity, context)
                tasks2.datasetCreate(resource)
            else:
                return
