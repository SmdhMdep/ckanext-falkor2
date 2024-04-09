import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import logging

import ckan.model as model
from ckan.common import config as ckanconfig

from ckan.lib.dictization import table_dictize
from ckan.model.domain_object import DomainObjectOperation

from ckanext.falkor import falkor_client, auth

# from flask import Blueprint, render_template

log = logging.getLogger(__name__)


def get_config_value(config, key: str) -> str:
    value = config.get(key)
    if not value:
        raise Exception(f"{key} not present in configration")
    return value

# def render_audit():
#     u'''A simple view function'''
#     return render_template(u"falkor-audit.html")


class FalkorPlugin(plugins.SingletonPlugin):
    falkor: falkor_client.Falkor = None

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable, inherit=True)
    # plugins.implements(plugins.IBlueprint)
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

    def get_helpers(self):
        if self.falkor is None:
            self.configure(ckanconfig)
        return { 'get_audit_trail': self.falkor.document_audit_trail }

    # def get_blueprint(self):
    #     u'''Return a Flask Blueprint object to be registered by the app.'''
    #
    #     # Create Blueprint for plugin
    #     blueprint = Blueprint("test", __name__)
    #     blueprint.template_folder = u'templates'
    #
    #     # Add plugin url rules to Blueprint object
    #     blueprint.add_url_rule(u'/hello_plugin', u'hello_plugin', render_audit)
    #
    #     return blueprint
