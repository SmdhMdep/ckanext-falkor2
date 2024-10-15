import logging

import sqlalchemy as sa
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.model as ckan_model

from ckan.lib.dictization import table_dictize
from ckanext.falkor import client, auth, event_handler
from ckan.model.domain_object import DomainObjectOperation


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

    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.ITemplateHelpers)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)
    # plugins.implements(plugins.IActions)

    # IConfigurer
    def update_config(self, config):
        toolkit.add_template_directory(config, "templates")
        toolkit.add_public_directory(config, "public")

    def configure(self, config):
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

    # IResourceController
    def before_show(self, resource_dict):
        # self.event_handler.handle_resource_read(
        #     resource_id=resource_dict["id"],
        #     package_id=resource_dict["package_id"],
        #     user_id=get_user_id()
        # )
        self.get_helpers()

    def notify(self, entity, operation=None):
        context = {
            "model": ckan_model,
            "ignore_auth": True,
            "defer_commit": True
        }
        if isinstance(entity, ckan_model.Package):
            package = table_dictize(entity, context)

            # We do not want to create datasets on Falkor that are still
            # in draft on CKAN.
            if package["state"] != "active":
                return

            self.event_handler.handle_package_create(
                package=package,
                user_id=get_user_id()
            )

        elif isinstance(entity, ckan_model.Resource):
            resource = table_dictize(entity, context)
            # self.handle_resource_modification_event(resource, operation)

    def handle_resource_modification_event(
            self,
            resource: dict,
            operation: DomainObjectOperation
    ):
        if operation == DomainObjectOperation.new:
            self.event_handler.handle_resource_create()
        elif operation == DomainObjectOperation.changed:
            self.event_handler.handle_resource_update()
        elif operation == DomainObjectOperation.deleted:
            self.event_handler.handle_resource_delete()

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

    # def handle_resource_read(self, resource_id: str, package_id: str):
    #     session = sa.orm.Session(bind=self.engine)
    #     try:
    #         event = new_falkor_event(
    #             id=uuid.uuid4(),
    #             object_id=resource_id,
    #             object_type="resource",
    #             status="pending",
    #             created_at=datetime.datetime.now(),
    #             synced_at=None,
    #         )
    #         session.add(event)
    #         session.commit()
    #
    #         self.falkor.document_read(
    #             package_id=resource_id,
    #             resource_id=package_id
    #         )
    #         log.info(session.query(FalkorEvent).all())
    #     except:
    #         session.rollback()
    #     finally:
    #         session.close()


# @toolkit.side_effect_free
# def hello_world(context, data_dict: Optional[dict] = None) -> str:
#     return {"message": f"Hello, {data_dict['name'] if 'name' in data_dict else 'World'}!"}
