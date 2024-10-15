import requests
import logging
import json
import ckan.lib.jobs as jobs
import ckan.plugins.toolkit as toolkit
import ckan.model as model

from typing import TypedDict
from ckanext.falkor import auth

log = logging.getLogger(__name__)

HttpHeaders = TypedDict(
    "HttpHeaders", {"Content-Type": str, "accept": str, "Authorization": str}
)


def base_headers(access_token: str, user_id: str) -> HttpHeaders:
    return {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": "Bearer " + access_token,
        "x-user": user_id,
    }


def falkor_post(url: str, payload: dict, auth: auth.Auth, user_id: str):
    response = requests.post(url, headers=base_headers(
        auth.access_token, user_id), json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_put(url: str, payload: dict, auth: auth.Auth, user_id: str):
    response = requests.put(url, headers=base_headers(
        auth.access_token, user_id), json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_get(url: str, auth: auth.Auth, user_id: str):
    response = requests.get(url, headers=base_headers(
        auth.access_token, user_id), timeout=120)
    log.debug(response.json())
    return response


def falkor_delete(url: str, auth: auth.Auth, user_id: str):
    response = requests.delete(url, headers=base_headers(
        auth.access_token, user_id), timeout=120)
    log.debug(response.json())
    return response


class Client:
    __auth: auth.Auth
    __core_base_url: str
    __admin_base_url: str
    __tenant_id: str

    def __init__(
        self,
        auth: auth.Auth,
        tenant_id: str,
        core_base_url: str,
        admin_base_url: str
    ):
        self.__auth = auth
        self.__tenant_id = tenant_id
        self.__core_base_url = core_base_url
        self.__admin_base_url = admin_base_url

    def dataset_create(self, package_id: str):
        url = self.__admin_base_url + self.__tenant_id + "/dataset"
        payload = {
            "datasetId": package_id,
            "encryptionType": "none",
            "externalStorage": "false",
            "permissionEnabled": "false",
            "taggingEnabled": "true",
            "linkedContract": "none",
            "iotaEnabled": "false",
            "tokensEnabled": "false",
        }

        # run async request
        log.debug(f"Create dataset with id {package_id}")
        # jobs.enqueue(
        #     falkor_post, [url, payload, self.__auth, get_user_id()]
        # )

    def document_read(self, package_id: str, resource_id: str):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + package_id
            + "/"
            + resource_id
            + "/body"
        )

        log.debug(f"Read for document with id {resource_id}")
        # jobs.enqueue(falkor_get, [url, self.__auth, get_user_id()])

    def document_create(
        self,
        resource: model.Resource,
        organisation_id: str,
    ):

        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + resource.package_id
            + "/create"
        )
        payload = {
            "documentId": resource.id,
            "data": json.dumps(resource.as_dict()),
            "tags": {
                "organisation_id": organisation_id,
                "package_id": resource.package_id,
                "resource_id": resource.id,
            },
        }

        log.debug(f"Creating document with id {resource.id}")
        # jobs.enqueue(
        #     falkor_post, [url, payload, self.__auth, get_user_id()]
        # )

    def document_update(self, resource: model.Resource):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + resource.package_id
            + "/"
            + resource.id
            + "/body"
        )

        log.debug(f"Updating document with id {resource.id}")
        # jobs.enqueue(
        #     falkor_put, [url, resource.as_dict(), self.__auth, get_user_id()]
        # )

    def document_delete(self, resource: model.Resource):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + resource.package_id
            + "/"
            + resource.id
        )

        log.debug(f"Deleting document with id {resource.id}")
        # jobs.enqueue(falkor_delete, [url, self.__auth, get_user_id()])
