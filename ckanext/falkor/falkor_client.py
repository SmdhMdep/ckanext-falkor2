from ckanext.falkor import auth
import ckan.lib.jobs as jobs
import ckan.plugins.toolkit as toolkit
from typing import TypedDict
import requests
import logging
import json
import datetime

log = logging.getLogger(__name__)

HttpHeaders = TypedDict(
    "HttpHeaders", {"Content-Type": str, "accept": str, "Authorization": str}
)


def base_headers(access_token: str) -> HttpHeaders:
    user = toolkit.g.userobj
    user_id = "guest" if not user else user.id
    return {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": "Bearer " + access_token,
        "x-user": user_id,
    }


def falkor_post(url, payload, headers):
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_put(url, payload, headers):
    response = requests.put(url, headers=headers, json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_get(url, headers):
    response = requests.get(url, headers=headers, timeout=120)
    log.debug(response.json())
    return response


def falkor_delete(url, headers):
    response = requests.delete(url, headers=headers, timeout=120)
    log.debug(response.json())
    return response


class Falkor:
    __auth: auth.Auth
    __core_base_url: str
    __admin_base_url: str
    __tenant_id: str

    def __init__(
        self, auth: auth.Auth, tenant_id: str, core_base_url: str, admin_base_url: str
    ):
        self.__auth = auth
        self.__tenant_id = tenant_id
        self.__core_base_url = core_base_url
        self.__admin_base_url = admin_base_url

    def dataset_create(self, resource):
        resource_id = str(resource["id"])
        url = self.__admin_base_url + self.__tenant_id + "/dataset"
        payload = {
            "datasetId": resource_id,
            "encryptionType": "none",
            "externalStorage": "false",
            "permissionEnabled": "false",
            "taggingEnabled": "true",
            "linkedContract": "none",
            "iotaEnabled": "false",
            "tokensEnabled": "false",
        }

        # run async request
        log.debug(f"Create dataset with id {resource_id}")
        jobs.enqueue(
            falkor_post, [url, payload, base_headers(self.__auth.access_token)]
        )

    def document_read(self, resource):
        resource_id = str(resource["id"])
        package_id = str(resource["package_id"])
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
        jobs.enqueue(falkor_get, [url, base_headers(self.__auth.access_token)])

    def document_create(
        self,
        resource: dict,
        organisation_id: str,
        package_id: str,
    ):
        resource_id = str(resource["id"])
        package_id = str(resource["package_id"])

        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + package_id
            + "/create"
        )
        payload = {
            "documentId": resource_id,
            "data": json.dumps(resource),
            "tags": {
                "organisation_id": organisation_id,
                "package_id": package_id,
                "resource_id": resource_id,
            },
        }

        log.debug(f"Creating document with id {resource_id}")
        jobs.enqueue(
            falkor_post, [url, payload, base_headers(self.__auth.access_token)]
        )

    def document_update(self, resource):
        resource_id = str(resource["id"])
        package_id = str(resource["package_id"])

        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + package_id
            + "/"
            + resource_id
            + "/body"
        )

        log.debug(f"Updating document with id {resource_id}")
        jobs.enqueue(
            falkor_put, [url, resource, base_headers(self.__auth.access_token)]
        )

    def document_delete(self, resource):
        resource_id = str(resource["id"])
        package_id = str(resource["package_id"])

        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + package_id
            + "/"
            + resource_id
        )

        log.debug(f"Deleting document with id {resource_id}")
        jobs.enqueue(falkor_delete, [url, base_headers(self.__auth.access_token)])
