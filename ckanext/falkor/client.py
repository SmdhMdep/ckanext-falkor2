import requests
import logging
import json
import ckan.model as model

from typing import TypedDict
from ckanext.falkor import auth
from ckanext.falkor.model import FalkorEvent

log = logging.getLogger(__name__)

HttpHeaders = TypedDict(
    "HttpHeaders", {"Content-Type": str, "accept": str, "Authorization": str}
)


def base_headers(access_token: str) -> HttpHeaders:
    return {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": "Bearer " + access_token,
    }


def falkor_post(
        url: str,
        payload: dict,
        auth: auth.Auth,
) -> requests.Response:
    response = requests.post(url, headers=base_headers(
        auth.access_token), json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_put(
        url: str,
        payload: dict,
        auth: auth.Auth,
) -> requests.Response:
    response = requests.put(url, headers=base_headers(
        auth.access_token), json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_get(
    url: str,
    auth: auth.Auth,
) -> requests.Response:
    response = requests.get(url, headers=base_headers(
        auth.access_token), timeout=120)
    log.debug(response.json())
    return response


def falkor_delete(
        url: str,
        auth: auth.Auth,
) -> requests.Response:
    response = requests.delete(url, headers=base_headers(
        auth.access_token), timeout=120)
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
            "taggingEnabled": "false",
            "iotaEnabled": "false",
            "tokensEnabled": "false",
        }

        falkor_post(url, payload, self.__auth).raise_for_status()

    def document_get(self, package_id: str, resource_id: str):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + package_id
            + "/"
            + resource_id
            + "/body"
        )

        resp = falkor_get(url, self.__auth)
        resp.raise_for_status()
        return resp.json()

    def document_create(
        self,
        package_id: str,
        event: FalkorEvent,
        # organisation_id: str,
    ):

        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + package_id
            + "/create"
        )
        payload = {
            "documentId": str(event.object_id),
            "data": json.dumps([{
                "id": str(event.id),
                "event_type": event.event_type,
                "user_id": event.user_id,
                "created_at": str(event.created_at),
            }]),
            "documentMetadata": {
                # "organisation_id": organisation_id,
                "package_id": package_id,
                "resource_id": str(event.object_id),
            },
        }

        # log.debug(f"Creating document with id {resource.id}")
        falkor_post(url, payload, self.__auth).raise_for_status()

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
        #     falkor_put, [url, resource.as_dict(), self.__auth]
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
