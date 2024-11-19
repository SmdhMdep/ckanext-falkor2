import requests
import logging

from typing import TypedDict
from ckanext.falkor import auth
from requests import HTTPError

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

    def dataset_exists(self, package_id: str) -> bool:
        url = self.__core_base_url + self.__tenant_id + "/dataset/" + package_id + "/info"
        try:
            falkor_get(url, self.__auth).raise_for_status()
            return True
        except HttpError as e:
            if e.response.status_code == 404:
                return False
            else:
                raise e

    def document_exists(self, package_id: str, resource_id: str) -> bool:
        url = self.__core_base_url + self.__tenant_id + \
            "/dataset/" + package_id + "/" + resource_id + "/info"
        try:
            falkor_get(url, self.__auth).raise_for_status()
            return True
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                raise e

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
        dataset_id: str,
        document_id: str,
        data: str,
        metadata: dict,
    ):

        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + dataset_id
            + "/create"
        )
        payload = {
            "documentId": document_id,
            "data": data,
            "documentMetadata": metadata,
        }

        falkor_post(url, payload, self.__auth).raise_for_status()

    def document_update(
            self,
            resource_id: str,
            package_id: str,
            data: str
    ):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + package_id
            + "/"
            + resource_id
            + "/body"
        )

        falkor_put(url, data, self.__auth).raise_for_status()
