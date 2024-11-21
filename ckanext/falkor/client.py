import requests
import logging
import json

from typing import TypedDict
from ckanext.falkor import auth
from requests import HTTPError, Session
from requests.adapters import HTTPAdapter, Retry

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
        session: Session,
        url: str,
        payload: dict,
        auth: auth.Auth,
) -> requests.Response:
    response = session.post(url, headers=base_headers(
        auth.access_token), json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_put(
        session: Session,
        url: str,
        payload: dict,
        auth: auth.Auth,
) -> requests.Response:
    response = session.put(url, headers=base_headers(
        auth.access_token), json=payload, timeout=120)
    log.debug(response.json())
    return response


def falkor_get(
    session: Session,
    url: str,
    auth: auth.Auth,
) -> requests.Response:
    response = session.get(url, headers=base_headers(
        auth.access_token), timeout=120)
    log.debug(response.json())
    return response


def falkor_delete(
        session: Session,
        url: str,
        auth: auth.Auth,
) -> requests.Response:
    response = session.delete(url, headers=base_headers(
        auth.access_token), timeout=120)
    log.debug(response.json())
    return response


class Client:
    __auth: auth.Auth
    __core_base_url: str
    __admin_base_url: str
    __tenant_id: str
    __http_session: Session

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

        http_session = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])
        http_session.mount(
            self.__core_base_url, HTTPAdapter(max_retries=retries))
        http_session.mount(
            self.__admin_base_url, HTTPAdapter(max_retries=retries))

        self.__http_session = http_session

    def dataset_create(self, dataset_id: str):
        url = self.__admin_base_url + self.__tenant_id + "/dataset"
        payload = {
            "datasetId": dataset_id,
            "encryptionType": "none",
            "externalStorage": "false",
            "permissionEnabled": "false",
            "taggingEnabled": "false",
            "iotaEnabled": "false",
            "tokensEnabled": "false",
        }

        falkor_post(self.__http_session, url, payload,
                    self.__auth).raise_for_status()

    def dataset_exists(self, dataset_id: str) -> bool:
        url = self.__core_base_url + self.__tenant_id + "/dataset/" + dataset_id + "/info"
        try:
            falkor_get(self.__http_session, url,
                       self.__auth).raise_for_status()
            return True
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                log.exception(e)
                raise e

    def document_exists(self, dataset_id: str, document_id: str) -> bool:
        url = self.__core_base_url + self.__tenant_id + \
            "/dataset/" + dataset_id + "/" + document_id + "/info"
        try:
            falkor_get(self.__http_session, url,
                       self.__auth).raise_for_status()
            return True
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                log.exception(e)
                raise e

    def document_get(self, dataset_id: str, document_id: str):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + dataset_id
            + "/"
            + document_id
            + "/body"
        )

        resp = falkor_get(self.__http_session, url, self.__auth)
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
            "data": json.dumps(data),
            "documentMetadata": metadata,
        }
        log.debug(f"Creating document with payload:\n {payload}")

        falkor_post(self.__http_session, url, payload,
                    self.__auth).raise_for_status()

    def document_update(
            self,
            dataset_id: str,
            document_id: str,
            data: str
    ):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + dataset_id
            + "/"
            + document_id
            + "/body"
        )
        log.debug(f"Updating document with payload:\n {data}")

        falkor_put(self.__http_session, url, data,
                   self.__auth).raise_for_status()
