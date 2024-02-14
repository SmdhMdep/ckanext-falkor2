from ckanext.falkor import auth
import ckan.lib.jobs as jobs
from typing import TypedDict
import requests
import logging
import json

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

    def document_read(self, context, resource):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + resource["package_id"]
            + "/"
            + resource["id"]
            + "/body?userId="
        )

        if "user_obj" not in context:
            url = url + "guest"
        else:
            url = url + context["user_obj"].id

        jobs.enqueue(falkor_get, [url, base_headers(self.__auth.access_token)])

    def document_create(self, resource: dict):
        url = (
            self.__core_base_url
            + self.__tenant_id
            + "/dataset/"
            + resource["package_id"]
            + "/create"
        )
        resource_id = str(resource["id"])
        payload = {"documentId": resource_id, "data": json.dumps(resource)}

        log.debug(f"Creating document with id {resource_id}")
        jobs.enqueue(
            falkor_post, [url, payload, base_headers(self.__auth.access_token)]
        )
