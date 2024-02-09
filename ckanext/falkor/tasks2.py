# updated verson of tasks

from urllib import response

import json
import requests
import ckan.model as models
import ckan.plugins.toolkit as toolkit
import ckan.lib.jobs as jobs
import ckanext.falkor
from typing import Union
from ckanext.falkor import auth

import os
import sys

import logging

log = logging.getLogger(__name__)

# Constants
TENANT_ID = 2001
CORE_BASE_URL = "http://192.168.66.1:8080/api/core/v0/"
ADMIN_BASE_URL = "http://192.168.66.1:8585/api/admin/v0/"

credentials = auth.Credentials("falkor", "", "testuser", "")
endpoint = (
    "http://192.168.66.1:38080/realms/byzgen-falkor/protocol/openid-connect/token"
)
auth = auth.Auth(credentials, endpoint)


def base_headers() -> dict[str, str, str]:
    return {
        "Content-Type": "application/json",
        "accept": "application/json",
        "Authorization": "Bearer " + auth.access_token,
    }


# Send a post request to falkor
def falkorPost(url, payload, headers):
    response = requests.post(url, headers=headers, json=payload, timeout=120)
    log.debug(response.json())
    return response


# Send a post request to falkor
def falkorPut(url, payload, headers):
    response = requests.put(url, headers=headers, json=payload, timeout=120)
    log.debug(response.json())
    return response


# Send a get request to falkor
def falkorGet(url, headers):
    response = requests.get(url, headers=headers, timeout=120)
    log.debug(response.json())
    return response


def falkorDelete(url, headers):
    response = requests.delete(url, headers=headers, timeout=120)
    log.debug(response.json())
    return response


def documentCreate(resource):
    # Format data for falkor
    url = (
        CORE_BASE_URL
        + str(TENANT_ID)
        + "/dataset/"
        + resource["package_id"]
        + "/create"
    )
    payload = {"documentId": resource["id"], "data": json.dumps(resource)}

    log.debug(f'Creating document with id {str(resource["id"])}')
    # run async request
    jobs.enqueue(falkorPost, [url, payload, base_headers()])


# Cannot be used till falkor can deal with:
#   document UUIDS
#   JSON document updates
def documentUpdate(resource):
    # Format data for falkor
    url = (
        CORE_BASE_URL
        + str(TENANT_ID)
        + "/dataset/"
        + resource["package_id"]
        + "/"
        + resource["id"]
        + "/body"
    )
    payload = {"data": resource}
    log.debug(f'Updating document with id {str(resource["id"])}')
    # run async request
    jobs.enqueue(falkorPut, [url, payload, base_headers()])


def documentRead(context, resource):
    if "user_obj" not in context:
        url = (
            CORE_BASE_URL
            + str(TENANT_ID)
            + "/dataset/"
            + resource["package_id"]
            + "/"
            + resource["id"]
            + "/body?userId="
            + "guest"
        )
    else:
        url = (
            CORE_BASE_URL
            + str(TENANT_ID)
            + "/dataset/"
            + resource["package_id"]
            + "/"
            + resource["id"]
            + "/body?userId="
            + context["user_obj"].id
        )
    log.debug(url)
    # run async request
    jobs.enqueue(falkorGet, [url, base_headers()])


def documentDelete(resource):
    url = (
        CORE_BASE_URL
        + str(TENANT_ID)
        + "/dataset/"
        + resource["package_id"]
        + "/"
        + resource["id"]
    )
    # run async request
    log.debug(f'Deleting document with id {str(resource["id"])}')
    jobs.enqueue(falkorDelete, [url, base_headers()])


def datasetCreate(resource):
    # Format data for falkor
    url = ADMIN_BASE_URL + str(TENANT_ID) + "/dataset"
    payload = {
        "datasetId": str(resource["id"]),
        "encryptionType": "none",
        "externalStorage": "false",
        "permissionEnabled": "false",
        "taggingEnabled": "false",
        "iotaEnabled": "false",
        "tokensEnabled": "false",
    }

    # run async request
    log.debug(f'Create dataset with id {str(resource["id"])}')
    jobs.enqueue(falkorPost, [url, payload, base_headers()])
