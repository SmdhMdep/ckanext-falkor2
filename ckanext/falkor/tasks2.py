#updated verson of tasks

from urllib import response

import json
import requests
import ckan.model as models
import ckan.plugins.toolkit as toolkit
import ckan.lib.jobs as jobs
import ckanext.falkor

import os
import sys

import logging
log = logging.getLogger(__name__)

# Constants

TENANT_ID = 2001 
BEARER = '' 
CORE_BASE_URL = "http://192.168.66.1:8080/api/core/v0/"
ADMIN_BASE_URL = "http://192.168.66.1:8585/api/admin/v0/"

# Base header constant
baseHeaders = {
            'Content-Type': 'application/json',
            "accept": "application/json",
            "Authorization": "Bearer " + BEARER 
        }

# Send a post request to falkor
def falkorPost(url, payload, headers):
    response = requests.post(url, headers = headers,json = payload,timeout=120)
    log.debug(response)
    return response

# Send a post request to falkor
def falkorPut(url, payload, headers):
    response = requests.put(url, headers = headers,json = payload,timeout=120)
    return response

# Send a get request to falkor
def falkorGet(url, headers):
    response = requests.get(url, headers = headers,timeout=120)
    return response

def documentCreation(resource):
    # Format data for falkor
    url = CORE_BASE_URL + str(TENANT_ID) +"/dataset/" + resource['package_id'] + "/create"
    payload = {
            'documentId': resource['id'],
            'data': "name = " + resource['name']
            }

    #run async request
    jobs.enqueue(
        falkorPost,
        [url, payload, baseHeaders]
    )

# Cannot be used till falkor can deal with:
#   document UUIDS 
#   JSON document updates
def documentUpdate(resource):
    # Format data for falkor
    url = CORE_BASE_URL + str(TENANT_ID) +"/dataset/"+ resource['package_id'] + "/" + resource['id'] +"/body"
    payload = {
            'data': "name = " + resource['name']
            }

    #run async request
    jobs.enqueue(
        falkorPut,
        [url, payload, baseHeaders]
    )

def documentRead(context, resource):
    if "user_obj" not in context:
        url = CORE_BASE_URL + str(TENANT_ID) + resource['package_id'] + "/" + resource['id'] + "/body?userId=" + "guest"
    else:
        url = CORE_BASE_URL + str(TENANT_ID) + resource['package_id'] + "/" + resource['id'] + "/body?userId=" + context["user_obj"].id
    log.debug(url)
    #run async request
    jobs.enqueue(
       falkor_get,
       [url, baseHeaders]
    )

def datasetCreation(resource):
    # Format data for falkor
    url = ADMIN_BASE_URL + str(TENANT_ID) +"/dataset"
    log.debug(f'DATASET {resource["id"]}')
    payload = {
        'datasetId': str(resource['id']),
        "encryptionType": "none",
        "externalStorage": "false",
        "permissionEnabled": "false",
        "taggingEnabled": "false",
        "iotaEnabled": "false",
        "tokensEnabled": "false"
    }

    #run async request
    jobs.enqueue(
        falkorPost,
        [url, payload, baseHeaders]
    )