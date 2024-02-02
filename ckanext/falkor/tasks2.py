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

TenantID = 3

Bearer = "~~~~~~~~~~~~~~~~~~~~~~~~~~" 

baseurl = "https://test.falkor.byzgen.com/api/core/v0/"

# Base header constant
baseHeaders = {
            'Content-Type': 'application/json',
            "accept": "application/json",
            "Authorization": "Bearer " + Bearer
        }

# Send a post request to falkor
def falkorPost(url, payload, headers):
    response = requests.post(url, headers = headers,json = payload,timeout=2)
    return response

# Send a post request to falkor
def falkorPut(url, payload, headers):
    response = requests.put(url, headers = headers,json = payload,timeout=2)
    return response

# Send a get request to falkor
def falkorGet(url, headers):
    response = requests.get(url, headers = headers,timeout=2)
    return response

def documentCreation(resource):
    # Format data for falkor
    url = baseurl + TenantID +"/dataset/" + resource['package_id'] + "/create"
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
    url = baseurl + str(TenantID) +"/dataset/"+ resource['package_id'] + "/" + resource['id'] +"/body"
    payload = {
            'data': "name = " + resource['name']
            }

    #run async request
    jobs.enqueue(
        falkorPut,
        [url, payload, baseHeaders]
    )

def documentRead(context, resource):

    if context["user_obj"] == None:
        url = baseurl + str(TenantID) + resource['package_id'] + "/" + resource['id'] + "/body?userId=" + "guest"
    else:
        url = baseurl + str(TenantID) + resource['package_id'] + "/" + resource['id'] + "/body?userId=" + context["user_obj"].id

    #run async request
    #jobs.enqueue(
    #    falkorGett,
    #    [url, baseHeaders]
    #)

def datasetCreation(resource):
    # Format data for falkor
    url = baseurl + TenantID +"/dataset"
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