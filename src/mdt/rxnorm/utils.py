import os
import urllib
from pathlib import Path
import importlib.resources as pkg_resources
import requests
from typing import Callable, Any

from . import sql


def json_extract(obj, key):
    """Recursively fetch values from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    values = extract(obj, arr, key)
    print(values)
    return values


def payload_constructor(base_url, params):
    # TODO: exception handling for params as dict

    params_str = urllib.parse.urlencode(params, safe=':+')
    payload = {
        'base_url': base_url,
        'params': params_str
    }

    # debug print out
    print("""Payload built with base URL: {0} and parameters: {1}""".format(base_url,params_str))

    return payload


def rxapi_get_requestor(request_dict):
    """Sends a GET request to either RxNorm or RxClass"""
    response = requests.get(
        request_dict['base_url'],
        params=request_dict['params']
    )

    # debug print out
    print("GET Request sent to URL: {0}".format(response.url))
    print("Response HTTP Code: {0}".format(response.status_code))

    # TODO: Add execption handling that can manage 200 responses with no JSON
    if response.status_code == 200:
        return response.json()


def get_dataset(
        dest: os.PathLike = Path.cwd(),
        handler: Callable[[Any], None] = None
):
    url = 'https://download.nlm.nih.gov/rxnorm/RxNorm_full_prescribe_current.zip'
    response = requests.get(url)
    if handler:
        return handler(response.content)
    (dest / url.split('/')[-1]).write_bytes(response.content)
    return response


def get_sql(file_name):
    meps_sql = pkg_resources.read_text(sql, file_name)
    return meps_sql
