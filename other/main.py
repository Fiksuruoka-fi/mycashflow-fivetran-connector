import requests
import datetime
import pytz
import sys
from flask import jsonify


def _filter(data: list, updated_at) -> list:
    if not data or not updated_at or not isinstance(data[0], dict):
        return data
    return [d for d in data if (not d.get('updated_at')) or (d['updated_at'] >= updated_at)]


def _get_data(endpoint, params, api_username, api_key, updated_at, api_version='v1'):
    url = f'https://halparuoka.mycashflow.fi/api/{api_version}/{endpoint}'
    response = requests.get(url=url, params=params, auth=(api_username, api_key))
    assert 'next' not in response.json()['meta'], 'More than 1 page, increase page_size'
    return _filter(response.json()['data'], updated_at)


def handler(request):
    request_json = request.get_json()
    api_username = request_json['secrets']['api_username']
    api_key = request_json['secrets']['api_key']

    def get_data(endpoint, params, api_version='v1'):
        return _get_data(
            endpoint,
            params,
            api_username=api_username,
            api_key=api_key,
            updated_at=request_json['state'].get('updated_at'),
            api_version=api_version
        )

    tz = pytz.timezone('Europe/Helsinki')
    updated_at = datetime.datetime.now(tz=tz).replace(microsecond=0).isoformat()

    ret = {
        "state": {
            "updated_at": updated_at,
        },
        "insert": {
            'campaign': get_data('campaigns', {'page_size': 1000, 'expand': 'prices,visibilities'}, api_version='v0'),
            'version': get_data('versions', {}),
            'supplier': get_data('suppliers', {'page_size': 10000}),
            'shipping_method': get_data('shipping-methods', {}),
            'category': get_data('categories', {'page_size': 1000, 'expand': 'visibilities'})
        },
        "schema": {
            "campaign": {
                "primary_key": ["id"]
            },
            "version": {
                "primary_key": ["id"]
            },
            "supplier": {
                "primary_key": ["id"]
            },
            "shipping_method": {
                "primary_key": ["id"]
            },
            "category": {
                "primary_key": ["id"]
            }
        },
        "hasMore": False
    }
    try:
        assert sys.getsizeof(str(ret)) < 10 * 10 ** 7, 'data too long for response'
    except AssertionError:
        return 'Data too long for response (10MB limit)', 500
    return jsonify(ret), 200
