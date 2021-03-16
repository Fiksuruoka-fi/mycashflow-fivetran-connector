import requests
from flask import jsonify
import datetime
import pytz


def _filter(data: list, updated_at) -> list:
    if not data or not updated_at or not isinstance(data[0], dict):
        return data
    return [d for d in data if (not d.get('updated_at')) or (d['updated_at'] >= updated_at)]


def get_data(updated_at, page, api_username, api_key) -> tuple:
    url = 'https://halparuoka.mycashflow.fi/api/v1/customers'
    params = {
        'page': page,
        'page_size': 500,
        'expand': 'customer_groups',
        'sort': 'id-asc'
    }
    r = requests.get(url, params=params, auth=(api_username, api_key))
    r.raise_for_status()
    data = r.json()['data']
    data = _filter(data, updated_at=updated_at)
    hasmore = 'next' in r.json()['meta']
    return {'registered_customer': data}, hasmore


def handler(request):
    """secrets in format {'api_username': '<>', 'api_key': '<>'} """
    request_json = request.get_json()
    api_username = request_json['secrets']['api_username']
    api_key = request_json['secrets']['api_key']
    updated_at = request_json['state'].get('updated_at')
    start_page = request_json['state'].get('page', 1)

    tz = pytz.timezone('Europe/Helsinki')
    this_partial_update_started_at = datetime.datetime.now(tz=tz).replace(microsecond=0).isoformat()
    latest_full_update_started_at = request_json['state'].get('latest_full_update_started_at')

    result, hasmore = get_data(updated_at, start_page, api_username, api_key)

    ret = {
        "state": {
            "updated_at": latest_full_update_started_at if not hasmore else updated_at,
            "latest_full_update_started_at": this_partial_update_started_at if start_page == 1 else latest_full_update_started_at,
            "page": 1 if not hasmore else start_page + 1
        },
        "insert": result,
        "schema": {
            "registered_customer": {
                "primary_key": ["id"]
            }
        },
        "hasMore": hasmore
    }
    return jsonify(ret), 200
