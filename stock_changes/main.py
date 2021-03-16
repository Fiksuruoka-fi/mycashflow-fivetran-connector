import requests
from flask import jsonify


def get_data(updated_at, api_username, api_key) -> tuple:
    url = 'https://halparuoka.mycashflow.fi/api/v1/stock-changes'
    params = {
        'page_size': 10000,
        'sort': 'id-asc'
    }
    if updated_at:
        params['changed_at-from'] = updated_at
    response = requests.get(url, params=params, auth=(api_username, api_key))
    response.raise_for_status()
    data = response.json()['data']
    result = {'stock_change': data}
    if data:
        updated_at_new = data[-1]['changed_at']
    else:
        updated_at_new = updated_at
    hasmore = 'next' in response.json()['meta']
    return result, updated_at_new, hasmore


def handler(request):
    request_json = request.get_json()
    api_username = request_json['secrets']['api_username']
    api_key = request_json['secrets']['api_key']
    updated_at = request_json['state'].get('updated_at')
    result, updated_at_new, hasmore = get_data(updated_at, api_username, api_key)
    ret = {
        "state": {
            "updated_at": updated_at_new,
        },
        "insert": result,
        "schema": {
            "stock_change": {
                "primary_key": ["id"]
            }
        },
        "hasMore": hasmore
    }
    return jsonify(ret), 200
