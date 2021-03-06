import requests
import collections
from flask import jsonify
import datetime


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def add_time(timestamp: str, hours: int) -> str:
    if not timestamp:
        return timestamp
    d = datetime.datetime.fromisoformat(timestamp)
    return (d + datetime.timedelta(hours=hours)).isoformat()


def get_orders(updated_at, api_username, api_key) -> tuple:
    url = 'https://halparuoka.mycashflow.fi/api/v0/orders'
    params = {
        'page_size': 100,
        'sort': 'updated_at-asc',
        'expand': 'products,shipments,tax_summary,events,comments,payments'
    }
    if updated_at:
        params['updated_at-from'] = updated_at
    response = requests.get(url, params=params, auth=(api_username, api_key))
    if response.status_code == requests.codes.internal_server_error:
        # handles a bug in MCF
        return {'orders': [], 'order_row': [], 'shipment': []}, add_time(updated_at, 1), True
    response.raise_for_status()
    data = response.json()['data']
    result = {'orders': [], 'order_row': [], 'shipment': []}
    for d in data:
        result['order_row'] += [flatten(p) for p in d['products']]
        result['shipment'] += [flatten(s) for s in d['shipments']]
        del d['products']
        del d['shipments']
        result['orders'].append(flatten(d))
    if data:
        updated_at_new = data[-1]['updated_at']
    else:
        updated_at_new = updated_at
    hasmore = 'next' in response.json()['meta']
    return result, updated_at_new, hasmore


def handler(request):
    request_json = request.get_json()
    api_username = request_json['secrets']['api_username']
    api_key = request_json['secrets']['api_key']
    updated_at = request_json['state'].get('updated_at')
    result, updated_at_new, hasmore = get_orders(updated_at, api_username, api_key)
    ret = {
        "state": {
            "updated_at": updated_at_new,
        },
        "insert": result,
        "schema": {
            "orders": {
                "primary_key": ["id"]
            },
            "order_row": {
                "primary_key": ["id"]
            },
            "shipment": {
                "primary_key": ["id"]
            }
        },
        "hasMore": hasmore
    }
    return jsonify(ret), 200
