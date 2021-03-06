import requests
import collections
import datetime
import pytz
import sys
from flask import jsonify


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_data(updated_at, next_page, api_username, api_key) -> tuple:
    result = {'product': [], 'stock_item': [], 'brand': []}
    url = 'https://halparuoka.mycashflow.fi/api/v1/products'
    hasmore = True
    while hasmore and sys.getsizeof(str(result)) < 5 * 10 ** 6:
        # 10MB response limit. mainly relevant in historical sync.
        params = {
            'page_size': 250,
            'page': next_page,
            'sort': 'id-asc',
            'expand': 'visibilities,category_links,brand,image_links,stock_item'
        }
        response = requests.get(url, params=params, auth=(api_username, api_key))
        data = response.json()['data']
        for d in data:
            if not updated_at or d['updated_at'] > updated_at:
                if d['stock_item']:
                    if not updated_at or d['stock_item']['updated_at'] > updated_at:
                        result['stock_item'].append(d['stock_item'])
                del d['stock_item']
                if d['brand']:  # empty list is returned if does not have a brand
                    if not updated_at or d['brand']['updated_at'] > updated_at:
                        result['brand'].append(d['brand'])
                del d['brand']
                result['product'].append(flatten(d))
        hasmore = 'next' in response.json()['meta']
        next_page += 1
    if not hasmore:
        # Last page, set page to 1 so that next time it starts from beginning
        next_page = 1
    return result, next_page, hasmore


def handler(request):
    request_json = request.get_json()
    api_username = request_json['secrets']['api_username']
    api_key = request_json['secrets']['api_key']

    # Latest completed full update process started at this timestamp.
    # Used for filtering objects to be inserted or updated
    updated_at = request_json['state'].get('updated_at')
    # Start page where last partial update left off or one in case of new full update or historical sync
    start_page = request_json['state'].get('page', 1)
    # Keep this in the state to assign correct timestamp to updated_at at the end of full update
    latest_full_update_started_at = request_json['state'].get('latest_full_update_started_at')
    tz = pytz.timezone('Europe/Helsinki')
    # Used to overwrite latest_full_update_started_at timestamp in state in case of first partial update in full update
    this_partial_update_started_at = datetime.datetime.now(tz=tz).replace(microsecond=0).isoformat()
    result, next_page, hasmore = get_data(updated_at, start_page, api_username, api_key)

    ret = {
        "state": {
            # first partial update updates latest_update_started_at timestampp &
            # last partial update updates updated_at timestamp
            # page keeps track page
            "updated_at":
                latest_full_update_started_at if not hasmore else updated_at,
            "latest_full_update_started_at":
                this_partial_update_started_at if start_page == 1 else latest_full_update_started_at,
            "page":
                next_page
        },
        "insert": result,
        "schema": {
            "product": {
                "primary_key": ["id"]
            },
            "stock_item": {
                "primary_key": ["id"]
            },
            "brand": {
                "primary_key": ["id"]
            }
        },
        "hasMore": hasmore
    }
    return jsonify(ret), 200
