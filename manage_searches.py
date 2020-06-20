from copy import deepcopy
import json
import requests
import os

import yaml

from logging_utils.logging_utils import  create_logger

logger = create_logger(__name__, 'sh', 'INFO')
# TODO: Fix saved searches config location
config = os.path.join('config', 'saved_searches.yaml')

def create_saved_search(search_filter, search_name,
                        saved_searches_config=config):
    """Creates a saved search on the Planet API and returns
    the search ID.

    search_filter: dict
        Planet API formatted search filter
        https://developers.planet.com/docs/data/searches-filtering/
    search_name: str
        Name to write to config file, used for looking up ID for
        subsequent searches.
    """

    # API URLs
    PLANET_URL = r'https://api.planet.com/data/v1'
    SEARCH_URL = '{}/searches'.format(PLANET_URL)

    PLANET_API_KEY = os.getenv('PL_API_KEY')
    if not PLANET_API_KEY:
        logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')

    with requests.Session() as s:
        s.auth = (PLANET_API_KEY, '')

        saved_search = \
            s.post(SEARCH_URL, json=search_filter)
        saved_search_id = saved_search.json()['id']

    with open(saved_searches_config, 'a') as src:
        src.write('{}: {}'.format(search_name, saved_search_id))

    return saved_search_id


def list_saved_searches():
    with open(config, 'r') as stream:
        all_searches = yaml.safe_load(stream)
        search_names = list(all_searches.keys())

    return search_names


def get_search_id(search_name):
    """Return the saved search ID for the given search name"""
    with open(config, 'r') as stream:
        all_searches = yaml.safe_load(stream)
        if search_name not in all_searches.keys()
            logger.error('Search name not found: {}'.format(search_name))
            search_id = None
        else:
            search_id = all_searches[search_name]

    return search_id
