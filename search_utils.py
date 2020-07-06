from copy import deepcopy
import json
import requests
import os
import sys
from pprint import pprint

import geopandas as gpd

from logging_utils.logging_utils import  create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# TODO: convert to search_session class (all fxns that take session)
# TODO: Add filter: id NOT IN [list of existing IDs]

# config = os.path.join('config', 'saved_searches.yaml')

# API URLs and key
PLANET_URL = r'https://api.planet.com/data/v1'
SEARCH_URL = '{}/searches'.format(PLANET_URL)
STATS_URL = '{}/stats'.format(PLANET_URL)
PLANET_API_KEY = os.getenv('PL_API_KEY')
if not PLANET_API_KEY:
    logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')

# To ensure passed filter type is valid
filter_types = ('DateRangeFilter', 'RangeFilter', 'UpdateFilter',
                'GeometryFilter', 'NumberInFilter', 'StringInFilter')


def vector2geometry(vector_file):
    """Convert vector file to geojson geometry"""
    gdf = gpd.read_file(vector_file)
    geom = gdf.geometry.values[0]
    gs = gpd.GeoSeries([geom])
    geometry = json.loads(gs.to_json())['features'][0]['geometry']

    return geometry


def split_filter_arg(filter_arg):
    """Convert string of : separated arguments to form a filter
    to a formated search_filter dict"""
    # logger.debug('Filter arg: {}'.format(filter_arg))
    filter_types = ('DateRangeFilter', 'RangeFilter', 'UpdateFilter',
                    'GeometryFilter', 'NumberInFilter', 'StringInFilter')
    filter_type = filter_arg[0]
    if filter_type not in filter_types:
        logger.error('Unrecognized filter type: {}'.format(filter_type))
        raise Exception
    if filter_type in ('DateRangeFilter', 'RangeFilter', 'UpdateFilter'):
        if len(filter_arg) != 4:
            logger.error('Unexpected number of arguments: {}'.format(len(filter_arg)))
            logger.error('Arguments: {}'.format(filter_arg))
            raise Exception
        # TODO: If DateRangeFilter, convert input to properly formatted date
        field_name = filter_arg[1]
        compare = filter_arg[2]
        value = filter_arg[3]
        if filter_type == 'RangeFilter':
            value = float(value)
        if filter_type == 'DateRangeFilter':
            # Format the date (yyyy-mm-dd) with time for API call
            value += "T00:00:00.000Z"
        if compare not in ('gte', 'gt', 'lte', 'lt'):
            logger.error('Unrecognized comparison argument: {}'.format(compare))
            raise Exception
        config = {compare: value}
    elif filter_type in ('NumberInFilter', 'StringInFilter'):
        field_name = filter_arg[1]
        config = filter_arg[2:]
    elif filter_type == 'GeometryFilter':
        if len(filter_arg) == 2:
            field_name = 'geometry'
            vector_file = filter_arg[1]
        elif len(filter_arg) == 3:
            field_name = filter_arg[1]
            vector_file = filter_arg[2]
        logger.debug('Loading geometry from vector file: {}'.format(vector_file))
        assert os.path.exists(vector_file)
        config = vector2geometry(vector_file=vector_file)

    else:
        field_name, config = filter_arg[1], filter_arg[2]

    search_filter = {
        "type": filter_type,
        "field_name": field_name,
        "config": config
        }
    # logger.debug('Created search filter:\nType: {}\nField: {}\nConfig: {}'.format(filter_type,
    #                                                                               field_name,
    #                                                                               config))
    return search_filter


def create_master_filter(search_filters):
    master_filter = {
        "type": "AndFilter",
        "config": search_filters
    }
    return master_filter


def create_search_request(name, item_types, search_filters):
    # logger.debug('Creating search request...')
    # Create a master filter - all filters applied as AND
    master_filter = create_master_filter(search_filters=search_filters)
    # Create search request json
    search_request = {
        "name": name,
        "item_types": item_types,
        "filter": master_filter
    }

    return search_request


def create_saved_search(search_request, overwrite_saved=False):
    """Creates a saved search on the Planet API and returns the search ID."""
    with requests.Session() as s:
        logger.debug('Authorizing using Planet API key...')
        s.auth = (PLANET_API_KEY, '')
        saved_searches = get_all_searches(s)

        search_name = search_request["name"]
        # Determine if a saved search with the provided name exists
        ids_with_same_name = [x for x in saved_searches.keys() if saved_searches[x]['name'] == search_name]

        if ids_with_same_name:
            logger.warning("Saved search with name '{}' already exists.".format(search_name))
            if overwrite_saved:
                logger.warning("Overwriting saved search with same name.")
                for overwrite_id in ids_with_same_name:
                    overwrite_url = '{}/{}'.format(SEARCH_URL, overwrite_id)
                    r = s.delete(overwrite_url)
                    if r.status_code == 204:
                        logger.debug('Deleted saved search: {}'.format(overwrite_id))
            else:
                logger.warning('Overwrite not specified, exiting')
                sys.exit()
        # Create new saved search
        saved_search = s.post(SEARCH_URL, json=search_request)
        logger.debug('Search creation request status: {}'.format(saved_search.status_code))
        if saved_search.status_code == 200:
            saved_search_id = saved_search.json()['id']
            logger.info('New search created successfully: {}'.format(saved_search_id))
        else:
            logger.error('Error creating new search.')
            saved_search_id = None

    return saved_search_id


def get_all_searches(session):
    logger.debug('Getting saved searches...')
    res = session.get(SEARCH_URL, params={"search_type": "saved"})
    searches = res.json()["searches"]

    saved_searches = dict()
    for se in searches:
        saved_searches[se["id"]] = {k: se[k] for k in se.keys()}

    logger.debug('Saved searches found: {}'.format(len(saved_searches.keys())))
    logger.debug('Saved search IDs:\n{}'.format('\n'.join(saved_searches.keys())))

    return saved_searches


def get_search_id(session, search_name):
    """Return the saved search ID for the given search name"""
    all_searches = get_all_searches(session)
    matches = [s for s in all_searches if s["name"]]
    search_id = all_searches[search_name]["id"]

    return search_id


def get_saved_search(session, search_id=None, search_name=None):
    all_searches = get_all_searches(session=session)
    if search_id:
        ss = all_searches[search_id]
    elif search_name:
        ss_id = get_search_id(session=session, search_name=search_name)
        ss = all_searches[search_id]
    else:
        logger.error('Must provide one of search_id or search_name')

    return ss


def delete_saved_search(session, search_name=None, search_id=None, dryrun=False):
    all_searches = get_all_searches(session)
    if search_id:
        if search_id in all_searches.keys():
            delete_id = search_id
            delete_url = "{}/{}".format(SEARCH_URL, delete_id)
    elif search_name:
        ids_names = [(s_id, all_searches[s_id]['name']) for s_id in all_searches
                     if all_searches[s_id]['name'] == search_name]
        if len(ids_names) == 0:
            logger.warning('No search with name {} found.'.format(search_name))
        elif len(ids_names) > 1:
            logger.warning('Multiple searches fouind with name {}\n{}'.format(search_name, ids_names))
        else:
            delete_id = ids_names[0]
        delete_url = "{}/{}".format(SEARCH_URL, delete_id)

    logger.debug('ID to delete: {}'.format(delete_id))

    if not dryrun:
        r = session.delete(delete_url)
        if r.status_code == 204:
            logger.info('Successfully deleted search.')


def get_search_count(search_request):
    sr_copy = deepcopy(search_request)
    name = sr_copy['name']
    stats_request = dict()
    stats_request['filter'] = sr_copy['filter']
    stats_request['item_types'] = sr_copy['item_types']
    stats_request["interval"] = "year"

    buckets_key = 'buckets'
    count_key = 'count'

    with requests.Session() as session:
        logger.debug('Authorizing using Planet API key...')
        session.auth = (PLANET_API_KEY, '')
        stats = session.post(STATS_URL, json=stats_request)
        if not str(stats.status_code).startswith('2'):
            logger.error('Error connecting to {} with request:\n{}'.format(STATS_URL, str(stats_request)))
        logger.debug(stats)

    total_count = sum(bucket[count_key] for bucket in stats.json()[buckets_key])
    logger.debug('Total count for search request "{}": {:,}'.format(name, total_count))

    return total_count
