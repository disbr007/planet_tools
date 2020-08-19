import argparse
from copy import deepcopy
import json
import requests
import os
from multiprocessing.dummy import Pool as ThreadPool
import threading
from retrying import retry
import sys

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from lib import write_gdf
from logging_utils.logging_utils import create_logger
from search_utils import get_saved_search, get_search_count
from db_utils import Postgres

# TODO: add get_search_area function to aid in managing quota
# TODO: add remove_ids function
logger = create_logger(__name__, 'sh', 'DEBUG')

# API URLs
PLANET_URL = r'https://api.planet.com/data/v1'
STATS_URL = '{}/stats'.format(PLANET_URL)
SEARCH_URL = '{}/searches'.format(PLANET_URL)
# Environmental variable
PL_API_KEY = 'PL_API_KEY'
# Get API key
PLANET_API_KEY = os.getenv(PL_API_KEY)

# Constants
fld_id = 'id'
srid = 4326

# Set up threading
thread_local = threading.local()


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.auth = (PLANET_API_KEY, '')
    return thread_local.session


def response2gdf(response):
    """Converts API response to a geodataframe."""
    # Coordinate system of features returned by Planet API
    crs = 'epsg:4326'
    # Response feature property keys
    features_key = 'features'
    id_key = 'id'
    geometry_key = 'geometry'
    type_key = 'type'
    coords_key = 'coordinates'
    properties_key = 'properties'
    # Response feature values
    polygon_type = 'Polygon'
    point_type = 'Point'
    # All properites provided in API reponse
    property_atts = ['acquired', 'anomalous_pixels', 'cloud_cover',
                     'columns', 'epsg_code', 'ground_control', 'gsd',
                     'instrument', 'item_type', 'origin_x', 'origin_y',
                     'pixel_resolution', 'provider', 'published',
                     'quality_category', 'rows', 'satellite_id',
                     'strip_id', 'sun_azimuth', 'sun_elevation',
                     'updated', 'view_angle']

    features = response.json()[features_key]
    # Format response in dictionary format supported by geopandas
    reform_feats = {att: [] for att in property_atts}
    reform_feats[id_key] = []
    reform_feats[geometry_key] = []

    for feat in features:
        # Get ID
        reform_feats[id_key].append(feat[id_key])
        # Get geometry as shapely object
        geom_type = feat[geometry_key][type_key]
        if geom_type == polygon_type:
            geometry = Polygon(feat[geometry_key][coords_key][0])
        elif geom_type == point_type:
            geometry = Point(feat[geometry_key][coords_key][0])
        reform_feats[geometry_key].append(geometry)
        # Get all properties
        for att in property_atts:
            try:
                reform_feats[att].append(feat[properties_key][att])
            except KeyError:
                reform_feats[att].append(None)

    gdf = gpd.GeoDataFrame(reform_feats, crs=crs)
    # gdf['acquired'] = pd.to_datetime(gdf['acquired'], format="%Y-%m-%dT%H%M%S%fZ")

    return gdf


def get_search_page_urls(saved_search_id):
    # TODO: Speed this up, bottle neck - is it possible to speed up?
    # TODO: How much longer would it take to just process each page...?
    # TODO: Rewrite as loop up to total number of scenes in search id
    def fetch_pages(search_url, all_pages):
        session = get_session()
        all_pages.append(search_url)
        res = session.get(search_url)
        if res.status_code != 200:
            logger.error('Error connecting to search API: {}'.format(first_page_url))
            logger.error('Status code: {}'.format(res.status_code))
            raise ConnectionError
        page = res.json()
        next_url = page['_links'].get('_next')
        if next_url:
            fetch_pages(next_url, all_pages)

    logger.debug('Getting page urls for search...')
    all_pages = []
    # 250 is max page size
    first_page_url = '{}/{}/results?_page_size={}'.format(SEARCH_URL, saved_search_id, 250)
    fetch_pages(first_page_url, all_pages=all_pages)
    logger.debug('Pages: {}'.format(len(all_pages)))

    return all_pages


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def process_page(page_url):
    session = get_session()
    res = session.get(page_url)
    if res.status_code == 429:
        logger.debug('Response: {} - rate limited - retrying...'.format(res.status_code))
        raise Exception("Rate limit error.")
    if res.status_code != 200:
        logger.error('Error connecting to search API: {}'.format(page_url))
        logger.error('Status code: {}'.format(res.status_code))
        raise ConnectionError

    gdf = response2gdf(response=res)

    return gdf


def get_features(saved_search_id):

    master_footprints = gpd.GeoDataFrame()

    all_pages = get_search_page_urls(saved_search_id=saved_search_id)

    threads = 1
    thread_pool = ThreadPool(threads)
    logger.debug('Getting features...')
    results = thread_pool.map(process_page, all_pages)

    master_footprints = pd.concat(results)

    return master_footprints


def select_scenes(search_id, dryrun=False):

    # Test a request
    session = get_session()
    r = session.get(PLANET_URL)
    if not r.status_code == 200:
        logger.error('Error connecting to Planet Data API.')

    logger.info('Performing query using saved search ID: {}'.format(search_id))

    # Get a total count for the given filters
    # Get search request of passed search ID
    sr = get_saved_search(session=session, search_id=search_id)
    sr_name = sr['name']
    total_count = get_search_count(search_request=sr)
    logger.info('Total count for search parameters: {:,}'.format(total_count))

    # Perform requests to API to return features, which are converted to footprints in a geodataframe
    master_footprints = gpd.GeoDataFrame()
    if not dryrun:
        master_footprints = get_features(saved_search_id=search_id)
    logger.info('Total features processed: {:,}'.format(len(master_footprints)))

    return master_footprints, sr_name


def write_scenes(scenes, out_name=None, out_path=None, out_dir=None):
    if out_dir:
        # Use search request name as output
        out_path = os.path.join(out_dir, '{}.geojson'.format(out_name))
    # TODO: Check if out_scenes exists (earlier) abort if not overwrite
    logger.info('Writing selected features to file: {}'.format(out_path))
    write_gdf(scenes, out_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="""Get the footprints of a previously created search.""")

    parser.add_argument('-i', '--search_id', type=str,
                        help='The ID of a previously created search. Use create_saved_search.py to do so.')
    parser.add_argument('-op', '--out_path', type=os.path.abspath,
                        help='Path to write selected scene footprints to.')
    parser.add_argument('-od', '--out_dir', type=os.path.abspath,
                        help="""Directory to write scenes footprint to -
                        the search request name will be used for the filename.""")
    parser.add_argument('--to_tbl', type=str,
                        help="""Insert search results into this table.""")
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions, but do not actually download or write anything.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Set logging level to DEBUG')

    args = parser.parse_args()

    search_id = args.search_id
    out_path = args.out_path
    out_dir = args.out_dir
    to_tbl = args.to_tbl
    dryrun = args.dryrun
    verbose = args.verbose

    # Logging
    if verbose:
        log_lvl = 'DEBUG'
    else:
        log_lvl = 'INFO'
    logger = create_logger(__name__, 'sh', log_lvl)

    if not PLANET_API_KEY:
        logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')

    scenes, search_name = select_scenes(search_id=search_id, dryrun=dryrun)
    if len(scenes) == 0:
        logger.warning('No scenes found. Exiting.')
        sys.exit()
    if any([out_path, out_dir]):
        if out_dir:
            write_scenes(scenes, out_name=search_name, out_dir=out_dir)
        else:
            write_scenes(scenes, out_path=out_path)

    if to_tbl:
        with Postgres('sandwich-pool.planet') as db:
            db.insert_new_records(scenes, table=to_tbl, unique_id=fld_id,
                                  date_cols=['acquired'], dryrun=dryrun)
