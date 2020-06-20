from copy import deepcopy
import json
import requests
import os

from pprint import pprint
# from planet import api
# from planet.api import filters
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from logging_utils.logging_utils import  create_logger
import search_config
import saved_search_utils

def p(data):
    print(json.dumps(data, indent=2))

def response2gdf(response):
    """Converts API response to a geodataframe."""
    features = response.json()[features_key]
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

    return gdf


def get_search_count(search_request):
    stats_req = deepcopy(search_request)
    buckets_key = 'buckets'
    count_key = 'count'

    stats_req["interval"] = "year"
    stats = session.post(STATS_URL, json=stats_req)
    logger.debug(stats.json()[buckets_key])
    total_count = sum(bucket[count_key] for bucket in stats.json()[buckets_key])

    return total_count


def get_features(saved_search_id):
    # TODO: Paralleize: https://developers.planet.com/docs/quickstart/best-practices-large-aois/
    master_footprints = gpd.GeoDataFrame()
    # 250 is max page size
    page_search_url = '{}/{}/results?_page_size={}'.format(SEARCH_URL, saved_search_id, 250)

    processed_count = 0
    process_next = True
    while process_next:
        # Process current page of responses (250 features)
        res = session.get(page_search_url)  # , json=search_request)
        if res.status_code != 200:
            logger.error('Error connecting to search API: {}'.format(page_search_url))
            logger.error('Status code: {}'.format(res.status_code))
            raise ConnectionError
        page_footprints = response2gdf(res)
        master_footprints = pd.concat([master_footprints, page_footprints])

        # Log progress
        processed_count += len(page_footprints)
        logger.debug('Processed features: {:,}'.format(processed_count))

        # Check for next page, continue processing if it exists
        next_page = '_next'
        page_search_url = res.json()['_links'].get(next_page)

        if not page_search_url:
            process_next = False

    return master_footprints


# ARGS
saved_search_name = 'planet_stereo'
out_shp = r'V:\pgc\data\scratch\jeff\projects\planet\scratch\fairbanks_features_2019.shp'

#### Constants
# API URLs
PLANET_URL = r'https://api.planet.com/data/v1'
STATS_URL = '{}/stats'.format(PLANET_URL)
QSEARCH_URL = '{}/quick-search'.format(PLANET_URL)
SEARCH_URL = '{}/searches'.format(PLANET_URL)

# Environmental variable
PL_API_KEY = 'PL_API_KEY'
# Redundant? Is three band just reduced 4-band or diff sensors?
dove_item_types = ['PSScene3Band']  # 'PSScene4Band'

# Coordinate system of features returned by Planet API
crs = 'epsg:4326'

# Response feature property keys
features_key = 'features'
id_key = 'id'
geometry_key = 'geometry'
type_key = 'type'
coords_key = 'coordinates'
properties_key = 'properties'
property_atts = ['acquired', 'anomalous_pixels', 'cloud_cover',
                 'columns', 'epsg_code', 'ground_control', 'gsd',
                 'instrument', 'item_type', 'origin_x', 'origin_y',
                 'pixel_resolution', 'provider', 'published',
                 'quality_category', 'rows', 'satellite_id',
                 'strip_id', 'sun_azimuth', 'sun_elevation',
                 'updated', 'view_angle']
# Response feature values
polygon_type = 'Polygon'
point_type = 'Point'


#### Set up
# Logging
logger = create_logger(__name__, 'sh', 'DEBUG')
# Get API key
PLANET_API_KEY = os.getenv(PL_API_KEY)
if not PLANET_API_KEY:
    logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')


# Start session
session = requests.session()
# Auth
session.auth = (PLANET_API_KEY, '')
# Test a request
r = session.get(PLANET_URL)
if not r.status_code == 200:
    logger.error('Error connecting to Planet Data API.')

# Get search request
# TODO: clean this file structure, etc. up. Return search dict with name?
sr = search_config.master_search
saved_search_id = saved_search_utils.get_search_id(saved_search_name)

# Get a total count for the given filters
total_count = get_search_count(search_request=sr)
logger.debug('Total count for search parameters: {:,}'.format(total_count))

# Perform requests to API to return features, which are converted to footprints in a geodataframe
master_footprints = get_features(saved_search_id=saved_search_id)

# TODO: best output format? stream directly to postgres DB?
master_footprints.to_file(out_shp)