import argparse
from copy import deepcopy
import json
import requests
import os

from pprint import pprint
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon

from logging_utils.logging_utils import  create_logger
import config.search_filter as search_filter
import manage_searches

# API URLs
PLANET_URL = r'https://api.planet.com/data/v1'
STATS_URL = '{}/stats'.format(PLANET_URL)
# QSEARCH_URL = '{}/quick-search'.format(PLANET_URL)
SEARCH_URL = '{}/searches'.format(PLANET_URL)
# Environmental variable
PL_API_KEY = 'PL_API_KEY'

# Logging
logger = create_logger(__name__, 'sh', 'DEBUG')
# Get API key
PLANET_API_KEY = os.getenv(PL_API_KEY)
if not PLANET_API_KEY:
    logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')


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

    return gdf


def get_features(session, saved_search_id):
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


def select_scenes(search_id, out_scenes):
    # Start session
    with requests.Session() as s:
        # Auth
        s.auth = (PLANET_API_KEY, '')
        # Test a request
        r = s.get(PLANET_URL)
        if not r.status_code == 200:
            logger.error('Error connecting to Planet Data API.')

        logger.info('Performing query using saved search ID: {}'.format(search_id))

        # Get a total count for the given filters
        # Get search request of passed search ID
        sr = manage_searches.get_saved_search(session=s, search_id=search_id)
        total_count = manage_searches.get_search_count(search_request=sr)
        logger.debug('Total count for search parameters: {:,}'.format(total_count))

        # Perform requests to API to return features, which are converted to footprints in a geodataframe
        master_footprints = get_features(session=s, saved_search_id=search_id)

        # TODO: best output format? stream directly to postgres DB?
        logger.info('Writing selected features to file: {}'.format(out_scenes))
        master_footprints.to_file(out_scenes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--search_id', type=str,
                        help='The ID of a previously created search. Use create_saved_search.py to do so.')
    parser.add_argument('-o', '--out_scenes', type=os.path.abspath,
                        help='Path to write selected scene footprints to.')

    args = parser.parse_args()

    search_id = args.search_id
    out_scenes = args.out_scenes
    
    select_scenes(search_id=search_id, out_scenes=out_scenes)
