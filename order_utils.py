import json
import requests
from requests.auth import HTTPBasicAuth
import pathlib
import os
import time

import pandas as pd
import geopandas as gpd

from db_utils import Postgres, generate_sql
from logging_utils.logging_utils import create_logger


# Constants
stereo_pair_cand = 'candidate_pairs'
fld_acq = 'acquired1'
fld_ins = 'instrument'
fld_ins1 = '{}1'.format(fld_ins)
fld_ins2 = '{}2'.format(fld_ins)
fld_date_diff = 'date_diff'
fld_view_angle_diff = 'view_angle_diff'
fld_ovlp_perc = 'ovlp_perc'
fld_geom = 'ovlp_geom'

logger = create_logger(__name__, 'sh', 'DEBUG')

ORDERS_URL = 'https://api.planet.com/compute/ops/orders/v2'
PLANET_API_KEY = os.getenv('PL_API_KEY')
if not PLANET_API_KEY:
    logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')
auth = HTTPBasicAuth(PLANET_API_KEY, '')

# AWS
aws_conf = os.path.join(os.path.dirname(__file__),'config', 'aws_creds.json')

aws_params = json.load(open(aws_conf))
aws_access_key_id = aws_params['aws_access_key_id']
aws_secret_access_key = aws_params['aws_secret_access_key']
aws_bucket = aws_params['aws_bucket']
aws_region = aws_params['aws_region']
aws_path_prefix = aws_params['aws_path_prefix']

# Check authorization
auth_resp = requests.get(ORDERS_URL, auth=auth)
logger.info('Authorizing Planet API Key....')
if auth_resp.status_code != 200:
    logger.error('Issue authorizing: {}'.format(auth_resp))
logger.info('Response: {}'.format(auth_resp))

headers = {'content-type': 'application/json'}


def check_where(where):
    if where:
        where += """ AND """
    else:
        where = ""

    return where


def intersect_aoi_where(aoi, geom_col):
    """Create a where statement for a PostGIS intersection between the geometry(s) in
    the aoi geodataframe and a PostGIS table with geometry in geom_col"""
    aoi_epsg = aoi.crs.to_epsg()
    aoi_wkts = [geom.wkt for geom in aoi.geometry]
    intersect_wheres = ["""ST_Intersects(ST_SetSRID('{}'::geometry, {}), {})""".format(wkt,
                                                                                       aoi_epsg,
                                                                                       geom_col)
                        for wkt in aoi_wkts]
    aoi_where = " AND ".join(intersect_wheres)

    return aoi_where


def stereo_pair_sql(aoi=None, date_min=None, date_max=None, ins=None,
                    date_diff=None, view_angle_diff=None,
                    ovlp_perc_min=None, ovlp_perc_max=None,
                    limit=None, orderby=False, orderby_asc=False,
                    geom_col=fld_geom, columns='*'):
    """Create SQL statment to select stereo pairs based on passed arguments."""
    where = ""
    if date_min:
        where = check_where(where)
        where += "{} >= '{}'".format(fld_acq, date_min)
    if date_max:
        where = check_where(where)
        where += "{} <= '{}'".format(fld_acq, date_max)
    if ins:
        where = check_where(where)
        where += "{0} = '{1}' AND {2} = '{1}'".format(fld_ins1, ins, fld_ins2)
    if date_diff:
        where = check_where(where)
        where += "{} <= {}".format(fld_date_diff, date_diff)
    if view_angle_diff:
        where = check_where(where)
        where += "{} >= {}".format(fld_view_angle_diff, view_angle_diff)
    if ovlp_perc_min:
        where = check_where(where)
        where += "{} >= {}".format(fld_ovlp_perc, ovlp_perc_min)
    if ovlp_perc_max:
        where = check_where(where)
        where += "{} >= {}".format(fld_ovlp_perc, ovlp_perc_max)
    if isinstance(aoi, gpd.GeoDataFrame):
        where = check_where(where)
        where += intersect_aoi_where(aoi, geom_col=geom_col)

    if columns != '*' and geom_col not in columns:
        columns.append(geom_col)

    sql = generate_sql(layer=stereo_pair_cand, columns=columns, where=where,
                       limit=limit, orderby=orderby, orderby_asc=orderby_asc)

    return sql


def get_stereo_pairs(**kwargs):
    """Load stereo pairs from DB"""
    sql = stereo_pair_sql(**kwargs)
    # Load records
    with Postgres() as db:
        results = gpd.GeoDataFrame.from_postgis(sql=sql, con=db.get_engine().connect(),
                                                geom_col='ovlp_geom', crs='epsg:4326')

    return results


def pairs_to_list(pairs_df, id1='id1', id2='id2'):
    """Take a dataframe containing two ID columns and return as single list of IDs."""
    out_list = []
    for i, row in pairs_df.iterrows():
        out_list.append(row[id1])
        out_list.append(row[id2])

    return out_list


def create_aws_delivery(aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        bucket=aws_bucket, aws_region=aws_region, path_prefix=aws_path_prefix):
    aws_delivery = {
        'amazon_s3': {
            'bucket': bucket,
            'aws_region': aws_region,
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'path_prefix': path_prefix
        }
    }

    return aws_delivery


def create_order_request(order_name, ids, delivery='aws'):
    """Create order from list of IDs"""
    order_request = {
        'name': order_name,
        'products': [
            {'item_ids': ids,
             'item_type': 'PSScene3Band',
             'product_bundle': 'basic_analystic'}
        ]
    }
    if delivery == 'aws':
        order_request.update(create_aws_delivery())
    return order_request


def place_order(order_request):
    response = requests.post(ORDERS_URL, data=json.dumps(order_request), auth=auth, headers=headers)
    logger.debug('Place order response: {}'.format(response))
    if response.status_code != 200:
        logger.error('Error placing order "{}": {}'.format(response))
    order_id = response.json()['id']
    logger.debug('Order ID: {}'.format(order_id))
    order_url = '{}/{}'.format(ORDERS_URL, order_id)

    return order_url


def cancel_order(order_url):
    order_id = order_url.split('/')[-1]
    # Get current status
    state = requests.get(order_url, auth=auth).json()['state']
    logger.debug('Order {} state: {}'.format(order_id, state))

    logger.debug('Cancelling order: {}'.format(order_id))
    requests.put(order_url, auth=auth)
    state = requests.get(order_url, auth=auth).json()['state']
    logger.debug('Order {} state: {}'.format(order_id, state))

    if state == 'cancelled':
        logger.info('Order {} successfully cancelled.'.format(order_id))
    else:
        logger.info('Order {} not cancelled, state: {}'.format(order_id, state))


def poll_for_success(order_url, num_checks=30, wait=10):
    check_count = 0
    finished = False
    while check_count < num_checks:
        check_count += 1
        response = requests.get(ORDERS_URL).json()
        state = response['state']
        end_states = ['success', 'failed', 'partial']
        if state in end_states:
            logger.info('Order finished. State: {}'.format(state))
            finshed = True
            break
        time.sleep(wait)

    if not finished:
        logger.info('Order did not reach an end state within {} checks with {}s waits.'.format(num_checks), wait)

    return finished


def get_order_results(order_url):
    response = requests.get(order_url).json()
    results = response['_links']['results']

    logger.debug('\n'.join([r['name'] for r in results]))

    return results


def download_results(results, overwrite=False):
    results_urls = [r['location'] for r in results]
    results_names = [r['name'] for r in results]

    logger.info('Items to download: {}'.format(len(results_urls)))

    # TODO: add tqdm?
    for url, name in zip(results_urls, results_names):
        # TODO: Create subdir for each ID
        path = pathlib.Path(os.path.join('data', name))

        if overwrite or not path.exists():
            logger.debug('Downloading {}: {}'.format(name, url))
            r = requests.get(url, allow_redirects=True)
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, 'wb') as dst:
                dst.write(r.content)
        else:
            logger.debug('File already exists, skipping: {} {}'.format(name, url))


def list_orders(state=None):
    # TODO: Check if there is a page limit on orders return
    params = {}
    if state:
        params = {'state': state}

    orders = requests.get(ORDERS_URL, params=state)['orders']

    for o in orders:
        logger.info('ID: {} Name: {} State: {}'.format(o['id'], o['name'], o['state']))

    return orders


# Args
# order_name = 'test_order'
# # aoi_p = r'V:\pgc\data\scratch\jeff\projects\planet\scratch\test_aoi.shp'
# aoi_p = r'C:\temp\test_aoi_fbks.shp'
# aoi = gpd.read_file(aoi_p)
# aoi = aoi.to_crs('epsg:4326')
#
# kwa = {'aoi':aoi, 'date_min': '2020-02-01',
#        'ins':'PS2', 'date_diff':10, 'ovlp_perc_min':0.50,
#        'view_angle_diff': 1.5, 'geom_col': 'ovlp_geom'}
#
#
# stereo_pairs = get_stereo_pairs(**kwa)
# stereo_ids = pairs_to_list(stereo_pairs)
# order_request = create_order_request(order_name=order_name, ids=stereo_ids)
# order_url = place_order(order_request=order_request)
# success = poll_for_success(order_url=order_url)
# if not success:
#     logger.warning('Order placement did not finish.')
# results = get_order_results(order_url=order_url)
# download_results(results=results)
