"""
This code has not been tested in a while. It was originally used
to help with ordering IDs for xtrack stereo with two IDs, but has
not been used since moving to multilook stereo.
"""
# TODO: REMOVE?
import argparse
import os
from pathlib import Path
import requests
from retrying import retry
import sys
import time

import geopandas as gpd

from lib.logging_utils import create_logger
# from lib.db import Postgres
from lib.search import get_stereo_pairs, pairs_to_list, \
    create_order_request, place_order
import lib.constants as constants

logger = create_logger(__name__, 'sh', 'INFO')

# External modules
sys.path.append(str(Path(__file__).parent / '..'))
try:
    from db_utils.db import Postgres, generate_sql
except ImportError as e:
    logger.error('db_utils module not found. It should be adjacent to '
                 'the planet_tools directory. Path: \n{}'.format(sys.path))
    sys.exit()


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def count_concurrent_orders():
    # TODO: Move these to a config file
    # orders_url = 'https://api.planet.com/compute/ops/stats/orders/v2'
    PLANET_API_KEY = os.getenv(constants.PL_API_KEY)
    if not PLANET_API_KEY:
        logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')

    with requests.Session() as s:
        logger.error('Authorizing using Planet API key...')
        s.auth = (PLANET_API_KEY, '')

        res = s.get(constants.ORDERS_URL)
        if res.status_code == 200:
            order_statuses = res.json()['user']
            queued = order_statuses['queued_orders']
            running = order_statuses['running_orders']
            total = queued + running
        else:
            logger.error(res.status_code)
            logger.error(res.reason)
            raise Exception

    return total


def main(args):
    name = args.order_name
    # ids_path = args.ids
    aoi_path = args.aoi
    date_min = args.date_min
    date_max = args.date_max
    ovlp_min = args.overlap_min
    ovlp_max = args.overlap_max
    date_diff = args.date_diff
    view_angle_diff = args.view_angle_diff
    remove_onhand = not args.do_not_remove_onhand
    dryrun = args.dryrun

    # if ids_path:
    #     logger.info('Reading IDs from: {}'.format(ids_path))
    #     stereo_ids = list(set([x.strip() for x in open(ids_path, 'r')]))
    #     logger.info('IDs found: {:,}'.format(len(stereo_ids)))
    # else:
    if aoi_path:
        logger.info('Loading AOI: {}'.format(aoi_path))
        aoi = gpd.read_file(aoi_path)
        if aoi.crs != 'epsg:4326':
            aoi = aoi.to_crs('epsg:4326')
    else:
        aoi = None

    order_params = {"aoi": aoi,
                    "date_min": date_min,
                    "date_max": date_max,
                    "ins": "PS2",
                    "date_diff": date_diff,
                    "ovlp_perc_min": ovlp_min,
                    "ovlp_perc_max": ovlp_max,
                    "view_angle_diff": view_angle_diff,
                    "geom_col": "ovlp_geom"}

    logger.info('Finding stereopairs from stereo_candidates table...')
    stereo_pairs = get_stereo_pairs(**order_params)
    logger.info('Stereopairs found: {:,}'.format(len(stereo_pairs)))
    stereo_ids = pairs_to_list(stereo_pairs)
    logger.info('Unique scenes: {:,}'.format(len(stereo_ids)))

    logger.info('Removing onhand IDs...')
    if remove_onhand:
        with Postgres(constants.SANDIWCH_POOL_PLANET) as db:
            onhand = set(db.get_values(constants.SCENES_ONHAND, columns=[constants.ID]))

        stereo_ids = list(set(stereo_ids) - onhand)
        logger.info('IDs remaining: {:,}'.format(len(stereo_ids)))

    # Limits are 500 ids per order, 80 concurrent orders
    assets_per_order = 500
    max_concur = 80
    ids_chunks = [stereo_ids[i:i + assets_per_order]
                  for i in range(0, len(stereo_ids), assets_per_order)]

    for i, ids in enumerate(ids_chunks):
        order_name = '{}_{}'.format(name, i)
        order_request = create_order_request(order_name=order_name, ids=ids)
        order_submitted = False
        while order_submitted is False:
            concurrent_orders = count_concurrent_orders()
            logger.info('Concurrent orders: {}'.format(concurrent_orders))
            if concurrent_orders < max_concur:
                # Submit
                if not dryrun:
                    logger.info('Submitting order: {}'.format(order_name))
                    order_id, order_url = place_order(order_request=order_request)
                    logger.info('Order ID: {}'.format(order_id))
                    logger.info('Order URL: {}'.format(order_url))
                    # success = poll_for_success(order_id=order_id)
                    # if success:
                    #     logger.info("Order placed successfully.")
                    # else:
                    #     logger.warning("Order placement did not finish.")
                    order_submitted = True
                else:
                    logger.info('(dryrun) Order submitted: {}'.format(order_name))
                    order_submitted = True
            else:
                logger.info('Concurrent orders reached ({}) waiting...'.format(max_concur))
                time.sleep(10)

        # Avoid submitting too fast
        time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-n', '--order_name', type=str,
                        help='Name to attached to order.')
    # parser.add_argument('--ids', type=os.path.abspath,
    #                     help='Path to text file of IDs to order. All other parameters ignored.')
    parser.add_argument('--aoi', type=os.path.abspath,
                        help='AOI to restrict order to.')
    parser.add_argument('--date_min', type=str,
                        help='Earliest date to include.')
    parser.add_argument('--date_max', type=str,
                        help='Most recent date to include.')
    parser.add_argument('--date_diff', type=int,
                        help='Maximum date difference to include.')
    parser.add_argument('--view_angle_diff', type=float,
                        help='Minimum view angle difference to include.')
    parser.add_argument('--overlap_min', type=float,
                        help='Minimum overlap percent to include.')
    parser.add_argument('--overlap_max', type=float,
                        help='Maximum overlap percent to include.')
    parser.add_argument('--do_not_remove_onhand', action='store_true',
                        help='On hand IDs are removed by default. Use this flag to not remove.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Create order request, but do not place.')

    args = parser.parse_args()

    main(args)

    logger.info('Done.')
