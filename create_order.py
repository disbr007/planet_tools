import argparse
import datetime
import os
import requests
from retrying import retry
import time

import geopandas as gpd

from lib import read_ids
from logging_utils.logging_utils import create_logger
from db_utils import Postgres
from order_utils import get_stereo_pairs, pairs_to_list, \
    create_order_request, place_order, poll_for_success, get_order_results

logger = create_logger(__name__, 'sh', 'INFO')

planet_db = 'sandwich-pool.planet'
scenes_onhand_tbl = 'scenes_onhand'
scene_id = 'id'

@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def count_concurrent_orders():
    # TODO: Move these to a config file
    orders_url = 'https://api.planet.com/compute/ops/stats/orders/v2'
    PLANET_API_KEY = os.getenv('PL_API_KEY')
    if not PLANET_API_KEY:
        logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')

    with requests.Session() as s:
        logger.debug('Authorizing using Planet API key...')
        s.auth = (PLANET_API_KEY, '')

        res = s.get(orders_url)
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
    ids_path = args.ids
    selection_path = args.selection
    orders_path = args.orders
    product_bundle = args.product_bundle
    remove_onhand = not args.do_not_remove_onhand
    dryrun = args.dryrun

    if ids_path:
        logger.info('Reading IDs from: {}'.format(ids_path))
        ids = list(set([x.strip() for x in open(ids_path, 'r')]))
        logger.info('IDs found: {:,}'.format(len(ids)))
    elif selection_path:
        logger.info('Reading IDs from selection: {}'.format(selection_path))
        ids = read_ids(selection_path, field='id')

    if remove_onhand:
        logger.info('Removing onhand IDs...')
        with Postgres(planet_db) as db:
            onhand = set(db.get_values(scenes_onhand_tbl, columns=[scene_id]))

        ids = list(set(ids) - onhand)
        logger.info('IDs remaining: {:,}'.format(len(ids)))

    # Limits are 500 ids per order, 80 concurrent orders
    max_concur = 80
    assets_per_order = 500
    ids_chunks = [ids[i:i + assets_per_order]
                  for i in range(0, len(ids), assets_per_order)]
    more_than_one = len(ids_chunks) > 1

    # Open file to write list of orders to
    orders_txt = open(orders_path, 'w')
    # Iterate over IDs in chunks, submitting each as an order
    for i, ids_chunk in enumerate(ids_chunks):
        if more_than_one:
            order_name = '{}_{}'.format(name, i)
        else:
            order_name = '{}'.format(name)
        order_request = create_order_request(order_name=order_name, ids=ids_chunk, product_bundle=product_bundle)
        order_submitted = False
        while order_submitted is False:
            concurrent_orders = count_concurrent_orders()
            logger.info('Concurrent orders: {}'.format(concurrent_orders))
            if concurrent_orders < max_concur:
                # Submit
                if not dryrun:
                    logger.info('Submitting order: {}'.format(order_name))
                    order_id, order_url = place_order(order_request=order_request)
                    orders_txt.write(order_id)
                    orders_txt.write('\n')
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

    orders_txt.close()

if __name__ == '__main__':
    # Choices
    all_bundle_types = ['panchromatic_dn', 'analytic_5b', 'basic_l1a_dn', 'pansharpened_udm2',
                        'basic_uncalibrated_dn', 'analytic_sr_udm2', 'basic_panchromatic_dn',
                        'uncalibrated_dn', 'basic_uncalibrated_dn_nitf', 'analytic_5b_udm2',
                        'basic_analytic', 'basic_uncalibrated_dn_udm2', 'panchromatic_dn_udm2',
                        'analytic_udm2', 'analytic', 'basic_analytic_nitf', 'uncalibrated_dn_udm2',
                        'basic_uncalibrated_dn_nitf_udm2', 'analytic_sr', 'pansharpened', 'panchromatic',
                        'basic_analytic_udm2', 'basic_analytic_nitf_udm2', 'basic_panchromatic', 'visual']

    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--order_name', type=str,
                        help='Name to attached to order.')
    parser.add_argument('--ids', type=os.path.abspath,
                        help='Path to text file of IDs to order. All other parameters ignored.')
    parser.add_argument('--selection', type=os.path.abspath,
                        help='Path to selection of footprints to order, by ID.')
    parser.add_argument('--product_bundle', type=str, default='basic_analytic_dn',
                        choices=all_bundle_types,
                        help='Assest types to include in order.')
    parser.add_argument('--orders', type=os.path.abspath, default=os.path.join(os.getcwd(), 'planet_orders.txt'),
                        help='Path to write order IDs to.')
    parser.add_argument('--do_not_remove_onhand', action='store_true',
                        help='On hand IDs are removed by default. Use this flag to not remove.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Create order request, but do not place.')

    args = parser.parse_args()

    main(args)

    logger.info('Done.')
