import argparse
import datetime
import os

import geopandas as gpd

from logging_utils.logging_utils import create_logger
from order_utils import get_stereo_pairs, pairs_to_list, \
    create_order_request, place_order, poll_for_success, get_order_results

logger = create_logger(__name__, 'sh', 'INFO')


def main(args):
    name = args.order_name
    ids_path = args.ids
    aoi_path = args.aoi
    date_min = args.date_min
    date_max = args.date_max
    ovlp_min = args.overlap_min
    ovlp_max = args.overlap_max
    date_diff = args.date_diff
    view_angle_diff = args.view_angle_diff
    dryrun = args.dryrun

    if ids_path:
        logger.info('Reading IDs from: {}'.format(ids_path))
        stereo_ids = list(set([x.strip() for x in open(ids_path, 'r')]))
        logger.info('IDs found: {:,}'.format(len(stereo_ids)))
    else:
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
        logger.info('Stereopairs found: {}'.format(len(stereo_pairs)))
        stereo_ids = pairs_to_list(stereo_pairs)
        logger.info('Unique scenes: {}'.format(len(stereo_ids)))

    # Limit to 250 per request
    step = 250
    logger.debug('Chunking IDs into groups of {}...'.format(step))
    total = len(stereo_ids)
    for i in range(0, total, step):
        step_ids = stereo_ids[i: i+step]
        step_name = '{}_{}'.format(name, i)
        order_request = create_order_request(order_name=step_name, ids=step_ids)

        if not dryrun:
            logger.info('Placing order for IDs {} - {}'.format(i, i+step))
            order_id, order_url = place_order(order_request=order_request)
            logger.info('Order ID: {}'.format(order_id))
            logger.info('Order URL: {}'.format(order_url))

            success = poll_for_success(order_id=order_id)
            if success:
                logger.info("Order placed successfully.")
            else:
                logger.warning("Order placement did not finish.")

        # results = get_order_results(order_url=order_url)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-n', '--order_name', type=str,
                        help='Name to attached to order.')
    parser.add_argument('--ids', type=os.path.abspath,
                        help='Path to text file of IDs to order. All other parameters ignored.')
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
    parser.add_argument('--dryrun', action='store_true',
                        help='Create order request, but do not place.')

    args = parser.parse_args()

    main(args)

    logger.info('Done.')
