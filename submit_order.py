import argparse
import os

from lib.logging_utils import create_logger
from lib.order import submit_order

# TODO: order compressed?

logger = create_logger(__name__, 'sh', 'INFO')

if __name__ == '__main__':
    # Choices
    all_bundle_types = [
        'panchromatic_dn', 'analytic_5b', 'basic_l1a_dn', 'pansharpened_udm2',
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
                        help='Path to text file of IDs to order. All other '
                             'parameters ignored.')
    parser.add_argument('--selection', type=os.path.abspath,
                        help='Path to selection of footprints to order, by ID.')
    parser.add_argument('--product_bundle', type=str, default='basic_analytic_dn',
                        nargs='+',
                        choices=all_bundle_types,
                        help='Product bundle types to include in order.')
    parser.add_argument('--orders', type=os.path.abspath, default=os.path.join(os.getcwd(), 'planet_orders.txt'),
                        help='Path to write order IDs to.')
    parser.add_argument('--do_not_remove_onhand', action='store_true',
                        help='On hand IDs are removed by default. Use this flag to not remove.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Create order request, but do not place.')

    args = parser.parse_args()

    name = args.order_name
    ids_path = args.ids
    selection_path = args.selection
    orders_path = args.orders
    product_bundle = args.product_bundle
    remove_onhand = not args.do_not_remove_onhand
    dryrun = args.dryrun

    submit_order(name=name, ids_path=ids_path, selection_path=selection_path,
                 orders_path=orders_path, product_bundle=product_bundle,
                 remove_onhand=remove_onhand, dryrun=dryrun)

    logger.info('Done.')
