import argparse
import datetime
from pathlib import Path
import platform
import os
import sys
import time

from lib.lib import read_ids, get_config, linux2win
from lib.logging_utils import create_logger, create_logfile_path
from lib.order import submit_order, poll_for_success
# from submit_order import submit_order
from lib.order import download_parallel
import lib.constants as constants

logger = create_logger(__name__, 'sh', 'INFO')

default_dst_parent = get_config(constants.DOWNLOAD_LOC)
if platform.system() == constants.WINDOWS:
    default_dst_parent = linux2win(default_dst_parent)

# DELIVERY
DELIVERY_OPTIONS = [constants.AWS, constants.ZIP]


def order_and_download(order_name, order_ids_path,
                       order_selection_path,
                       out_orders_list,
                       order_product_bundle,
                       remove_onhand=True,
                       order_only=False,
                       delivery=constants.ZIP,
                       zip_single_archive=False,
                       initial_wait=1200,
                       download_par_dir=default_dst_parent,
                       overwrite_downloads=False,
                       dl_orders=None,
                       dryrun=False):
    """Submit orders to Planet API with delivery to AWS. Selection will
    be chunked into groups of 500 IDs/order  Download order from AWS
    to download_par_dir with subdirectories for each chunk's order ID.
    TODO: Change to using kwargs for submit_order and download_parallel"""
    if not dl_orders:
        logger.info('Submitting orders...')
        order_ids = submit_order(name=order_name, ids_path=order_ids_path,
                                 selection_path=order_selection_path,
                                 product_bundle=order_product_bundle,
                                 orders_path=out_orders_list,
                                 remove_onhand=remove_onhand,
                                 delivery=delivery,
                                 zip_single_archive=zip_single_archive,
                                 dryrun=dryrun)
        logger.info('Waiting {:,} seconds before checking for orders '
                    'status...'.format(initial_wait))
        now = datetime.datetime.now()
        waiting_start = datetime.datetime.now()
        resume_time = waiting_start + datetime.timedelta(seconds=initial_wait)
        wait_interval = initial_wait / 10
        if dryrun:
            logger.info('DRYRUN - exiting.')
            sys.exit()
        while now < resume_time:
            now = datetime.datetime.now()
            logger.info('...waiting. {:,}s remain'.format(
                round((resume_time - now).total_seconds())))
            time.sleep(wait_interval)
    else:
        logger.info('Loading order IDs from file...')
        order_ids = read_ids(dl_orders)
        logger.info('Orders IDs: {}'.format(len(order_ids)))

    if not order_only:
        logger.info('Checking for ready orders...')
        download_parallel(order_ids, dst_par_dir=download_par_dir,
                          delivery=delivery,
                          overwrite=overwrite_downloads, dryrun=dryrun)

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
    order_args = parser.add_argument_group('ORDERING')
    download_args = parser.add_argument_group('DOWNLOADING')

    order_args.add_argument('-n', '--order_name', type=str,
                            help='Name to attached to order.')
    order_args.add_argument('--ids', type=os.path.abspath,
                            help='Path to text file of IDs to order. All other parameters ignored.')
    order_args.add_argument('--selection', type=os.path.abspath,
                            help='Path to selection of footprints to order, by ID.')
    order_args.add_argument('--product_bundle', type=str, default='basic_analytic_dn',
                            choices=all_bundle_types, metavar='', nargs='+',
                            help='Product bundle types to include in order, e.g.: basic_analytic')
    order_args.add_argument('--orders', type=os.path.abspath,
                            default=os.path.join(os.getcwd(), 'planet_orders.txt'),
                            help='Path to write order IDs to.')
    order_args.add_argument('--do_not_remove_onhand', action='store_true',
                            help='On hand IDs are removed by default. Use this flag to not remove.')
    order_args.add_argument('--delivery', choices=DELIVERY_OPTIONS, default=constants.ZIP,
                            help='Delivery method to use. Supported options: '
                                 '{}'.format(DELIVERY_OPTIONS))
    order_args.add_argument('--zip_single_archive', action='store_true',
                            help='If using {} delivery, use this flag to have the order delivered '
                                 'as a single zip archive.')
    order_args.add_argument('--order_only', action='store_true',
                            help='Place orders only - do not true to download.')
    download_args.add_argument('--initial_wait', type=int, default=600,
                               help='Initial period to wait before checking AWS for completed orders, in seconds.')
    download_args.add_argument('--download_orders', type=os.path.abspath,
                               help='Skip ordering and begin downloading all order IDs in the '
                                    'provided text file.')
    download_args.add_argument('-dpd', '--destination_parent_directory', type=os.path.abspath,
                                help="""Directory to download imagery to. Subdirectories for each 
                                order will be created here.""")
    # download_args.add_argument('-w', '--wait_max', type=int, default=3_600_000,
    #                            help='Maximum amount of time to wait for an order '
    #                                 'to arrive in AWS before skipping, in milliseconds.')
    download_args.add_argument('--overwrite', action='store_true',
                                help='Overwrite files in destination. Otherwise duplicates are skipped.')
    download_args.add_argument('-l', '--logfile', type=os.path.abspath,
                                help='Location to write log to.')

    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without downloading.')

    # TODO: left this so you can see how I was debugging
    # import sys
    # sys.argv = [
    #     __file__,
    #     # '-n', 'per_bundle',
    #     # '--selection', r'E:\disbr007\projects\planet\scratch\test_order2021mar11.shp',
    #     '--download_orders',
    #     r'E:\disbr007\projects\planet\scratch\test_order2021mar11_orders.txt',
    #     '-dpd',
    #     r'E:\disbr007\projects\planet\data',
    #     # '--zip_single_archive'
    # ]

    args = parser.parse_args()

    # Order args
    order_name = args.order_name
    order_ids_path = args.ids
    order_selection_path = args.selection
    out_orders_list = args.orders
    order_product_bundle = args.product_bundle
    remove_onhand = not args.do_not_remove_onhand
    delivery = args.delivery
    zip_single_archive = args.zip_single_archive
    order_only = args.order_only

    # Download args
    initial_wait = args.initial_wait
    download_orders = args.download_orders
    download_par_dir = args.destination_parent_directory
    # wait_max = args.wait_max
    overwrite_downloads = args.overwrite

    dryrun = args.dryrun
    logfile = args.logfile

    # Destination for downloads
    if not download_par_dir:
        dst_parent = Path(default_dst_parent)
    else:
        dst_parent = Path(download_par_dir)

    if not logfile:
        # TODO: Fix this
        logfile = os.path.join(os.getcwd(), 'order_and_download.log')

    # logfile = create_logfile_path(Path(__file__))

    logger = create_logger(__name__, 'fh', 'DEBUG', logfile)
    sublogger2 = create_logger('submit_order', 'fh', 'DEBUG', logfile)
    sublogger1 = create_logger('download_utils', 'fh', 'DEBUG', logfile)

    order_and_download(order_name=order_name,
                       order_ids_path=order_ids_path,
                       order_selection_path=order_selection_path,
                       out_orders_list=out_orders_list,
                       order_product_bundle=order_product_bundle,
                       remove_onhand=remove_onhand,
                       delivery=delivery,
                       zip_single_archive=zip_single_archive,
                       order_only=order_only,
                       initial_wait=initial_wait,
                       download_par_dir=dst_parent,
                       # wait_max=wait_max,
                       dl_orders=download_orders,
                       overwrite_downloads=overwrite_downloads,
                       dryrun=dryrun)

