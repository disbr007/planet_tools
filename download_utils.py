import argparse
import datetime
from itertools import product
import json
from multiprocessing.dummy import Pool as ThreadPool
import os
from pathlib import Path
import pathlib
import time

from tqdm import tqdm
# from retrying import retry, RetryError
import boto3

from logging_utils.logging_utils import create_logger, create_logfile_path


# AWS Credentials, Bucket Info
aws_conf = os.path.join(os.path.dirname(__file__),"config", "aws_creds.json")
aws_params = json.load(open(aws_conf))
aws_access_key_id = aws_params["aws_access_key_id"]
aws_secret_access_key = aws_params["aws_secret_access_key"]
aws_bucket = aws_params["aws_bucket"]
aws_region = aws_params["aws_region"]
aws_path_prefix = aws_params["aws_path_prefix"]

bucket_name = 'pgc-data'
prefix = r'jeff/planet'

# TODO: Default DL location -> move to config file
default_dst_parent = r'V:\pgc\data\scratch\jeff\projects\planet\data'


logger = create_logger(__name__, 'sh', 'INFO')


# TODO FIX THIS - bucket should not load on module loading - how to pass best bucket around?
s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key, )
bucket = s3.Bucket(bucket_name)


def get_oids():
    """Get all immediate subdirectories of prefix in AWS -> these are Planet order ids."""
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key, )

    bucket = s3.Bucket(bucket_name)

    bucket_filter = bucket.objects.filter(Prefix=prefix)
    oids = set()
    logger.info('Getting order IDs...')
    for i, bo in enumerate(bucket_filter):
        key = Path(bo.key)
        oid = key.relative_to(Path(prefix)).parent.parent
        if str(oid) != '.':
            oids.add(str(oid))

    logger.info('Order IDs found: {}'.format(len(oids)))

    return oids


def manifest_exists(order_id, bucket=bucket):
    """
    Check if manifest for given order id exists
    """
    # Path to manifest for order
    mani_path = prefix / Path(order_id) / 'manifest.json'
    # Get files that match manifest path - should only be one
    mani_filter = bucket.objects.filter(Prefix=mani_path.as_posix())
    objs = list(mani_filter)
    if len(objs) >= 1 and objs[0].key == mani_path.as_posix():
        logger.debug('Manifest for {} exists.'.format(order_id))
        mani_exists = True
    else:
        mani_exists = False

    return mani_exists


# def non_dled_orders(oids_queue):
#     orders2dl = any([v for k, v in oids_queue.items()])
#
#     return orders2dl

# @retry(retry_on_result=non_dled_orders,
#        wait_exponential_multiplier=1000, wait_exponential_max=30000, stop_max_delay=3600000)
def dl_order(oid, dst_par_dir, overwrite=False, dryrun=False):
    # TODO: Be sure of the type being passed
    if not isinstance(dst_par_dir, pathlib.PurePath):
        dst_dir = Path(dst_par_dir)

    logger.info('Downloading order: {}'.format(oid))

    # Filter the bucket for the order id
    order_prefix = '{}/{}'.format(prefix, oid)
    bucket_filter = bucket.objects.filter(Prefix=order_prefix)

    # For setting progress bar length
    item_count = len([x for x in bucket.objects.filter(Prefix=order_prefix)])
    # For aligning arrows in progress bar writing
    arrow_loc = max([len(x.key) for x in bucket_filter]) - len(order_prefix)
    # Set up progress bar
    pbar = tqdm(bucket_filter, total=item_count, desc='Order: {}'.format(oid), position=1)

    logger.info('Downloading {:,} files to: {}'.format(item_count, dst_par_dir))
    for bo in pbar:
        # Determine source and destination full paths
        aws_loc = Path(bo.key)
        # Create path with order id subdirectory
        oid_dir = dst_par_dir / aws_loc.relative_to(Path(prefix))
        if os.path.exists(oid_dir) and not overwrite:
            logger.debug('File exists at destination, skipping: {}'.format(oid_dir))
            continue

        if not os.path.exists(oid_dir.parent):
            os.makedirs(oid_dir.parent)

        # Download
        logger.debug('Downloading file: {}\n\t--> {}'.format(aws_loc, oid_dir))
        # pbar.write('Downloading: {}{}-> {}'.format(aws_loc.relative_to(*aws_loc.parts[:3]),
        #                                            ' '*(arrow_loc - len(str(aws_loc.relative_to(*aws_loc.parts[:3])))),
        #                                            oid_dir.relative_to(*dst_par_dir.parts[:9])))
        dl_issues = set()
        if not dryrun:
            try:
                bucket.download_file(bo.key, oid_dir.absolute().as_posix())
                dl_issues.add(False)
            except Exception as e:
                logger.error('Error downloading: {}'.format(aws_loc))
                logger.error(e)
                dl_issues.add(True)

    logger.info('Done.')

    all_success = any(dl_issues)

    return all_success


def dl_order_when_ready(order_id, dst_par_dir, overwrite=False, dryrun=False,
                        wait_start=2, wait_increase_exp=2,
                        wait_max_interval=8, wait_max=3600):
    logger.info('Waiting for orders to arrive in AWS...')
    start_time = datetime.datetime.now()
    running_time = (datetime.datetime.now() - start_time).total_seconds()
    wait = wait_start
    start_dl = False
    while running_time < wait_max and not start_dl:
        exists = manifest_exists(order_id)
        if not exists:
            running_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.debug('Manifest not present for order ID: {} :'
                         ' {}s remaining'.format(order_id, round(wait_max-running_time)))
            time.sleep(wait)
            if wait <= wait_max_interval:
                wait = wait**wait_increase_exp
            if wait > wait_max_interval:
                wait = wait_max_interval

        else:
            logger.debug('Manifest present - beginning download: {}'.format(order_id))
            start_dl = True

    if start_dl:
        logger.info('Started downloading: {}'.format(order_id))
        all_success = dl_order(order_id, dst_par_dir=dst_par_dir, overwrite=overwrite, dryrun=dryrun)
    else:
        logger.info('Maximum wait reached, did not begin download: {}'.format(order_id))

    return order_id, start_dl, all_success


def download_parallel(order_ids, dst_par_dir, overwrite=False, dryrun=False, threads=4, wait_max=3600):
    pool = ThreadPool(threads)
    results = pool.starmap(dl_order_when_ready, product(order_ids, [dst_par_dir],
                                                        [dryrun], [overwrite]))
    pool.close()
    pool.join()

    logger.info('Download statuses:\nOrder ID\t\tStarted\tSuccess\n{}'.format(
        '\n'.join(["{} {}\t{}".format(oid, start_dl, status) for oid, start_dl, status in results])
    ))

    return results


# def download_orders(order_ids, dst_par_dir, dryrun=False, overwrite=False):
#     logger.info('Order IDs to download:\n{}'.format('\n'.join([str(o) for o in order_ids])))
#     pbar = tqdm(order_ids, desc='Order ', position=0)
#     for i, oid in enumerate(pbar):
#         pbar.set_description('Order: {}/{}'.format(i+1, len(order_ids)))
#         dl_order(dst_dir=dst_par_dir, bucket_name=bucket_name,
#                  dryrun=dryrun, overwrite=overwrite)
#         logger.debug('Order downloaded: {}'.format(oid))
#         logger.info('\n\n')
#
#     logger.info('Done')


# @retry(retry_on_result=non_dled_orders,
#        wait_exponential_multiplier=1000, wait_exponential_max=30000, stop_max_delay=3600000)
# def download_ready_orders(oids_queue, dst_par_dir, dryrun=False):
#     logger.debug('Downloading ready orders...')
#     logger.debug(oids_queue)
#     for oid, dl_started in oids_queue.items():
#         if not dl_started:
#             # if not already downloaded, check for manifest
#             ready = manifest_exists(oid, bucket=bucket)
#             if ready:
#                 logger.debug('Order ID ready to download: {}'.format(oid))
#                 if not dryrun:
#                     order_prefix = '{}/{}'.format(prefix, oid)
#                     # Download
#                     logger.info('Downloading ...')
#                     dl_order(dst_dir=dst_par_dir, order_prefix=order_prefix, dryrun=dryrun)
#                     oids_queue[oid] = True
#             else:
#                 logger.debug('Order ID NOT ready for download: {}'.format(oid))
#                 print('Order ID NOT ready for download: {}'.format(oid))
#                 if dryrun:
#                     logger.debug('(dryrun) - Marking OID as downloaded: {}'.format(oid))
#                     oids_queue[oid] = True
#
#     logger.debug('Orders remaining to download: {}'.format(len([v for k, v in oids_queue.items() if not v])))
#
#     return oids_queue
#
#
# def download_aws(oids, dst_par_dir=default_dst_parent, dryrun=False):
#     oids_queue = {oid: False for oid in oids}
#     try:
#         logger.info('Downloading ready orders and waiting for not ready...')
#         download_ready_orders(oids_queue, dst_par_dir=dst_par_dir, dryrun=dryrun)
#     except RetryError as retry_error:
#         # This error is raised when @retry hits its max number of retries
#         logger.error(retry_error)
#         logger.warning('All orders did not complete delivery to AWS in allocated time.')
#
#     if dryrun:
#         # Turn all order status' back to False
#         oids_queue = {oid: False for oid, v in oids_queue.items()}
#
#
#
#     dl_started_oids = [oid for oid, dl_started in oids_queue.items() if dl_started]
#     dl_not_started_oids = [oid for oid, dl_started in oids_queue.items() if not dl_started]
#
#     logger.info('Started orders:\n{}'.format('\n'.join(dl_started_oids)))
#     logger.info('NOT Started orders:\n{}'.format('\n'.join(dl_not_started_oids)))
    # for oid in dl_started_oids:
    #     logger.info('Download started for order ID: {}'.format(oid))
    # logger.info('+{}+'.format('-'*65))
    # for oid in dl_not_started_oids:
    #     logger.info('Download NOT started for ID: {}'.format(oid))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    mut_exc = parser.add_mutually_exclusive_group()
    mut_exc.add_argument('-all', '--all_available', action='store_true',
                        help='Download all available order IDs. Overrides -oid arguments')
    mut_exc.add_argument('-oid', '--order_ids', type=str, nargs='+',
                        help='Order ID(s) to download.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without downloading.')
    parser.add_argument('-dpd', '--destination_parent_directory', type=os.path.abspath,
                        help='Directory to download imagery to. Subdirectories for each order will be created here.')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite files in destination. Otherwise duplicates are skipped.')
    # parser.add_argument('-l', '--logfile', type=os.path.abspath,
    #                     help='Location to wrtie log to.')

    args = parser.parse_args()

    all_available = args.all_available
    order_ids = args.order_ids
    dst_par_dir = args.destination_parent_directory
    dryrun = args.dryrun
    overwrite = args.overwrite
    logfile = args.logfile

    # Destination
    if not dst_par_dir:
        dst_parent = Path(default_dst_parent)
    else:
        dst_parent = Path(dst_par_dir)

    if all_available:
        order_ids = get_oids()

    # download_orders(order_ids=order_ids, dst_par_dir=dst_par_dir, dryrun=dryrun, overwrite=overwrite)
    download_parallel(order_ids=order_ids, dst_par_dir=dst_par_dir, dryrun=dryrun)
