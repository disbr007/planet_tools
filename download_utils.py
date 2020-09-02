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
# TODO: Update when NASA bucket created
prefix = r'jeff/planet'

# TODO: Default DL location -> finalize and move to config file
default_dst_parent = r'V:\pgc\data\scratch\jeff\projects\planet\data'


logger = create_logger(__name__, 'sh', 'DEBUG')


# TODO FIX THIS - bucket should not load on module loading - how to pass best bucket around?
def connect_aws_bucket(bucket_name=bucket_name,
                       aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key):
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key, )
    bucket = s3.Bucket(bucket_name)

    return bucket


def get_oids(prefix=prefix):
    """
    Get all immediate subdirectories of prefix in AWS, these are Planet order ids.
    """
    # s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
    #                     aws_secret_access_key=aws_secret_access_key, )
    # 
    # bucket = s3.Bucket(bucket_name)
    bucket = connect_aws_bucket()

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


def manifest_exists(order_id, bucket):
    """
    Check if manifest for given order id exists in AWS bucket.
    Manifest is last file delivered for order and so presence
    order is ready to download.
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


def dl_order(oid, dst_par_dir, bucket, overwrite=False, dryrun=False):
    """Download an order id (oid) to destination parent directory, creating
    a new subdirectory for the order id. Order ID is also name of subdirectory
    in AWS bucket."""
    logger.info('Downloading order: {}'.format(oid))

    # Filter the bucket for the order id, removing any directory keys
    order_prefix = '{}/{}'.format(prefix, oid)
    bucket_filter = [bo for bo in bucket.objects.filter(Prefix=order_prefix)
                     if not bo.key.endswith('/')]

    # Set up progress bar
    # For setting progress bar length
    item_count = len([x for x in bucket_filter])
    pbar = tqdm(bucket_filter, total=item_count, desc='Order: {}'.format(oid), position=1)
    # For aligning arrows in progress bar writing
    # arrow_loc = max([len(x.key) for x in bucket_filter]) - len(order_prefix)

    oid_dir = dst_par_dir / oid
    if not os.path.exists(oid_dir):
        os.makedirs(oid_dir)

    logger.info('Downloading {:,} files to: {}'.format(item_count, oid_dir))
    for bo in pbar:
        # Determine source and destination full paths
        aws_loc = Path(bo.key)
        # Create destination subdirectory path with order id as subdirectory
        dst_path = dst_par_dir / aws_loc.relative_to(Path(prefix))
        if not os.path.exists(dst_path.parent):
            os.makedirs(dst_path.parent)

        # Download
        if os.path.exists(dst_path) and not overwrite:
            logger.debug('File exists at destination, skipping: {}'.format(dst_path))
            continue
        else:
            logger.debug('Downloading file: {}\n\t--> {}'.format(aws_loc, dst_path.absolute()))
            # pbar.write('Downloading: {}{}-> {}'.format(aws_loc.relative_to(*aws_loc.parts[:3]),
            #                                            ' '*(arrow_loc - len(str(aws_loc.relative_to(*aws_loc.parts[:3])))),
            #                                            oid_dir.relative_to(*dst_par_dir.parts[:9])))
        dl_issues = set()
        if not dryrun:
            try:
                bucket.download_file(bo.key, str(dst_path))
                dl_issues.add(False)
            except Exception as e:
                logger.error('Error downloading: {}'.format(aws_loc))
                logger.error(e)
                dl_issues.add(True)

    logger.info('Done.')

    all_success = any(dl_issues)

    return all_success


def dl_order_when_ready(order_id, dst_par_dir, bucket,
                        overwrite=False, dryrun=False,
                        wait_start=2, wait_interval=10,
                        # wait_increase_exp=2,
                        wait_max_interval=300, wait_max=3600):
    """
    Wrapper for dl_order that checks if manifest.json is present
    in order subdirectory in AWS bucket before downloading. Checks
    start every [wait_start] seconds, increasing by
    [wait_start]**[wait_increase_exp] until [wait_max_interval]
    is reached and then every [wait_max_interval] until [wait_max]
    is reached at which pint the attempt to download is aborted.
    """
    logger.info('Waiting for orders to arrive in AWS...')
    start_time = datetime.datetime.now()
    running_time = (datetime.datetime.now() - start_time).total_seconds()
    wait = wait_start
    start_dl = False
    # Check for presence of manifest, sleeping between checks.
    while running_time < wait_max and not start_dl:
        exists = manifest_exists(order_id, bucket=bucket)
        if not exists:
            running_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.debug('Manifest not present for order ID: {} :'
                         ' {}s remaining'.format(order_id, round(wait_max-running_time)))
            time.sleep(wait)
            if wait <= wait_max_interval:
                # wait = wait**wait_increase_exp
                wait += wait_interval
            if wait > wait_max_interval:
                wait = wait_max_interval

        else:
            logger.debug('Manifest present - beginning download: {}'.format(order_id))
            start_dl = True

    if start_dl:
        logger.info('Started downloading: {}'.format(order_id))
        all_success = dl_order(order_id, dst_par_dir=dst_par_dir, bucket=bucket,
                               overwrite=overwrite, dryrun=dryrun)
    else:
        logger.info('Maximum wait reached, did not begin download: {}'.format(order_id))

    return order_id, start_dl, all_success


def download_parallel(order_ids, dst_par_dir, overwrite=False, dryrun=False, threads=4, wait_max=3600):
    """
    Download order ids in parallel.
    """
    bucket = connect_aws_bucket()
    pool = ThreadPool(threads)
    results = pool.starmap(dl_order_when_ready, product(order_ids, [dst_par_dir], [bucket],
                                                        [dryrun], [overwrite]))
    pool.close()
    pool.join()

    logger.info('Download statuses:\nOrder ID\t\tStarted\t\tIssue\n{}'.format(
        '\n'.join(["{} {}\t{}".format(oid, start_dl, issue) for oid, start_dl, issue in results])
    ))

    return results


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
    parser.add_argument('-l', '--logfile', type=os.path.abspath,
                        help='Location to wrtie log to.')

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
        dst_par_dir = Path(dst_par_dir)

    if all_available:
        order_ids = get_oids()

    # download_orders(order_ids=order_ids, dst_par_dir=dst_par_dir, dryrun=dryrun, overwrite=overwrite)
    download_parallel(order_ids=order_ids, dst_par_dir=dst_par_dir, dryrun=dryrun)
