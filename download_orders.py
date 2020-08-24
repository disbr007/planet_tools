import argparse
import json
from datetime import datetime
from pathlib import Path
import os

from tqdm import tqdm
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


def get_oids():
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key, )

    bucket = s3.Bucket(bucket_name)

    bucket_filter = bucket.objects.filter(Prefix=prefix)
    oids = set()
    logger.info('Getting order IDs to download...')
    for i, bo in enumerate(bucket_filter):
        key = Path(bo.key)
        oid = key.relative_to(Path(prefix)).parent.parent
        if str(oid) != '.':
            oids.add(oid)

    logger.info('Order IDs found: {}'.format(len(oids)))

    return oids


def download_orders(bucket_name, order_prefix, overwrite=False, dryrun=False):
    oid = order_prefix.split('/')[-1]
    logger.info('Downloading order: {}'.format(oid))

    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,)

    bucket = s3.Bucket(bucket_name)

    bucket_filter = bucket.objects.filter(Prefix=order_prefix)
    # For setting progress bar length
    item_count = len([x for x in bucket.objects.filter(Prefix=order_prefix)])
    # For aligning errors in progress bar writing
    arrow_loc = max([len(x.key) for x in bucket_filter]) - len(order_prefix)
    # Set up progress bar
    pbar = tqdm(bucket_filter, total=item_count, desc='Order: {}'.format(oid), position=1)
    logger.info('Downloading {:,} files to: {}'.format(item_count, order_dst_dir))
    for bo in pbar:
        # Determine source and destination full paths
        aws_loc = Path(bo.key)
        dst_loc = dst_dir / aws_loc.relative_to(Path(prefix))
        if os.path.exists(dst_loc) and not overwrite:
            logger.debug('File exists at destination, skipping: {}'.format(dst_loc))
            # continue

        if not os.path.exists(dst_loc.parent):
            os.makedirs(dst_loc.parent)

        # Download
        logger.debug('Downloading file: {}\n\t--> {}'.format(aws_loc, dst_loc))
        pbar.write('Downloading: {}{}-> {}'.format(aws_loc.relative_to(*aws_loc.parts[:3]),
                                                   ' '*(arrow_loc - len(str(aws_loc.relative_to(*aws_loc.parts[:3])))),
                                                   dst_loc.relative_to(*order_dst_dir.parts[:9])))
        if not dryrun:
            try:
                bucket.download_file(bo.key, dst_loc.absolute().as_posix())
            except Exception as e:
                logger.error('Error downloading: {}'.format(aws_loc))
                logger.error(e)

    logger.info('Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    mut_exc = parser.add_mutually_exclusive_group()
    mut_exc.add_argument('-all', '--all_available', action='store_true',
                        help='Download all available order IDs..')
    mut_exc.add_argument('-oid', '--order_id', type=str, nargs='+',
                        help='Order ID(s) to download.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without downloading.')
    parser.add_argument('-dst', '--destination', type=os.path.abspath,
                        help='Directory to download imagery to.')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite files in destination. Otherwise duplicates are skipped.')
    parser.add_argument('-l', '--logfile', type=os.path.abspath,
                        help='Location to wrtie log to.')

    args = parser.parse_args()

    all_available = args.all_available
    order_ids = args.order_id
    dst_dir = args.destination
    dryrun = args.dryrun
    overwrite = args.overwrite
    logfile = args.logfile


    # Destination
    if not dst_dir:
        dst_dir = Path(r'V:\pgc\data\scratch\jeff\projects\planet\data')
    else:
        dst_dir = Path(dst_dir)

    if not logfile:
        logfile = create_logfile_path(Path(__file__).stem)

    logger = create_logger(__name__, 'sh', 'INFO')
    logger = create_logger(__name__, 'fh', 'DEBUG', logfile)

    if all_available:
        order_ids = get_oids()

    logger.info('Order IDs to download:\n{}'.format('\n'.join([str(o) for o in order_ids])))
    pbar = tqdm(order_ids, desc='Order ', position=0)
    for i, oid in enumerate(pbar):
        pbar.set_description('Order: {}/{}'.format(i+1, len(order_ids)))
        order_dst_dir = dst_dir / oid
        if not os.path.exists(order_dst_dir):
            os.makedirs(order_dst_dir)
        order_prefix = '{}/{}'.format(prefix, oid)
        download_orders(bucket_name=bucket_name, order_prefix=order_prefix,
                        dryrun=dryrun, overwrite=overwrite)
        logger.debug('Order downloaded: {}'.format(oid))
        logger.info('\n\n')

# TODO: Make cron:
# TODO: Check AWS bucket for order -> remove onhand/downloaded orders -> download new orders -> delete