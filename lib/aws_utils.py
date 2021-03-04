from datetime import datetime
import os
from pathlib import Path
import time

from tqdm import tqdm
try:
    import boto3
except ImportError:
    print('Warning: boto3 import failed, delivery via AWS will not work.')

from lib.lib import get_config
from lib.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')


# AWS
AWS_PARAMS = get_config("aws")
AWS_ACCESS_KEY_ID = AWS_PARAMS["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = AWS_PARAMS["aws_secret_access_key"]
AWS_BUCKET = AWS_PARAMS["aws_bucket"]
AWS_REGION = AWS_PARAMS["aws_region"]
AWS_PATH_PREFIX = AWS_PARAMS["aws_path_prefix"]
BUCKET_NAME = 'pgc-data'

# Constants
S3 = 's3'


def connect_aws_bucket(bucket_name=BUCKET_NAME,
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY):
    s3 = boto3.resource(S3, aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key, )
    bucket = s3.Bucket(bucket_name)

    return bucket


def manifest_exists(order_id, bucket):
    """
    Check if source for given order id exists in AWS bucket.
    Manifest is last file delivered for order and so presence
    order is ready to download.
    """
    # Path to source for order
    mani_path = AWS_PATH_PREFIX / Path(order_id) / 'source.json'
    # Get files that match source path - should only be one
    mani_filter = bucket.objects.filter(Prefix=mani_path.as_posix())
    objs = list(mani_filter)
    if len(objs) >= 1 and objs[0].key == mani_path.as_posix():
        logger.debug('Manifest for {} exists.'.format(order_id))
        mani_exists = True
    else:
        mani_exists = False

    return mani_exists


def create_aws_delivery(aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        bucket=AWS_BUCKET, aws_region=AWS_REGION,
                        path_prefix=AWS_PATH_PREFIX):
    aws_delivery = {
        "delivery": {
            "amazon_s3": {
                "bucket": bucket,
                "aws_region": aws_region,
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
                "path_prefix": path_prefix
            }
        }
    }

    return aws_delivery


def check_aws_delivery_status(order_id, bucket, start_time, wait_max,
                              wait_interval, wait_max_interval):
    # Check for presence of source, sleeping between checks.
    while running_time < wait_max and not start_dl:
        exists = manifest_exists(order_id, bucket=bucket)
        if not exists:
            running_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.debug('Manifest not present for order ID: {} :'
                         ' {}s remaining'.format(order_id, round(wait_max - running_time)))
            time.sleep(wait)
            if wait <= wait_max_interval:
                wait += wait_interval
            if wait > wait_max_interval:
                wait = wait_max_interval
        else:
            logger.debug('Manifest present - beginning download: {}'.format(order_id))
            start_dl = True

    return start_dl


def dl_aws(oid, dst_par_dir, oid_dir, bucket, overwrite=False,
           dryrun=False):
    # Filter the bucket for the order id, removing any directory keys
    order_prefix = '{}/{}'.format(AWS_PATH_PREFIX, oid)
    bucket_filter = [bo for bo in bucket.objects.filter(Prefix=order_prefix)
                     if not bo.key.endswith('/')]
    # Set up progress bar
    # For setting progress bar length
    item_count = len([x for x in bucket_filter])
    pbar = tqdm(bucket_filter, total=item_count, desc='Order: {}'.format(oid), position=1)
    # For aligning arrows in progress bar writing
    # arrow_loc = max([len(x.key) for x in bucket_filter]) - len(order_prefix)

    logger.info('Downloading {:,} files to: {}'.format(item_count, oid_dir))
    for bo in pbar:
        # Determine source and destination full paths
        aws_loc = Path(bo.key)
        # Create destination subdirectory path with order id as subdirectory
        dst_path = dst_par_dir / aws_loc.relative_to(Path(AWS_PATH_PREFIX))
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
    return dl_issues
