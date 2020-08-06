import argparse
import json
from datetime import datetime
import pathlib
import os

from tqdm import tqdm
import boto3

from misc_utils.logging_utils import create_logger


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


def download_orders(bucket_name, order_prefix):
    s3 = boto3.resource('s3', aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,)

    bucket = s3.Bucket(bucket_name)

    bucket_filter = bucket.objects.filter(Prefix=order_prefix)
    item_count = len([x for x in bucket.objects.filter(Prefix=order_prefix)])
    arrow_loc = max([len(x.key) for x in bucket_filter]) - len(order_prefix)
    pbar = tqdm(bucket_filter, total=item_count)
    logger.info('Downloading {} files to: {}'.format(item_count, order_dst_dir))
    for bo in pbar:
        # Determine source and destination full paths
        aws_loc = pathlib.Path(bo.key)
        dst_loc = dst_dir / aws_loc.relative_to(pathlib.Path(prefix))
        if os.path.exists(dst_loc):
            logger.debug('{} exists, skipping.'.format(dst_loc))
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

    parser.add_argument('-oid', '--order_id', type=str,
                        help='Order ID to download.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without downloading.')
    parser.add_argument('-dst', '--destination', type=os.path.abspath,
                        help='Directory to download imagery to.')
    parser.add_argument('-l', '--logfile', type=os.path.abspath,
                        help='Location to wrtie log to.')

    args = parser.parse_args()

    order_id = args.order_id
    dryrun = args.dryrun
    dst_dir = args.destination
    logfile = args.logfile

    # Destination
    if not dst_dir:
        dst_dir = pathlib.Path(r'V:\pgc\data\scratch\jeff\projects\planet\data')
    else:
        dst_dir = pathlib.Path(dst_dir)
    order_dst_dir = dst_dir / order_id
    if not os.path.exists(order_dst_dir):
        os.makedirs(order_dst_dir)

    if not logfile:
        now = datetime.now()
        logfile_name = 'planet_aws_download_{}.log'.format(datetime.strftime(now, '%Y%m%d_%H%M%S'))
        log_dir = dst_dir.parent / 'logs'
        logfile = log_dir / logfile_name

    logger = create_logger(__name__, 'sh', 'INFO')
    logger = create_logger(__name__, 'fh', 'DEBUG', logfile)

    order_prefix = '{}/{}'.format(prefix, order_id)

    download_orders(bucket_name=bucket_name, order_prefix=order_prefix)
