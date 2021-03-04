from pathlib import Path

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