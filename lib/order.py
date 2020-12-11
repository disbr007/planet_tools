import datetime
import json

import os
import pathlib
import requests
import time
import sys

from itertools import product
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
from requests.auth import HTTPBasicAuth
from retrying import retry, RetryError

import boto3

import geopandas as gpd

from tqdm import tqdm

from lib.lib import read_ids, get_config
from lib.db import Postgres, stereo_pair_sql
from lib.logging_utils import create_logger


logger = create_logger(__name__, 'sh', 'INFO')

# Constants
# stereo_pair_cand = "candidate_pairs"
# fld_acq = "acquired1"
# fld_ins = "instrument"
# fld_ins1 = "{}1".format(fld_ins)
# fld_ins2 = "{}2".format(fld_ins)
# fld_date_diff = "date_diff"
# fld_view_angle_diff = "view_angle_diff"
# fld_ovlp_perc = "ovlp_perc"
# fld_geom = "ovlp_geom"

# Constants
srid = 4326

# Planet
# API URLs
ORDERS_URL = "https://api.planet.com/compute/ops/orders/v2"
PLANET_API_KEY = os.getenv("PL_API_KEY")
if not PLANET_API_KEY:
    logger.error("Error retrieving API key. Is PL_API_KEY env. variable set?")
auth = HTTPBasicAuth(PLANET_API_KEY, "")

# Postgres
# Used for finding connection config file -> config/sandwich-pool.planet.json
# TODO read these from config file
planet_db = "sandwich-pool.planet"
scenes_onhand_tbl = 'scenes_onhand'
# TODO: rename to use the same variable throughout
scene_id = 'id'
fld_id = 'id'

# AWS
# TODO: Update when NASA bucket created
aws_params = get_config("aws")
aws_access_key_id = aws_params["aws_access_key_id"]
aws_secret_access_key = aws_params["aws_secret_access_key"]
aws_bucket = aws_params["aws_bucket"]
aws_region = aws_params["aws_region"]
aws_path_prefix = aws_params["aws_path_prefix"]
bucket_name = 'pgc-data'
prefix = r'jeff/planet'

default_dst_parent = get_config("shelve_loc")

# Check Planet authorization
auth_resp = requests.get(ORDERS_URL, auth=auth)
logger.debug("Authorizing Planet API Key....")
if auth_resp.status_code != 200:
    logger.error("Issue authorizing: {}".format(auth_resp))
logger.debug("Response: {}".format(auth_resp))

headers = {"content-type": "application/json"}


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
    Check if source for given order id exists in AWS bucket.
    Manifest is last file delivered for order and so presence
    order is ready to download.
    """
    # Path to source for order
    mani_path = prefix / Path(order_id) / 'source.json'
    # Get files that match source path - should only be one
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
    # TODO: Resolve why at least dst_par dir is not coming in as PurePath
    if not isinstance(oid, pathlib.PurePath):
        oid = Path(oid)
    if not isinstance(dst_par_dir, pathlib.PurePath):
        dst_par_dir = Path(dst_par_dir)

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
                        wait_max_interval=300, wait_max=5400):
    """
    Wrapper for dl_order that checks if source.json is present
    in order subdirectory in AWS bucket before downloading. Checks
    every [wait_start] seconds, increasing by [wait_interval] until
    [wait_max_interval] is reached and then every [wait_max_interval]
    until [wait_max] is reached at which point the attempt to download is aborted.
    """
    logger.debug('Waiting for orders to arrive in AWS...')
    start_time = datetime.datetime.now()
    running_time = (datetime.datetime.now() - start_time).total_seconds()
    wait = wait_start
    start_dl = False
    # Check for presence of source, sleeping between checks.
    while running_time < wait_max and not start_dl:
        exists = manifest_exists(order_id, bucket=bucket)
        if not exists:
            running_time = (datetime.datetime.now() - start_time).total_seconds()
            logger.debug('Manifest not present for order ID: {} :'
                         ' {}s remaining'.format(order_id, round(wait_max-running_time)))
            time.sleep(wait)
            if wait <= wait_max_interval:
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
    # Create a dl_order_when_ready call for each order id in order_ids,
    # with the specified arguments
    results = pool.starmap(dl_order_when_ready, product(order_ids, [dst_par_dir], [bucket],
                                                        [overwrite], [dryrun],
                                                        [2], [10], [wait_max]))
    pool.close()
    pool.join()

    logger.info('Download statuses:\nOrder ID\t\tStarted\t\tIssue\n{}'.format(
        '\n'.join(["{} {}\t{}".format(oid, start_dl, issue) for oid, start_dl, issue in results])
    ))

    return results


def remove_recent_ids(ids, num_days=31):
    today = datetime.datetime.today()
    max_date = today - datetime.timedelta(days=num_days)

    removed = [x for x in ids if
               datetime.datetime.strptime(x[:8], '%Y%m%d') < max_date]

    return removed


def get_stereo_pairs(**kwargs):
    """Load stereo pairs from DB"""
    sql = stereo_pair_sql(**kwargs)
    # Load records
    with Postgres(planet_db) as db:
        results = gpd.GeoDataFrame.from_postgis(sql=sql, con=db.get_engine().connect(),
                                                geom_col="ovlp_geom", crs="epsg:4326")

    return results


def pairs_to_list(pairs_df, id1="id1", id2="id2", removed_onhand=True):
    """Take a dataframe containing two ID columns and return as single list of IDs."""
    out_list = []
    for i, row in pairs_df.iterrows():
        out_list.append(row[id1])
        out_list.append(row[id2])

    out_list = list(set(out_list))

    if removed_onhand:
        pass
        # TODO: Read onhand IDs from a table and remove

    return out_list


def create_aws_delivery(aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        bucket=aws_bucket, aws_region=aws_region, path_prefix=aws_path_prefix):
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


def create_order_request(order_name, ids, item_type="PSScene4Band",
                         product_bundle="basic_analytic_dn",
                         delivery="aws"):
    """Create order from list of IDs"""
    if isinstance(product_bundle, list):
        product_bundle = ','.join(product_bundle)
    order_request = {
        "name": order_name,
        "products": [
            {"item_ids": ids,
             "item_type": item_type,
             "product_bundle": product_bundle}
        ]
    }
    if delivery == "aws":
        order_request.update(create_aws_delivery())
    # TODO: other cloud locations (Azure, Google)
    # logger.info(order_request)

    return order_request


def place_order(order_request):
    response = requests.post(ORDERS_URL, data=json.dumps(order_request), auth=auth, headers=headers)
    logger.debug("Place order response: {}".format(response))
    if response.status_code != 202:
        logger.error("Error placing order '{}': {}".format(order_request["name"], response))
        logger.error("Response reason: {}".format(response.reason))
        logger.error('Request:\n{}'.format(order_request))
        sys.exit()
    order_id = response.json()["id"]
    # logger.debug('Request:\n{}'.format(order_request))
    logger.debug("Order ID: {}".format(order_id))
    order_url = "{}/{}".format(ORDERS_URL, order_id)

    return order_id, order_url


def cancel_order(order_url):
    order_id = order_url.split("/")[-1]
    # Get current status
    state = requests.get(order_url, auth=auth).json()["state"]
    logger.debug("Order {} state: {}".format(order_id, state))

    logger.debug("Cancelling order: {}".format(order_id))
    requests.put(order_url, auth=auth)
    state = requests.get(order_url, auth=auth).json()["state"]
    logger.debug("Order {} state: {}".format(order_id, state))

    if state == "cancelled":
        logger.info("Order {} successfully cancelled.".format(order_id))
    else:
        logger.info("Order {} not cancelled, state: {}".format(order_id, state))


def poll_for_success(order_id, num_checks=50, wait=10):
    running_states = ["queued", "running"]
    end_states = ["success", "failed", "partial"]

    check_count = 0
    finished = False
    waiting_reported = False
    while check_count < num_checks and finished is False:
        check_count += 1
        r = requests.get(ORDERS_URL, auth=auth)
        response = r.json()
        state = None
        for i, o in enumerate(response['orders']):
            if o['id'] == order_id:
                state = o["state"]
                logger.debug('Order ID found: {}'.format(order_id))
        if state in running_states:
            logger.debug('Order not finished. State: {}'.format(state))
        elif state in end_states:
            logger.info("Order finished. State: {}".format(state))
            finished = True
            break
        else:
            logger.warning("Order ID {} not found.".format(order_id))
            finished = True
        if not finished:
            if not waiting_reported:
                logger.debug('Order did not reach end state, waiting {}s'.format(wait*num_checks))
                time.sleep(wait)
                waiting_reported = True

    if not finished:
        logger.info("Order did not reach an end state within {} checks with {}s waits.".format(num_checks, wait))

    return finished


def get_order_results(order_url):
    response = requests.get(order_url).json()
    results = response["_links"]["results"]

    logger.debug("\n".join([r["name"] for r in results]))

    return results


# TODO: Function to get IDs from inprogress orders: state='not success' ['products']['item_ids']
# def download_results(results, overwrite=False):
#     results_urls = [r["location"] for r in results]
#     results_names = [r["name"] for r in results]
#
#     logger.info("Items to download: {}".format(len(results_urls)))
#
#     # TODO: add tqdm?
#     for url, name in zip(results_urls, results_names):
#         # TODO: Create subdir for each ID
#         path = pathlib.Path(os.path.join("data", name))
#
#         if overwrite or not path.exists():
#             logger.debug("Downloading {}: {}".format(name, url))
#             r = requests.get(url, allow_redirects=True)
#             path.parent.mkdir(parents=True, exist_ok=True)
#             with open(path, "wb") as dst:
#                 dst.write(r.content)
#         else:
#             logger.debug("File already exists, skipping: {} {}".format(name, url))


def list_orders(state=None):
    logger.info('Listing orders...')
    # TODO: Check if there is a page limit on orders return
    # TODO: Date limit on orders returned
    # TODO: verbose print with keys:
    #  'created_on', 'delivery', 'error_hints', 'id', 'last_message',
    #  'last_modified', 'name', 'products', 'state'
    params = {}
    if state:
        params = {"state": state}

    response = requests.get(ORDERS_URL, auth=auth, params=state)
    if response.status_code != 200:
        logger.error('Bad response: {}: {}'.format(response.status_code, response.reason))
        # TODO: Create error class
        raise ConnectionError
    orders = response.json()["orders"]

    # Pretty printing
    spaces = 1
    dashes = 6
    longest_id = max([len(o['id']) for o in orders])
    longest_name = max([len(o['name']) for o in orders])

    for o in orders:
        id_spaces = '{0}{1}{0}'.format(spaces*' ', round((dashes + longest_id - len(o['id'])) / 2)*'-')
        name_spaces = '{0}{1}{0}'.format(spaces*' ', round((dashes + longest_name - len(o['name'])) / 2) * '-')
        logger.info("ID: {}{}Name: {}{}State: {}".format(o["id"], id_spaces,
                                                         o["name"], name_spaces,
                                                         o["state"]))

    return orders


@retry(wait_exponential_multiplier=1000, wait_exponential_max=60000)
def count_concurrent_orders():
    # TODO: Move these to a config file
    orders_url = 'https://api.planet.com/compute/ops/stats/orders/v2'
    PLANET_API_KEY = os.getenv('PL_API_KEY')
    if not PLANET_API_KEY:
        logger.error('Error retrieving API key. Is PL_API_KEY env. variable '
                     'set?')

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


def submit_order(name, ids_path, selection_path, product_bundle,
                 orders_path=None, remove_onhand=True,
                 dryrun=False):

    if ids_path:
        logger.info('Reading IDs from: {}'.format(ids_path))
        # ids = read_ids(ids_path, field=ids_field)
        ids = list(set([x.strip() for x in open(ids_path, 'r')]))

    elif selection_path:
        logger.info('Reading IDs from selection: {}'.format(selection_path))
        ids = read_ids(selection_path, field='id')

    logger.info('IDs found: {:,}'.format(len(ids)))

    # logger.info('Removing any recent IDs, per Planet limit on recent image
    # ordering.')
    # ids = remove_recent_ids(ids)
    # logger.info('Remaining IDs: {}'.format(len(ids)))

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

    submitted_orders = []
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
            logger.debug('Concurrent orders: {}'.format(concurrent_orders))
            if concurrent_orders < max_concur:
                # Submit
                if not dryrun:
                    logger.info('Submitting order: {}'.format(order_name))
                    order_id, order_url = place_order(order_request=order_request)
                    submitted_orders.append(order_id)
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

    if orders_path:
        logger.info('Writing order IDs to file: {}'.format(orders_path))
        with open(orders_path, 'w') as orders_txt:
            for order_id in submitted_orders:
                orders_txt.write(order_id)
                orders_txt.write('\n')

    return submitted_orders


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#
#     parser.add_argument('-l', '--list', action='store_true',
#                         help='List orders: ID, Name, State')
#
#     args = parser.parse_args()
#
#     lo = args.list
#
#     if lo:
#         list_orders()


# CLI from download_utils for downloading order IDs
# if __name__ == '__main__':
#     parser = argparse.ArgumentParser()
#
#     mut_exc = parser.add_mutually_exclusive_group()
#     mut_exc.add_argument('-all', '--all_available', action='store_true',
#                         help='Download all available order IDs. Overrides -oid arguments')
#     mut_exc.add_argument('-oid', '--order_ids', type=str, nargs='+',
#                         help='Order ID(s) to download.')
#     parser.add_argument('--dryrun', action='store_true',
#                         help='Print actions without downloading.')
#     parser.add_argument('-dpd', '--destination_parent_directory', type=os.path.abspath,
#                         help='Directory to download imagery to. Subdirectories for each order will be created here.')
#     parser.add_argument('--overwrite', action='store_true',
#                         help='Overwrite files in destination. Otherwise duplicates are skipped.')
#     parser.add_argument('-l', '--logfile', type=os.path.abspath,
#                         help='Location to wrtie log to.')
#
#     args = parser.parse_args()
#
#     all_available = args.all_available
#     order_ids = args.order_ids
#     dst_par_dir = args.destination_parent_directory
#     dryrun = args.dryrun
#     overwrite = args.overwrite
#     logfile = args.logfile
#
#     # Destination
#     if not dst_par_dir:
#         dst_parent = Path(default_dst_parent)
#     else:
#         dst_par_dir = Path(dst_par_dir)
#
#     if all_available:
#         order_ids = get_oids()
#
#     download_parallel(order_ids=order_ids, dst_par_dir=dst_par_dir, dryrun=dryrun)

