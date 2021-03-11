import datetime
import json

import os
import pathlib
import requests
import time
import sys
import zipfile

from itertools import product
from multiprocessing.dummy import Pool as ThreadPool
from pathlib import Path
from requests.auth import HTTPBasicAuth
from retrying import retry, RetryError

try:
    import boto3
except ImportError:
    print('Warning: boto3 import failed, delivery via AWS will not work.')
import geopandas as gpd

from lib.lib import read_ids, stereo_pair_sql
# from lib.db import Postgres, stereo_pair_sql
from lib.logging_utils import create_logger
import lib.aws_utils as aws_utils
import lib.constants as constants

# TODO:
#  -Rename download functions that are only for AWS deliveries to aws_[func_name]()
logger = create_logger(__name__, 'sh', 'INFO')

# External modules
sys.path.append(str(Path(__file__).parent / '..'))
try:
    from db_utils.db import Postgres
except ImportError as e:
    logger.error('db_utils module not found. It should be adjacent to '
                 'the planet_tools directory. Path: \n{}'.format(sys.path))
    sys.exit()


# Constants
srid = 4326

# Planet
# API URLs
# ORDERS_URL = "https://api.planet.com/compute/ops/orders/v2"
PLANET_API_KEY = os.getenv(constants.PL_API_KEY)
if not PLANET_API_KEY:
    logger.error("Error retrieving API key. Is PL_API_KEY env. variable set?")
auth = HTTPBasicAuth(PLANET_API_KEY, "")

# DELIVERY
DELIVERY_OPTS = [constants.AWS, constants.ZIP]
# Maximum time to wait for delivery to complete (in seconds)
WAIT_MAX = 5000

# Check Planet authorization
auth_resp = requests.get(constants.ORDER_URL, auth=auth)
logger.debug("Authorizing Planet API Key....")
if auth_resp.status_code != 200:
    logger.error("Issue authorizing: {}".format(auth_resp))
logger.debug("Response: {}".format(auth_resp))

headers = {"content-type": "application/json"}


def unzip_delivery(zf):
    logger.debug('Unzipping: {}'.format(zf))
    with zipfile.ZipFile(str(zf), 'r') as zipref:
        zipref.extractall(zf.parent)
    fp = zf.parent / 'files'
    scenes_dir = fp / os.listdir(fp)[0]
    scenes_dir.rename(scenes_dir.parent.parent / scenes_dir.name)
    # Remove now empty "files" directory
    os.rmdir(fp)
    # Remove original zipfile
    os.remove(zf)


def get_url(url, sleep_base=10, sleep_add=10, **kwargs):
    r = requests.get(url, **kwargs)
    sleep_time = sleep_base
    while r.status_code == 429:
        logger.debug(r)
        logger.warning('Too many requests, sleeping: {}s'.format(sleep_time))
        time.sleep(sleep_time)
        r = requests.get(url, **kwargs)
        sleep_time += sleep_add

    return r


def download_url(url, save_path, chunk_size=1024):
    """Download a file at a given url, used for Zip deliveries."""
    if not Path(save_path).parent.exists():
        os.makedirs(Path(save_path.parent))

    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def dl_order(oid, dst_par_dir, delivery, bucket=None, overwrite=False, dryrun=False):
    """Download an order id (oid) to destination parent directory, creating
    a new subdirectory for the order id. Order ID is also name of subdirectory
    in AWS bucket."""
    # TODO: Resolve why at least dst_par dir is not coming in as Path
    if not isinstance(oid, pathlib.PurePath):
        oid = Path(oid)
    if not isinstance(dst_par_dir, pathlib.PurePath):
        dst_par_dir = Path(dst_par_dir)

    logger.info('Downloading order: {}'.format(oid))

    # Destination directory
    oid_dir = dst_par_dir / oid
    if not os.path.exists(oid_dir):
        os.makedirs(oid_dir)

    # AWS
    if delivery == constants.AWS:
        dl_issues = aws_utils.dl_aws(oid=oid, dst_par_dir=dst_par_dir,
                                     oid_dir=oid_dir, bucket=bucket,
                                     overwrite=overwrite,
                                     dryrun=dryrun)
    elif delivery == constants.ZIP:
        order_status_url = '{}/{}'.format(constants.ORDER_URL, oid)
        r = get_url(order_status_url, auth=auth)

        response = r.json()
        # response['results'] is an array of dicts, one for each file. there should
        # be two files, one for the manifest, one for the zip. Each dict has a 'name'
        # key that holds [order_id]/[filename] where filename is either [order_id].zip
        # or [order_id]/manifest.json
        names_urls = [(r['name'], r['location']) for r in response['_links']['results']]
        for name, url in names_urls:
            dest_path = oid_dir / Path(name).name
            if not dest_path.exists():
                logger.info('Downloading {} to:\n{}'.format(name, dest_path))
                download_url(url=url, save_path=dest_path)
                if dest_path.suffix == '.zip':
                    unzip_delivery(dest_path)
            else:
                logger.debug('Destination exists, skipping download: '
                             '{}'.format(dest_path))
        # TODO: create a method for actually checking success of direct
        #  downloads (existence of download zip?
        dl_issues = [None]

    logger.info('Done.')

    all_success = any(dl_issues)

    return all_success


def dl_order_when_ready(order_id, dst_par_dir, delivery,
                        bucket=None,
                        overwrite=False, dryrun=False,
                        wait_start=2, wait_interval=10,
                        wait_max_interval=300, wait_max=5400):
    """
    Wrapper for dl_order that checks if order is ready before downloading.
    Checks every [wait_start] seconds, increasing by [wait_interval] until
    [wait_max_interval] is reached and then every [wait_max_interval]
    until [wait_max] is reached at which point the attempt to download is aborted.
    For AWS, checks if source.json is present in order subdirectory in AWS bucket
    before downloading.
    For ZIP, checks if order status is success.
    # TODO: All delivery methods could likely use ZIP method of GETting the status
    """
    logger.debug('Waiting for orders to arrive in AWS...')
    start_time = datetime.datetime.now()
    running_time = (datetime.datetime.now() - start_time).total_seconds()
    wait = wait_start
    start_dl = False
    # AWS TODO: Move to aws download function
    if delivery == constants.AWS:
        aws_utils.check_aws_delivery_status(order_id=order_id, bucket=bucket,
                                            wait_max=wait_max,
                                            wait_interval=wait_interval,
                                            wait_max_interval=wait_max_interval)
    elif delivery == constants.ZIP:
        start_dl, _response = poll_for_success(order_id=order_id)

    if start_dl:
        logger.info('Started downloading: {}'.format(order_id))
        all_success = dl_order(order_id, dst_par_dir=dst_par_dir, delivery=delivery,
                               bucket=bucket,
                               overwrite=overwrite, dryrun=dryrun)
    else:
        logger.info('Maximum wait reached, did not begin download: {}'.format(order_id))
        all_success = False

    return order_id, start_dl, all_success


def download_parallel(order_ids, dst_par_dir, delivery,
                      overwrite=False,
                      dryrun=False,
                      threads=4,
                      wait_max=WAIT_MAX):
    """
    Download order ids in parallel.
    """
    if delivery == constants.AWS:
        bucket = aws_utils.connect_aws_bucket()
    else:
        bucket = None

    pool = ThreadPool(threads)
    # Create a dl_order_when_ready call for each order id in order_ids,
    # with the specified arguments
    results = pool.starmap(dl_order_when_ready,
                           product(order_ids, [dst_par_dir], [delivery],
                                   [bucket], [overwrite], [dryrun], [2], [10],
                                   [wait_max]))
    pool.close()
    pool.join()

    logger.info('Download statuses:\nOrder ID\t\t\t\t\tStarted\t\tIssue\n{}'.format(
        '\n'.join(["{}\t\t{}\t{}".format(oid, start_dl, issue)
                   for oid, start_dl, issue in results])
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
    with Postgres() as db:
        results = gpd.GeoDataFrame.from_postgis(sql=sql,
                                                con=db.get_engine().connect(),
                                                geom_col="ovlp_geom",
                                                crs="epsg:4326")

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


def create_zip_delivery():
    zip_delivery = {
        "delivery": {
            "archive_type": "zip",
            "single_archive": True,
            "archive_filename": "{{order_id}}.zip"
        }
    }
    # zip_delivery = json.dumps(zip_delivery)

    return zip_delivery


def create_order_request(order_name, ids, item_type="PSScene4Band",
                         product_bundle="basic_analytic_dn",
                         delivery=constants.AWS):
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
    if delivery == constants.AWS:
        order_request.update(aws_utils.create_aws_delivery())
    elif delivery == constants.ZIP:
        # pass
        order_request.update(create_zip_delivery())

    logger.debug(order_request)

    return order_request


def place_order(order_request):
    response = requests.post(constants.ORDER_URL, data=json.dumps(order_request),
                             auth=auth, headers=headers)
    logger.debug("Place order response: {}".format(response))
    if response.status_code != 202:
        logger.error("Error placing order '{}': {}".format(order_request["name"], response))
        logger.error("Response reason: {}".format(response.reason))
        logger.error('Request:\n{}'.format(order_request))
        sys.exit()
    order_id = response.json()["id"]
    # logger.debug('Request:\n{}'.format(order_request))
    logger.debug("Order ID: {}".format(order_id))
    order_url = "{}/{}".format(constants.ORDER_URL, order_id)

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
        r = get_url(constants.ORDER_URL, auth=auth)
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

    return finished, response


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

    response = requests.get(constants.ORDER_URL, auth=auth, params=state)
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
    PLANET_API_KEY = os.getenv('PL_API_KEY')
    if not PLANET_API_KEY:
        logger.error('Error retrieving API key. Is PL_API_KEY env. variable '
                     'set?')

    with requests.Session() as s:
        logger.debug('Authorizing using Planet API key...')
        s.auth = (PLANET_API_KEY, '')

        res = s.get(constants.ORDER_URL)
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
                 delivery=constants.AWS,
                 dryrun=False):

    if ids_path:
        logger.info('Reading IDs from: {}'.format(ids_path))
        # ids = read_ids(ids_path, field=ids_field)
        ids = list(set([x.strip() for x in open(ids_path, 'r')]))

    elif selection_path:
        logger.info('Reading IDs from selection: {}'.format(selection_path))
        ids = read_ids(selection_path, field='id')

    logger.info('IDs found: {:,}'.format(len(ids)))

    # logger.info('Removing any recent IDs, per Planet limit on recent IMAGE
    # ordering.')
    # ids = remove_recent_ids(ids)
    # logger.info('Remaining IDs: {}'.format(len(ids)))

    if remove_onhand:
        logger.info('Removing onhand IDs...')
        with Postgres() as db:
            onhand = set(db.get_values(constants.SCENES_ONHAND, columns=[constants.ID]))

        ids = list(set(ids) - onhand)
        logger.info('IDs remaining: {:,}'.format(len(ids)))

    # Limits are 500 ids per order, 80 concurrent orders
    assets_per_order = 500
    max_concur = 80
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
        order_request = create_order_request(order_name=order_name,
                                             ids=ids_chunk,
                                             product_bundle=product_bundle,
                                             delivery=delivery)
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

