import argparse
import os
import pathlib
from pathlib import Path
import sys

from tqdm import tqdm

from logging_utils.logging_utils import create_logger, create_logfile_path
from lib import metadata_path_from_scene, gdf_from_metadata, k_scenes_id, k_order_id
from db_utils import Postgres

planet_db = 'sandwich-pool.planet'
scenes_onhand_tbl = 'scenes_onhand'
# TODO: Switch to permanent order arrival location
# data_directory = Path(r'V:\pgc\data\scratch\jeff\projects\planet\data')
data_directory = Path(r'E:\disbr007\projects\planet\data')
item_types = ['PSScene3Band', 'PSScene4Band']
date_cols = ['acquired']
scene_suffix = r'1B_AnalyticMS.tif'

windows = 'Windows'
linux = 'Linux'
# TODO: Create json config that is schema for scenes_onhand


def is_udm(filepath):
    """filepath : pathlib.Path"""
    if not isinstance(filepath, pathlib.PurePath):
        filepath = Path(filepath)
    if filepath.stem[-3:] == 'udm':
        udm = True
    else:
        udm = False

    return udm


def get_onhand_ids():
    # Connect to DB
    with Postgres(planet_db) as db:
        if scenes_onhand_tbl not in db.list_db_tables():
            onhand = set()
        else:
            # Get all unique IDs
            onhand = set(db.get_values(layer=scenes_onhand_tbl, columns=k_scenes_id))

    logger.info('Onhand IDs: {:,}'.format(len(onhand)))
    # logger.debug('\n{}'.format('\n'.join(onhand)))

    return onhand


def get_scenes_noh(data_directory):
    # Walk data dir, find all .tif files not in onhand table
    scenes_to_parse = list()
    onhand = get_onhand_ids()

    if onhand:
        for root, dirs, files in tqdm(os.walk(data_directory)):
            logger.debug('Removing onhand IDs from files to parse...')
            for f in files:
                # TODO: Find a better way to identify imagery/scene tifs
                if not f.startswith(tuple(onhand)) and f.endswith('.tif') and not f.endswith('_udm.tif'):
                    scenes_to_parse.append(Path(root) / f)
    else:
        logger.debug('No onhand IDs found, adding all scenes.')
        for root, dirs, files in tqdm(os.walk(data_directory)):
            for f in files:
                fp = Path(root) / f
                if f.endswith('.tif') and not f.endswith('_udm.tif'):
                    scenes_to_parse.append(fp)

    # logger.debug('Found IDS not onhand:\n{}'.format('\n'.join([str(x) for x in scenes_to_parse])))
    logger.debug('Found IDs to add: {}'.format(len(scenes_to_parse)))

    return scenes_to_parse


def update_scenes_onhand(data_directory=data_directory,
                         parse_orders=None,
                         dryrun=False):
    # Get all scenes to parse
    logger.info('Locating scenes to add...')
    scenes_to_parse = get_scenes_noh(data_directory=data_directory)

    # **REMOVE**
    # logger.warning('Selecting first 1000 for debugging/testing.')
    # scenes_to_parse = scenes_to_parse[:1000]

    logger.info('Scenes to add: {:,}'.format(len(scenes_to_parse)))
    if len(scenes_to_parse) == 0:
        logger.info('No new scenes found to add. Exiting')
        sys.exit()

    # Get remaining directories to parse, just for reporting
    # order_dirs = {f.relative_to(data_directory).parts[0] for f in scenes_to_parse}
    # logger.info('New order directories to parse: {}'.format(len(order_dirs)))
    # logger.debug('Order directories to parse:\n{}'.format('\n'.join(order_dirs)))

    # Get metadata paths
    logger.info('Parsing metadata files...')
    # Tuples of (scene_path, metadata_path)
    scene_and_md_paths = [(s, metadata_path_from_scene(s)) for s in scenes_to_parse]
    # Remove non-existing
    scene_and_md_paths = [(s, m) for s, m in scene_and_md_paths if s.exists() and m.exists()]
    logger.info('Scenes with metadata found: {:,}'.format(len(scene_and_md_paths)))

    # Parse metadata into rows to add to onhand table
    logger.info('Parsing metadata files to populate {}'.format(scenes_onhand_tbl))
    new_rows = gdf_from_metadata(scene_and_md_paths)
    logger.info('Scenes to add: {:,}'.format(len(new_rows)))

    if not dryrun:
        logger.info('Inserting new records to {}'.format(scenes_onhand_tbl))
        # Insert new records
        with Postgres(planet_db) as db:
            db.insert_new_records(new_rows, table=scenes_onhand_tbl,
                                  unique_id=k_scenes_id, date_cols=date_cols)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    method_args = parser.add_mutually_exclusive_group()
    # method_args.add_argument('--parse_orders', nargs='+', type=str,
    #                          help='Specific order numbers (subdirectories) to parse.')
    parser.add_argument('--logfile', type=os.path.abspath,
                        help='Path to write logfile to.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Set console logging to DEBUG ')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without adding new records.')

    args = parser.parse_args()

    # parse_orders = args.parse_orders
    logfile = args.logfile
    verbose = args.verbose
    dryrun = args.dryrun

    # Logging
    if verbose:
        log_lvl = 'DEBUG'
    else:
        log_lvl = 'INFO'

    logger = create_logger(__name__, 'sh', log_lvl)
    if not logfile:
        logfile = create_logfile_path(Path(__file__).stem)
    logger = create_logger(__name__, 'fh', 'DEBUG',
                           filename=create_logfile_path(Path(__file__).stem))

    # Main
    update_scenes_onhand(data_directory=data_directory,
                         # parse_orders=parse_orders,
                         dryrun=dryrun)

