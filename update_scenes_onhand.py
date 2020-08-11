import argparse
import os
import pathlib
from pathlib import Path

from logging_utils.logging_utils import create_logger, create_logfile_path
from scene_parsing import metadata_path_from_scene, gdf_from_metadata, scenes_id, order_id
from db_utils import Postgres

planet_db = 'sandwich-pool.planet'
scenes_onhand_tbl = 'scenes_onhand'
data_directory = Path(r'V:\pgc\data\scratch\jeff\projects\planet\data')
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


def get_onhand_ids(by_id=False, by_order=False):
    # Connect to DB
    with Postgres(planet_db) as db:
        if scenes_onhand_tbl not in db.list_db_tables():
            onhand = set()
        else:
            # Get all unique IDs, by order or id
            if by_id:
                onhand = set(db.get_values(layer=scenes_onhand_tbl,
                                           columns=scenes_id))
            elif by_order:
                onhand = set(db.get_values(layer=scenes_onhand_tbl,
                                           columns=order_id))

    logger.info('Onhand (ids or orders): {}'.format(len(onhand)))
    logger.debug('\n{}'.format('\n'.join(onhand)))

    return onhand


def get_scenes_noh(data_directory, by_order=False, by_id=False,):
    # Walk data dir, find all .tif files not on hand by order or id
    scenes_to_parse = list()
    onhand = get_onhand_ids(by_order=by_order, by_id=by_id)

    found_oids = set()
    for root, dirs, files in os.walk(data_directory):
        if by_order:
            logger.debug('Removing onhand order directories from search...')
            for f in files:
                fp = Path(root) / f
                oid = (Path(root) / f).relative_to(data_directory).parts[0]
                found_oids.add(oid)
                if oid not in onhand and fp.suffix == '.tif' and not is_udm(fp):
                    scenes_to_parse.append(fp)

        if by_id:
            onhand = get_onhand_ids(by_id=by_id)
            logger.debug('Removing onhand IDs from files to parse...')
            scenes_to_parse = [Path(root) / f for f in files
                               if not f.startswith(tuple(onhand)) and
                               f.endswith('.tif') and
                               not f.endswith('_udm.tif')]

    logger.debug('Found OIDS:\n{}'.format('\n'.join(found_oids)))

    return scenes_to_parse


def update_scenes_onhand(data_directory=data_directory,
                         parse_orders=None, by_order=True, by_id=False,
                         dryrun=False):
    # Get all scenes to parse
    logger.info('Locating scenes to add...')
    if parse_orders:
        scenes_to_parse = []
        for po in parse_orders:
            parse_directory = data_directory / po
            order_scenes = get_scenes_noh(data_directory=parse_directory, by_id=True)
            logger.info('Scenes for order {}: {}'.format(po, len(order_scenes)))
            scenes_to_parse.extend(order_scenes)
    else:
        scenes_to_parse = get_scenes_noh(data_directory=data_directory, by_order=by_order, by_id=by_id)

    logger.info('Scenes to add: {:,}'.format(len(scenes_to_parse)))

    # Get remaining directories to parse, just for reporting
    order_dirs = {f.relative_to(data_directory).parts[0] for f in scenes_to_parse}
    logger.info('New order directories to parse: {}'.format(len(order_dirs)))
    logger.debug('Order directories to parse:\n{}'.format('\n'.join(order_dirs)))

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
                                  unique_id=scenes_id, date_cols=date_cols)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    method_args = parser.add_mutually_exclusive_group()
    method_args.add_argument('--parse_orders', nargs='+', type=str,
                        help='Specific order numbers (subdirectories) to parse.')
    method_args.add_argument('--by_order', action='store_true',
                        help='Parse data directory by order - look only for order '
                             'directories that have not been parased yet.')
    method_args.add_argument('--by_id', action='store_true',
                        help='Parse by IDs. Look in all folders for any ID not in scenes_onhand.')
    parser.add_argument('--logfile', type=os.path.abspath,
                        help='Path to write logfile to.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Set console logging to DEBUG ')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without adding new records.')

    args = parser.parse_args()

    parse_orders = args.parse_orders
    by_order = args.by_order
    by_id = args.by_id
    logfile = args.logfile
    verbose = args.verbose
    dryrun = args.dryrun

    if verbose:
        log_lvl = 'DEBUG'
    else:
        log_lvl = 'INFO'

    logger = create_logger(__name__, 'sh', log_lvl)

    if not logfile:
        logfile = create_logfile_path(Path(__file__).stem)
    logger = create_logger(__name__, 'fh', 'DEBUG',
                           filename=create_logfile_path(Path(__file__).stem))

    update_scenes_onhand(data_directory=data_directory,
                         parse_orders=parse_orders, by_order=by_order, by_id=by_id,
                         dryrun=dryrun)

