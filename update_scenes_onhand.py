import argparse
import json
import os
import pathlib
from pathlib import Path
import platform
from tqdm import tqdm
from typing import List

import geopandas as gpd
from shapely.geometry import Polygon

from logging_utils.logging_utils import create_logger, create_logfile_path
from db_utils import Postgres

logger = create_logger(__name__, 'sh', 'INFO')

planet_db = 'sandwich-pool.planet'
scenes_onhand_tbl = 'scenes_onhand'
scenes_id = 'id'
order_id = 'order_id'
data_directory = Path(r'V:\pgc\data\scratch\jeff\projects\planet\data')
item_types = ['PSScene3Band', 'PSScene4Band']
date_cols = ['acquired']
scene_suffix = r'1B_AnalyticMS.tif'

windows = 'Windows'
linux = 'Linux'
tn_loc = 'tn_loc'
win_loc = 'win_loc'


def is_udm(filepath):
    """filepath : pathlib.Path"""
    if not isinstance(filepath, pathlib.PurePath):
        filepath = Path(filepath)
    if filepath.stem[-3:] == 'udm':
        udm = True
    else:
        udm = False

    return udm


def id_from_scene(scene, scene_levels=['1B']):
    """scene : pathlib.Path"""
    if not isinstance(scene, pathlib.PurePath):
        scene = Path(scene)
    scene_name = scene.stem
    scene_id = None
    for lvl in scene_levels:
        if lvl in scene_name:
            scene_id = scene_name.split('_{}_'.format(lvl))[0]
    if not scene_id:
        logger.error('Could not parse scene ID with any level in {}: {}'.format(scene_levels, scene_name))

    return scene_id


def win2linux(path):
    lp = path.replace('\\', '/').replace('V:', 'mnt')

    return lp


def linux2win(path):
    wp = path.replace('/', '\\').replace('mnt', 'V:')

    return wp


def metadata_path_from_scene(scene):
    par_dir = scene.parent
    sid = id_from_scene(scene)
    metadata_path = par_dir / '{}_metadata.json'.format(sid)

    return metadata_path


def scene_path_from_metadata(metadata):
    # TODO: regex matching scenes but not udm.tif
    scene_path = Path(str(metadata).replace("metadata.json", scene_suffix))

    return scene_path


def get_onhand_ids(by_id=False, by_order=False):
    # Connect to DB
    with Postgres(planet_db) as db:
        if scenes_onhand_tbl not in db.list_db_tables():
            onhand = set()
        else:
            # Get all unique IDs, by order or id
            if by_id:
                onhand = set(db.get_values(table=scenes_onhand_tbl,
                                           columns=scenes_id))
            elif by_order:
                onhand = set(db.get_values(table=scenes_onhand_tbl,
                                           columns=order_id))

    return onhand


def get_scenes_noh(data_directory, by_order=False, by_id=False,):
    # Walk data dir, find all .tif files not on hand by order or id
    scenes_to_parse = list()
    for root, dirs, files in os.walk(data_directory):
        if by_order:
            onhand = get_onhand_ids(by_order=by_order)
            logger.debug('Removing onhand order directories from search...')
            for f in files:
                fp = Path(root) / f
                oid = (Path(root) / f).relative_to(data_directory).parts[0]
                if oid not in onhand and \
                        fp.suffix == '.tif' and \
                        not is_udm(fp):
                    scenes_to_parse.append(fp)

        if by_id:
            onhand = get_onhand_ids(by_id=by_id)
            logger.debug('Removing onhand IDs from files to parse...')
            # TODO: this is probably going to be slow
            scenes_to_parse = [Path(root) / f for f in files
                               if not f.startswith(tuple(onhand))]

    return scenes_to_parse


def gdf_from_metadata(metadata_paths: List[pathlib.PurePath]):
    logger.info('Parsing metadata files to populate {}'.format(scenes_onhand_tbl))
    rows = []
    for mp in tqdm(metadata_paths):
        scene_path = scene_path_from_metadata(mp)
        logger.debug('Parsing metdata for: {}'.format(scene_path.stem))
        metadata = json.load(open(mp))
        properties = metadata['properties']
        oid = Path(mp).relative_to(data_directory).parts[0]
        properties[scenes_id] = metadata['id']
        properties[order_id] = oid
        # TODO: Get the scene path, not the metadata path
        if platform.system() == windows:
            properties[win_loc] = str(scene_path)
            properties[tn_loc] = win2linux(str(scene_path))
            if os.path.exists(properties[win_loc]):
                add_row = True
            else:
                add_row = False
        elif platform.system() == linux:
            properties[tn_loc] = str(scene_path)
            properties[win_loc] = linux2win(str(scene_path))
            if os.path.exists(properties[tn_loc]):
                add_row = True
            else:
                logger.debug('.tif does not exist, skipping adding.')
                add_row = False
        properties['geometry'] = Polygon(metadata['geometry']['coordinates'][0])

        if add_row:
            rows.append(properties)

    gdf = gpd.GeoDataFrame(rows, geometry='geometry', crs='epsg:4326')

    return gdf


def update_scenes_onhand(data_directory=data_directory, by_order=True, by_id=False, dryrun=False):
    # Get all scenes to parse
    scenes_to_parse = get_scenes_noh(data_directory=data_directory, by_order=True)

    # Get remaining directories to parse, just for reporting
    order_dirs = {f.relative_to(data_directory).parts[0] for f in scenes_to_parse}
    logger.info('New order directories to parse: {}'.format(len(order_dirs)))

    # Get metadata paths
    logger.info('Parsing metadata files...')
    metadata_paths = [metadata_path_from_scene(s) for s in scenes_to_parse]
    # Remove onhand
    metadata_paths = [m for m in metadata_paths if m.exists()]

    # Parse metadata into rows to add to onhand table
    new_rows = gdf_from_metadata(metadata_paths)

    if not dryrun:
        logger.info('Inserting new records to {}'.format(scenes_onhand_tbl))
        # Insert new records
        with Postgres(planet_db) as db:
            db.insert_new_records(new_rows, table=scenes_onhand_tbl,
                                  unique_id=scenes_id, date_cols=date_cols)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--by_order', action='store_true',
                        help='Parse data directory by order - look only for order '
                             'directories that have not been parased yet.')
    parser.add_argument('--by_id', action='store_true',
                        help='Parse by IDs. Look in all folders for any ID not in scenes_onhand.')
    parser.add_argument('--logfile', type=os.path.abspath,
                        help='Path to write logfile to.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without adding new records.')

    args = parser.parse_args()

    by_order = args.by_order
    by_id = args.by_order
    logfile = args.logfile
    dryrun = args.dryrun

    if not logfile:
        logfile = create_logfile_path(Path(__file__).stem)
    logger = create_logger(__name__, 'fh', 'DEBUG',
                           filename=create_logfile_path(Path(__file__).stem))

    update_scenes_onhand(data_directory=data_directory, by_order=by_order,
                         by_id=by_id, dryrun=dryrun)
