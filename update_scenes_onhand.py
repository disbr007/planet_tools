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

# Args
by_id = False
by_order = True
parse_directory = None

logger = create_logger(__name__, 'sh', 'INFO')
# logger = create_logger(__name__, 'fh', 'DEBUG',
                       # filename=create_logfile_path(Path(__file__).stem))

planet_db = 'sandwich-pool.planet'
scenes_onhand_tbl = 'scenes_onhand'
scenes_id = 'id'
order_id = 'order_id'
data_directory = Path(r'V:\pgc\data\scratch\jeff\projects\planet\data')
item_types = ['PSScene3Band', 'PSScene4Band']
date_cols = ['acquired']

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
    # TODO: redo this to take scene path -> scene_id -> metadata
    par_dir = scene.parent
    sid = id_from_scene(scene)
    metadata_path = par_dir / '{}_metadata.json'.format(sid)

    return metadata_path


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
    rows = []
    for mp in tqdm(metadata_paths):
        logger.debug('Parsing metdata for: {}'.format(mp.stem))
        metadata = json.load(open(mp))
        properties = metadata['properties']
        oid = Path(mp).relative_to(data_directory).parts[0]
        properties['id'] = metadata['id']
        properties['order_id'] = oid
        if platform.system() == windows:
            properties[win_loc] = str(mp)
            properties[tn_loc] = win2linux(str(mp))
        elif platform.system() == linux:
            properties[tn_loc] = str(mp)
            properties[win_loc] = linux2win(str(mp))
        properties['geometry'] = Polygon(metadata['geometry']['coordinates'][0])

        rows.append(properties)

    gdf = gpd.GeoDataFrame(rows, geometry='geometry', crs='epsg:4326')

    return gdf


# Get all scenes to parse
scenes_to_parse = get_scenes_noh(data_directory=data_directory, by_order=True)

# Get remaining directories to parse, just for reporting
order_dirs = {f.relative_to(data_directory).parts[0] for f in scenes_to_parse}
logger.info('Remaining order directories: {}'.format(len(order_dirs)))

# Get metadata paths
logger.info('Parsing metadata files...')
metadata_paths = [metadata_path_from_scene(s) for s in scenes_to_parse]
# Remove onhand
metadata_paths = [m for m in metadata_paths if m.exists()]

# Parse metadata into rows to add to onhand table
new_rows = gdf_from_metadata(metadata_paths)

# Insert new records
with Postgres(planet_db) as db:
    db.insert_new_records(new_rows, table=scenes_onhand_tbl,
                          unique_id=scenes_id, date_cols=date_cols)


# TODO: with argparse:
if by_id and by_order:
    logger.error('Cannot parse by both id and order, choose one.')

