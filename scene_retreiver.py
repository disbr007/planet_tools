import argparse
import os
from pathlib import Path
import pathlib
import shutil

import geopandas as gpd
from tqdm import tqdm

from db_utils import Postgres, ids2sql
from lib import read_ids, write_gdf
from scene_parsing import get_platform_location
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'DEBUG')

# Params
db = 'sandwich-pool.planet'
scenes_onhand_table = 'scenes_onhand'
scene_id = 'id'
# TODO: Change this to 'location' and convert to platform specific path in script
location = 'location'
platform_location = 'platform_location'
opf = None
required_fields = [scene_id, location]
# Transfer methods
tm_link = 'link'
tm_copy = 'copy'


def load_selection(scene_ids_path=None, footprint_path=None):
    # If path to file of scene ids is provided, look up locations in onhand table
    # Get scene ids
    if scene_ids_path:
        scene_ids = read_ids(scene_ids_path)
        logger.info('Total IDs found: {}'.format(len(scene_ids)))
        scene_ids = set(scene_ids)
        logger.info('Unique IDs found: {}'.format(len(scene_ids)))

        sql = """
        SELECT * FROM {}
        WHERE {} IN ({})""".format(scenes_onhand_table, scene_id,
                                   ids2sql(scene_ids))

        with Postgres(db) as db_src:
            gdf = db_src.sql2gdf(sql=sql)
            # TODO: Remove this once Postgres restriction on DUPS is implemented -> there should be no DUPs in scenes table
            gdf = gdf.drop_duplicates(subset=scene_id)
            logger.info('IDs found in {}: {}'.format(scenes_onhand_table, len(gdf)))

    elif footprint_path:
        # Use provided footprint
        gdf = gpd.read_file(footprint_path)
        # Make sure required fields are present
        for field in [location, scene_id]:
            if field not in gdf.columns:
                logger.error('Selection footprint missing required field: "{}"').format(field)

    return gdf


def locate_source_files(selection):
    # Locate scene files
    logger.info('Locating scene files...')
    selection[platform_location] = selection[location].apply(lambda x: get_platform_location(x))
    logger.debug('Platform location: {}'.format(selection[platform_location].values[0]))

    # Create glob generators for each scene to find to all scene files (metadata, etc.)
    # e.g. "..\PSScene4Band\20191009_160416_100d*"
    scene_path_globs = [Path(p).parent.glob('{}*'.format(sid)) for p, sid in zip(list(selection[platform_location]),
                                                                                 list(selection[scene_id]))]

    src_files = []
    for g in tqdm(scene_path_globs):
        for f in g:
            src_files.append(f)

    logger.info('Source files found: {:,}'.format(len(src_files)))
    # TODO: Break down files found by extension

    return src_files


def copy_files(src_files, destination_path, transfer_method=tm_copy, dryrun=False):
    # Create destination folder structure
    # TODO: Option to build directory tree the same way we will index (and other options, --opf)
    if opf:
        pass
    else:
        # Flat structure, just add the filename to the destination path
        src_dsts = [(src, destination_path / src.name) for src in src_files]

    # Move files
    logger.info('Moving files...')
    pbar = tqdm(src_dsts, desc='Copying...')
    for sf, df in pbar:
        logger.debug(sf)
        logger.debug(df)
        # Check for existence of destination path
        if df.exists():
            logger.debug('Destination file exists, skipping: {}'.format(sf.name))
            continue
        if not dryrun:
            if transfer_method == tm_link:
                os.symlink(sf, destination_path)
            else:
                shutil.copy2(sf, destination_path)
        pbar.write('Copied {} -> {}'.format(sf, df))

    logger.info('File transfer complete.')


def scene_retreiver(scene_ids_path=None, footprint_path=None, destination_path=None,
                    transfer_method=tm_copy, out_footprint=None, dryrun=False):
    # Convert string paths to pathlib.Path
    if scene_ids_path:
        scene_ids_path = Path(scene_ids_path)
    if footprint_path:
        footprint_path = Path(footprint_path)
    if destination_path:
        destination_path = Path(destination_path)

    # Load selection
    selection = load_selection(scene_ids_path=scene_ids_path, footprint_path=footprint_path)
    # Locate source files
    src_files = locate_source_files(selection=selection)
    # Copy to destination
    copy_files(src_files=src_files, destination_path=destination_path,
               transfer_method=transfer_method, dryrun=dryrun)

    if out_footprint:
        if footprint_path:
            logger.warning('Footprint selection provided - output footprint will be identical.')
        # If IDs were passed and a footprint is desired (otherwise would be identical to input footprint)
        logger.info('Writing footprint to file: {}'.format(out_footprint))
        write_gdf(selection, out_footprint=out_footprint)

        logger.info('Footprint writing complete.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=('Scene retriever for Planet data. Input can either be '
                                                  'list of IDs of selection from onhand table.'))
    parser.add_argument('--ids', type=os.path.abspath,
                        help='Path to list of IDs to retrieve.')
    parser.add_argument('--footprint', type=os.path.abspath,
                        help='Path to footprint of scenes to retreive. Must contain field: {}'.format(required_fields))
    parser.add_argument('-d', '--destination', type=os.path.abspath,
                        help='Path to directory to write scenes to.')
    parser.add_argument('--out_footprint', type=os.path.abspath,
                        help='Path to write footprint (only useful if providing a list of IDs.')
    parser.add_argument('-tm', '--transfer_method', type=str, choices=[tm_copy, tm_link],
                        help='Transfer method to use.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing copy.')

    args = parser.parse_args()

    scene_ids_path = args.ids
    footprint_path = args.footprint
    destination_path = args.destination
    out_footprint = args.out_footprint
    transfer_method = args.transfer_method
    dryrun = args.dryrun

    scene_retreiver(scene_ids_path=scene_ids_path, footprint_path=footprint_path,
                    destination_path=destination_path, out_footprint=out_footprint,
                    transfer_method=transfer_method, dryrun=dryrun)
