import argparse
import os
from pathlib import Path
import platform
import shutil

import geopandas as gpd
from tqdm import tqdm

from lib.db import Postgres, ids2sql
from lib.lib import get_config, linux2win, read_ids, write_gdf, \
    get_platform_location, PlanetScene
# from shelve_scenes import shelve_scenes
from lib.logging_utils import create_logger

# TODO: Add ability to select by either ID (current method) or filename

logger = create_logger(__name__, 'sh', 'DEBUG')

# Params
db = 'sandwich-pool.planet'
scenes_onhand_table = 'scenes_onhand'
scene_id = 'id'
# TODO: Change this to 'location' and convert to platform specific path in script
location = 'shelved_loc'
platform_location = 'platform_location'
opf = None
required_fields = [scene_id, location]
# Transfer methods
tm_link = 'link'
tm_copy = 'copy'
shelved_base = get_config(location)
if platform.system() == 'Windows':
    shelved_base = Path(linux2win(shelved_base))
else:
    shelved_base = Path(shelved_base)


def load_selection(scene_ids_path=None, footprint_path=None):
    # If path to file of scene ids is provided, look up locations in onhand table
    # Get scene ids
    if scene_ids_path:
        scene_ids = read_ids(scene_ids_path)
        logger.info('Total IDs found: {:,}'.format(len(scene_ids)))
        scene_ids = set(scene_ids)
        logger.info('Unique IDs found: {:,}'.format(len(scene_ids)))

        sql = """
        SELECT * FROM {}
        WHERE {} IN ({})""".format(scenes_onhand_table, scene_id,
                                   ids2sql(scene_ids))
        logger.info('Loading shelved locations from onhand database: '
                    '{}'.format(scenes_onhand_table))
        with Postgres() as db_src:
            gdf = db_src.sql2gdf(sql_str=sql)
            # TODO: Remove this once Postgres restriction on DUPS is
            #  implemented -> there should be no DUPs in scenes2index table
            gdf = gdf.drop_duplicates(subset=scene_id)
            logger.info('IDs found in {}: {:,}'.format(scenes_onhand_table,
                                                     len(gdf)))

    elif footprint_path:
        # Use provided footprint
        gdf = gpd.read_file(footprint_path)
        # Make sure required fields are present
        for field in [location, scene_id]:
            if field not in gdf.columns:
                logger.error('Selection footprint missing required field: '
                             '"{}"').format(field)

    return gdf


def locate_scenes(selection, destination_path):
    # Locate scene files
    logger.info('Locating scene files...')
    # Convert location to correct platform (Windows/Linux) if necessary
    selection[platform_location] = selection[location].apply(
        lambda x: get_platform_location(x))

    # Create glob generators for each scene to find to all scene files
    # (metadata, etc.) e.g. "..\PSScene4Band\20191009_160416_100d*"
    # scene_path_globs = [Path(p).parent.glob('{}*'.format(sid))
    #                     for p, sid in zip(list(selection[platform_location]),
    #                                       list(selection[scene_id]))]
    # scenes2index = [PlanetScene(pl, shelved_parent=destination_path,
    #                       scene_file_source=True)
    #           for pl in selection[platform_location].unique()]
    scenes = []
    for pl in tqdm(selection[platform_location].unique()):
        scenes.append(PlanetScene(pl,
                                  # shelved_parent=destination_path,
                                  scene_file_source=True))

    return scenes

    # src_files = []
    # for g in tqdm(scene_path_globs):
    #     for f in g:
    #         src_files.append(f)
    #
    # logger.info('Source files found: {:,}'.format(len(src_files)))
    #
    # return src_files


def copy_files(scenes, destination_path,
               use_shelved_struct=False,
               transfer_method=tm_copy,
               dryrun=False):
    # Create destination folder structure
    # TODO: Option to build directory tree the same way we will index
    #  (and other options, --opf)
    logger.info('Creating destination paths...')
    src_dsts = []
    for s in tqdm(scenes):
        for sf in s.scene_files:
            src = sf
            if use_shelved_struct:
                # Use same folder structure as shelved data
                dst_suffix = sf.relative_to(shelved_base)
                dst = destination_path / dst_suffix
            else:
                # Flat structure, just add the filename to the destination path
                dst = destination_path / sf.name
            src_dsts.append((src, dst))

    # Remove any destinations that already exist from copy list
    src_dsts = [s_d for s_d in src_dsts if not s_d[1].exists()]

    # Move files
    logger.info('Copying files...')
    pbar = tqdm(src_dsts, desc='Copying...')
    for sf, df in pbar:
        # Check for existence of destination path (can remove, done above)
        if df.exists():
            logger.debug('Destination file exists, skipping: {}'.format(sf.name))
            continue
        if not dryrun:
            if not df.parent.exists():
                os.makedirs(df.parent)
            if transfer_method == tm_link:
                os.link(sf, df)
            else:
                shutil.copy2(sf, df)

    logger.info('File transfer complete.')


def scene_retreiver(scene_ids_path=None,
                    footprint_path=None,
                    destination_path=None,
                    use_shelved_struct=False,
                    transfer_method=tm_copy,
                    out_footprint=None,
                    dryrun=False):
    # Convert string paths to pathlib.Path
    if scene_ids_path:
        scene_ids_path = Path(scene_ids_path)
    if footprint_path:
        footprint_path = Path(footprint_path)
    if destination_path:
        destination_path = Path(destination_path)

    # Load selection
    selection = load_selection(scene_ids_path=scene_ids_path,
                               footprint_path=footprint_path)
    # Locate source files
    scenes = locate_scenes(selection=selection,
                           destination_path=destination_path)
    # Copy to destination
    copy_files(scenes=scenes, destination_path=destination_path,
               use_shelved_struct=use_shelved_struct,
               transfer_method=transfer_method, dryrun=dryrun)

    if out_footprint:
        if footprint_path:
            logger.warning('Footprint selection provided - output footprint '
                           'will be identical.')
        # If IDs were passed and a footprint is desired
        # (otherwise would be identical to input footprint)
        logger.info('Writing footprint to file: {}'.format(out_footprint))
        write_gdf(selection, out_footprint=out_footprint)

        logger.info('Footprint writing complete.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=('Scene retriever for Planet data. Input can either be '
                     'list of IDs or selection from onhand table.'))
    parser.add_argument('--ids', type=os.path.abspath,
                        help='Path to list of IDs to retrieve.')
    parser.add_argument('--footprint', type=os.path.abspath,
                        help='Path to footprint of scenes2index to retreive. Must '
                             'contain field: {}'.format(required_fields))
    parser.add_argument('-d', '--destination', type=os.path.abspath,
                        help='Path to directory to write scenes2index to.')
    parser.add_argument('-uss', '--use_shelved_struct', action='store_true',
                        help='Use the same folder structure in destination '
                             'directory that is used for shelving.')
    parser.add_argument('--out_footprint', type=os.path.abspath,
                        help='Path to write footprint (only useful if '
                             'providing a list of IDs.)')
    parser.add_argument('-tm', '--transfer_method', type=str,
                        choices=[tm_copy, tm_link],
                        help='Transfer method to use.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing copy.')

    args = parser.parse_args()

    scene_ids_path = args.ids
    footprint_path = args.footprint
    destination_path = args.destination
    use_shelved_struct = args.use_shelved_struct
    out_footprint = args.out_footprint
    transfer_method = args.transfer_method
    dryrun = args.dryrun

    scene_retreiver(scene_ids_path=scene_ids_path,
                    footprint_path=footprint_path,
                    destination_path=destination_path,
                    use_shelved_struct=use_shelved_struct,
                    out_footprint=out_footprint,
                    transfer_method=transfer_method,
                    dryrun=dryrun)
