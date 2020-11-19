import argparse
import os
from pathlib import Path
import platform
import shutil
import sys
import time

import geopandas as gpd
from tqdm import tqdm

from lib.db import Postgres
from lib.lib import create_scene_manifests, PlanetScene, get_config
from lib.logging_utils import create_logger, create_logfile_path

logger = create_logger(__name__, 'sh', 'INFO')

# Constants
# Destination directory for shelving
planet_data_dir = get_config('shelve_loc')

# Index table name
index_tbl = 'scenes_onhand'
index_unique_constraint = get_config('db')['tables'][index_tbl]['unique_id']


def determine_copy_fxn(transfer_method):
    if platform.system() == 'Linux' and transfer_method == 'link':
        copy_fxn = os.link
    elif platform.system() == 'Windows' or transfer_method == 'copy':
        copy_fxn = shutil.copy2

    return copy_fxn


def create_all_scene_manifests(directory):
    """
    Finds all master manifests ("*/manifest.json") in the given
    directory, then parses each for the sections corresponding to scenes
    and creates new scene-level ([identifier]_manifest.json) files for
    each scene.
    """
    # Get all master manifests
    master_manifests = set(directory.rglob('manifest.json'))
    logger.info('Master manifests found: '
                '{}'.format(len(master_manifests)))
    logger.debug('Master manifests found:\n'
                 '{}'.format('\n'.join([str(m) for m in master_manifests])))
    # Create manifests for each scene
    logger.info('Creating scene manifests...')
    pbar = tqdm(master_manifests, desc='Creating scene manifests...')
    for mm in pbar:
        pbar.set_description('Creating scene manifests for: '
                             '{}'.format(mm.parent.name))
        # Create scene manifests (*_manifest.json) from a master manifest
        create_scene_manifests(mm, overwrite=False)


def handle_unshelveable(unshelveable, transfer_method, move_unshelveable,
                        remove_sources, dryrun=False):
    """Handle unshelveable scenes based on parameters specified.
    Parameters
    ----------
    unshelveable : list
        List of PlanetScene objects
    transfer_method : str
        Transfer method to use, one of 'link' or 'copy'
    move_unshelveable : str
        If provided, unshelveable data will be moved to this location.
    remove_sources : bool
        If true, unshelveable data will be removed from original 
        locations (after moving if specified)."""
    # Determine whether to copy or link based on transfer method and OS
    copy_fxn = determine_copy_fxn(transfer_method)

    logger.info('Creating list of unshelveable scenes and metadata files...')
    unshelve_src_dst = []
    # Create list of unshelveable files, including metadata files, with
    # destinations if moving
    for ps in unshelveable:
        for src in ps.scene_files:
            if move_unshelveable:
                dst = move_unshelveable / src.name
            else:
                dst = None
            unshelve_src_dst.append((src, dst))

    # Move unshelveable data
    if move_unshelveable:
        logger.info('Moving unshelveable scenes and meta files to: '
                    '{}'.format(move_unshelveable))
        for src, dst in unshelve_src_dst:
            if dryrun:
                continue
            if not dst.exists():
                copy_fxn(src, dst)

    # Remove sources
    if remove_sources:
        logger.info('Removing unshelveable scenes and meta files from '
                    'original locations...')
        for src, dst in unshelve_src_dst:
            if dryrun:
                continue
            try:
                os.remove(src)
            except Exception as e:
                logger.error('Unable to remove file: {}'.format(src))
                logger.error(e)


def shelve_scenes(input_directory, destination_directory=None,
                  scene_manifests_exist=True, verify_checksums=True,
                  transfer_method='copy', remove_sources=False,
                  locate_unshelveable=False, move_unshelveable=None,
                  manage_unshelveable_only=False,
                  cleanup=False,
                  dryrun=False):

    if not destination_directory:
        # Use default data directory
        destination_directory = planet_data_dir

    logger.info('Starting shelving routine...\n')
    logger.info('Source data location: {}'.format(input_directory))
    logger.info('Destination parent directory: '
                '{}'.format(destination_directory))
    logger.info('Scene manifests exists: {}'.format(scene_manifests_exist))
    logger.info('Verify checksums: {}'.format(verify_checksums))
    logger.info('Move unshelveable: {}'.format(move_unshelveable))
    logger.info('Transfer method: {}'.format(transfer_method))
    logger.info('Remove source files: {}'.format(remove_sources))
    logger.info('Dryrun: {}\n'.format(dryrun))

    # To allow cancelling if a parameter is not correct
    time.sleep(5)

    # Create scene-level manifests from master manifests
    if not scene_manifests_exist and not dryrun:
        create_all_scene_manifests(input_directory)
    logger.info('Locating scene manifests...')
    # TODO: Is this an ok way to get all scene manifests?
    # TODO: Speed up - multithread or - capture from create all scene manifests
    scene_manifests = input_directory.rglob('*_manifest.json')

    # Use manifests to create PlanetScene objects, this parses
    # the information in the scene manifest files into attributes
    # (scene_path, md5, bundle_type, received_date, etc.)
    logger.info('Loading scene metadata from scene manifests...')
    scenes = []
    for sm in tqdm(scene_manifests, desc='Creating scenes'):
        scenes.append(PlanetScene(sm, shelved_parent=destination_directory))
    if len(scenes) == 0:
        if dryrun:
            logger.info('No scenes found. Create scene_manifests using '
                        'generate_manifests_only first to proceed with '
                        'rest of dryrun.')
        else:
            logger.error('No scenes found. Are master manifests '
                         '("manifest.json") present in input_directory?\n'
                         'Input_directory: {}'.format(input_directory))
            sys.exit()

    logger.info('Scenes loaded: {:,}'.format(len(scenes)))

    # # Verify checksum, or mark all as skip if not checking
    # if verify_checksums:
    #     # TODO: Multithread this - this is a slow point
    #     logger.info('Verifying scene checksums...')
    #     for ps in tqdm(scenes, desc='Verifying scene checksums...'):
    #         ps.verify_checksum()
    # else:
    #     logger.info('Skipping checksum verification...')
    #     for ps in scenes:
    #         ps.skip_checksum = True

    # Manage unshelveable scenes, i.e don't have valid checksum, associated
    # xml not found, etc.
    # TODO: Make this default
    if locate_unshelveable:
        # Get all indexed IDs to skip any repeats
        indexed_ids = []
        logger.info('Loading indexed IDs...')
        with Postgres('sandwich-pool.planet') as db_src:
            indexed_ids = set(
                db_src.get_values(layer=index_tbl,
                                  columns=index_unique_constraint,
                                  distinct=True)
            )
        logger.debug('Indexed IDs loaded: {:,}'.format((len(indexed_ids))))

        # Locate scenes that are not shelveable, or have already been shelved
        # and indexed
        logger.info('Parsing XML files and locating any unshelveable or '
                    'previously shelved scenes...')
        unshelveable = []
        unshelveable_count = 0
        shelved_count = 0
        indexed_count = 0
        for ps in tqdm(scenes, desc='Parsing XML files:'):
            # skip_shelving = {'Unshelveable': False,
            #                  'Shelved': False,
            #                  'Indexed': False}
            if ps.is_shelved:
                # skip_shelving['Shelved'] = True
                shelved_count += 1
            if ps.identifier in indexed_ids:
                ps.indexed = True
                # skip_shelving['Indexed'] = True
                indexed_count += 1
            if ps.is_shelved and ps.indexed:
            # if skip_shelving['Shelved'] and skip_shelving['Indexed']:
                continue
            if not ps.shelveable:
                try:
                    logger.warning('UNSHELVABLE: {}'.format(ps.scene_path))
                    logger.debug('Scene exists: {}'.format(ps.scene_path.exists()))
                    logger.debug('Checksum: {}'.format(ps.verify_checksum() if
                                                       not ps.skip_checksum
                                                       else ps.skip_checksum))
                    logger.debug('XML Path: {}'.format(ps.xml_path))
                    logger.debug('XML parseable: {}'.format(ps.xml_valid))
                    logger.debug('Instrument: {}'.format(ps.instrument))
                    logger.debug('Product Type: {}'.format(ps.product_type))
                    logger.debug('Bundle type: {}'.format(ps.bundle_type))
                    logger.debug('Acquired: {}'.format(ps.acquisition_datetime))
                    logger.debug('Strip ID: {}'.format(ps.strip_id))
                except Exception as e:
                    logger.debug(e)
                # skip_shelving['Unshelveable'] = True
                unshelveable_count += 1

            # Add to list to skip if scene is unshelveable, or it is BOTH
            # shelved and indexed
            logger.info('{} {} {}'.format(ps.is_shelved, ps.indexed, ps.shelveable))
            if (ps.is_shelved and ps.indexed) or not ps.shelveable:
                logger.info('adding to unshelveable')
            # if (skip_shelving['Unshelveable'] or
            #         (skip_shelving['Shelved'] and
            #          skip_shelving['Indexed'])):
                unshelveable.append(ps)

        logger.info('Already shelved scenes found: {:,}'.format(shelved_count))
        logger.info('Already indexed scenes found: {:,}'.format(indexed_count))
        logger.info('Unshelveable scenes: {:,}'.format(unshelveable_count))

        # Remove unshelvable scenes from directory to shelve (optionally move)
        if len(unshelveable) > 0:
            logger.info('Total scenes not being shelved: '
                        '{:,}'.format(len(unshelveable)))
            handle_unshelveable(unshelveable,
                                transfer_method=transfer_method,
                                move_unshelveable=move_unshelveable,
                                remove_sources=remove_sources,
                                dryrun=dryrun)

            # Remove unshelveable scenes from list of scenes to shelve
            for unsh_ps in unshelveable:
                if unsh_ps in scenes:
                    scenes.remove(unsh_ps)
                else:
                    logger.warning('Unable to remove unshelveable scene from '
                                   'list of scenes to shelve: '
                                   '{}'.format(unsh_ps.scene_path))

    if manage_unshelveable_only:
        logger.info('Managing unshelveable scenes complete, exiting.')
        sys.exit()

    logger.info('Remaining scenes to shelve: {:,}'.format(len(scenes)))
    if len(scenes) == 0:
        return

    # Create list of tuples of (src, dst) where dst is shelved location
    logger.info('Determining shelved destinations...')
    srcs_dsts = []
    for ps in tqdm(scenes, desc='Determining shelved destinations'):
        # TODO: verify required meta files are present in scene files?
        try:
            move_list = [(sf, ps.shelved_dir / sf.name) for sf in
                         ps.scene_files if sf is not None]
        except Exception as e:
            logger.error('Error locating scene files.')
            logger.error(e)
            for sf in ps.scene_files:
                logger.error(sf.name)
        srcs_dsts.extend(move_list)

    logger.info('Copying scenes to shelved locations...')

    # Determine copy function based on platform
    copy_fxn = determine_copy_fxn(transfer_method)
    prev_order = None  # for logging only
    for src, dst in tqdm(srcs_dsts):
        # Log the current order directory being parsed
        current_order = src.relative_to(input_directory).parts[0]
        if current_order != prev_order:
            logger.info('Shelving order directory: {}'.format(current_order))
        # Go no further if dryrun
        if dryrun:
            prev_order = current_order
            continue
        # Perform copy
        if not dst.parent.exists():
            os.makedirs(dst.parent)
        if not dst.exists():
            try:
                copy_fxn(src, dst)
            except Exception as e:
                logger.error('Error copying:\n{}\n\t-->{}'.format(src, dst))
                logger.error(e)
        else:
            logger.debug('Destination exists, skipping: {}'.format(dst))
        prev_order = current_order

    # Remove source files that were moved
    if remove_sources:
        logger.info('Removing source files...')
        for src, dst in tqdm(srcs_dsts, desc='Removing source files'):
            if dryrun:
                continue
            if dst.exists():
                try:
                    os.remove(src)
                except Exception as e:
                    logger.error('Error removing {}'.format(src))
                    logger.error(e)
            else:
                logger.warning('Skipping removal of source file as shelved '
                               'location could not be found: {}'.format(src))
    if cleanup:
        logger.info('Cleaning up any remaining files...')
        for root, dirs, files in os.walk(input_directory):
            if dryrun:
                continue
            for f in files:
                src = Path(root) / Path(f)
                dst = Path(move_unshelveable) / Path(f)
                if move_unshelveable:
                    logger.debug('Moving remaining file: {}'.format(src))
                    copy_fxn(src, dst)
                    logger.debug('Deleting file: {}'.format(src))
                    os.remove(src)
                elif remove_sources:
                    logger.debug('Deleting file: {}'.format(src))
                    os.remove(src)

    return scenes


def index_scenes(scenes, index_tbl=index_tbl, dryrun=False):
    # TODO: Pop this out to it's own script that can index
    #  any scenes, then just import
    logger.info('Building index rows for shelveable scenes: '
                '{:,}'.format(len(scenes)))
    gdf = gpd.GeoDataFrame([s.index_row for s in scenes if s.shelveable],
                           geometry='geometry',
                           crs='epsg:4326')

    logger.info('Indexing shelveable scenes: {}'.format(len(scenes)))
    with Postgres('sandwich-pool.planet') as db_src:
        db_src.insert_new_records(gdf,
                                  table=index_tbl,
                                  dryrun=dryrun)


def main(args):
    input_directory = Path(args.input_directory)
    destination_directory = Path(args.destination_directory)
    scene_manifests_exist = args.scene_manifests_exist
    locate_unshelveable = args.locate_unshelveable
    move_unshelveable = (Path(args.move_unshelveable)
                         if args.move_unshelveable is not None else None)
    verify_checksums = not args.skip_checksums

    run_indexer = args.index_scenes
    transfer_method = args.transfer_method
    remove_sources = args.remove_sources
    cleanup = args.cleanup
    logdir = args.logdir
    dryrun = args.dryrun

    generate_manifests_only = args.generate_manifests_only
    manage_unshelveable_only = args.manage_unshelveable_only

    if logdir:
        logfile = create_logfile_path('shelve_scenes', logdir)
        logger = create_logger(__name__, 'fh', 'DEBUG', filename=logfile)

    # Verify arguments
    if not input_directory.exists():
        logger.error(
            'Data directory does not exists: {}'.format(input_directory))
        sys.exit()
    if not destination_directory.exists():
        logger.error('Destination directory does '
                     'not exist: {}'.format(destination_directory))
        sys.exit()
    if platform.system() == 'Windows' and transfer_method == 'link':
        logger.error('Transfer method "link" not compatible with Windows '
                     'platforms. Please use "copy".')
        sys.exit()
    if generate_manifests_only and not dryrun:
        # Just create scene manifests and exit
        logger.info('Creating scene manifests for all master manifests '
                    'in: {}'.format(input_directory))
        create_all_scene_manifests(input_directory)
        sys.exit()

    scenes = shelve_scenes(input_directory,
                           destination_directory=destination_directory,
                           scene_manifests_exist=scene_manifests_exist,
                           verify_checksums=verify_checksums,
                           locate_unshelveable=locate_unshelveable,
                           move_unshelveable=move_unshelveable,
                           manage_unshelveable_only=manage_unshelveable_only,
                           transfer_method=transfer_method,
                           remove_sources=remove_sources,
                           cleanup=cleanup,
                           dryrun=dryrun)

    if (scenes is not None) and (len(scenes) != 0) and run_indexer:
        index_scenes(scenes, index_tbl=index_tbl, dryrun=dryrun)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    alt_routine_group = parser.add_argument_group('Alternative Routines')

    parser.add_argument('-i', '--input_directory', type=os.path.abspath,
                        required=True,
                        help='Directory holding data to shelve.')
    # TODO: remove this arg and make a default
    parser.add_argument('--destination_directory', type=os.path.abspath,
                        default=planet_data_dir,
                        help='Base directory upon which to build filepath.')
    parser.add_argument('-sme', '--scene_manifests_exist', action='store_true',
                        help='Use to specify that scene manifests exist '
                             'and recreating is not necessary or not '
                             'possible, i.e. there are no master manifests.')
    parser.add_argument('--skip_checksums', action='store_true',
                        help='Skip verifying checksums, all new scenes found '
                             'in data directory will be moved to destination.')
    parser.add_argument('-tm', '--transfer_method', choices=['link', 'copy'],
                        default='copy',
                        help='Method to use for transfer.')
    parser.add_argument('-rs',  '--remove_sources', action='store_true',
                        help='Use flag to delete source files after shelving. '
                             'Otherwise source files will be left in '
                             'input_directory.')
    parser.add_argument('-lu', '--locate_unshelveable', action='store_true',
                        help='Locate unshelveable data and handle accourding '
                             'to move_unshelveable argument.')
    parser.add_argument('-mu', '--move_unshelveable', type=os.path.abspath,
                        help='If provided, move unshelveable files to this '
                             'location. If not provided and '
                             'locate_unshelveable, source files are '
                             'deleted.')
    parser.add_argument('--cleanup', action='store_true',
                        help='Move / remove any remaining files in '
                             'input_directory after shelving and moving any '
                             'unshelveable scenes. This catches any files '
                             'that were not associated with a master '
                             'manifest.')

    parser.add_argument('--index_scenes', action='store_true',
                        # TODO: Add name of index table
                        help='Add shelveable scenes to index table after '
                             'performing shelving.')

    # ALternative routines
    alt_routine_group.add_argument('--generate_manifests_only', action='store_true',
                              help='Only generate scene manifests from master '
                                   'manifests, do not perform copy operation. '
                                   'This is done as part of the copy routine, '
                                   'but this flag can be used to create scene '
                                   'manifests without copying.')
    alt_routine_group.add_argument('--manage_unshelveable_only',
                              action='store_true',
                              help='Move or remove unshelveable data and exit.')

    parser.add_argument('--logdir', type=os.path.abspath,
                        help='Path to write logfile to.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing.')

    # TODO: --include_pan (support shelving pan images -> duplicate xmls)

    # For debugging
    # sys.argv = ['shelve_scenes.py',
    #             '--input_directory',
    #             # r'V:\pgc\data\scratch\jeff\projects\planet\scratch'
    #             # r'\test_order\1fb3047b-705e-4ed0-b900-a86110b82dca',
    #             r'E:\disbr007\projects\planet\scratch\test_order',
    #             '--destination_directory',
    #             r'E:\disbr007\projects\planet\shelved',
    #             # r'V:\pgc\data\scratch\jeff\projects\planet\shelved',
    #             '--locate_unshelveable',
    #             # '--move_unshelveable',
    #             # r'E:\disbr007\projects\planet\unshelv',
    #             # r'V:\pgc\data\scratch\jeff\projects\planet\scratch\unshelv',
    #             '--skip_checksums',
    #             '--index_scenes',
    #             '-sme', '-h']
    #             # '--logdir',
    #             # r'V:\pgc\data\scratch\jeff\projects\planet\logs']

    args = parser.parse_args()

    main(args)
