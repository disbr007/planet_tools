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

subloggers = ['lib.db', 'lib.lib']

logger = create_logger(__name__, 'sh', 'INFO')
for sl in subloggers:
    create_logger(sl, 'sh', 'INFO')

# Constants
# Destination directory for shelving
planet_data_dir = get_config('shelve_loc')

# Index table name and unique constraint
index_tbl = 'scenes_onhand'
index_unique_constraint = get_config('db')['tables'][index_tbl]['unique_id']


def determine_copy_fxn(transfer_method):
    """
    Verify that transfer_method provided is valid for OS.

    Parameters
    ---------
    transfer_method : str
        'copy' or 'link'

    Returns
    --------
    copy_fxn : function
        Function that takes two arguments, source and destination
    """
    if platform.system() == 'Linux' and transfer_method == 'link':
        copy_fxn = os.link
    elif platform.system() == 'Windows':
        if transfer_method == 'link':
            logger.warning("Transfer method 'link' not valid on "
                           "Windows, defaulting to 'copy'.")
        copy_fxn = shutil.copy2

    return copy_fxn


def create_all_scene_manifests(directory):
    """
    Finds all master manifests ('manifest.json') in the given directory,
    then parses each for the sections corresponding to scenes and
    creates new scene-level ([identifier]_manifest.json) files for each
    scene, adjacent to the rest of the scene files.

    Parameters
    ---------
    directory : pathlib.Path, str
        Path to directory to parse for order-level manifests.
    Returns
    ---------
    None
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
    """
    Handle unshelveable scenes based on parameters specified.

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
        locations (after moving if specified).

    Returns
    ---------
    None
    """
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
                  scene_manifests_exist=True,
                  verify_checksums=True,
                  transfer_method='copy',
                  remove_sources=False,
                  move_unshelveable=None,
                  manage_unshelveable_only=False,
                  cleanup=False,
                  dryrun=False):
    """
    Shelve all Planet scenes found in the input_directory. Scenes are
    located by their scene-level manifest files
    ([scene_identifier]_manifest.json),
    which are created from order-level manifest files,
    which are located by as 'manifest.json' files in the input_directory.

    Parameters
    ----------
    input_directory : pathlib.Path, str
        Path to directory to parse for Planet scenes.
    destination_directory : pathlib.Path, str
        Path to parent directory under which to shelve scenes.
    scene_manifests_exist : bool
        Set to True if scene-level manifest files
        ([scene_identifier]_manifest.json files have already been
        created from master manifests.
        TODO: option to just create scene-level manifests and exit
    verify_checksums : bool
        True to compute checksums and verify against values in
        order-manifests (which are passed to scene-manifests.
    transfer_method : str
        'copy', 'link', 'move'
    remove_sources : bool
        True to delete sources after shelving.
    move_unshelveable : pathlib.Path, str, None
        Path to move unshelveable to. If None, unshelveable scenes will
        be deleted.
    manage_unshelveable_only : bool
        Determine unshelveable scenes and handle according to
        move_unshelveable, then exit.
    cleanup : bool
        True to remove any remaining files after shelving and managing
        unshelveable.
    dryrun : bool
        Locate scenes, determine if shelveable,

    Returns
    -------
    list : list of PlanetScene objects that were shelveable
    """

    # Use default directory if destination directory not provided
    if not destination_directory:
        # Use default data directory
        destination_directory = planet_data_dir

    # Convert to pathlib.Path objects if necessary
    if not isinstance(input_directory, Path):
        input_directory = Path(input_directory)
    if not isinstance(destination_directory, Path):
        destination_directory = Path(destination_directory)

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
    # TODO: Is this an ok way to get all scene-level manifests?
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

    # Manage unshelveable scenes, i.e don't have valid checksum, associated
    # xml not found, etc.
    # Get all indexed IDs to skip reindexing
    logger.info('Loading indexed IDs...')
    with Postgres() as db_src:
        indexed_ids = set(
            db_src.get_values(table=index_tbl,
                              columns=index_unique_constraint,
                              distinct=True)
        )
    logger.debug('Indexed IDs loaded: {:,}'.format((len(indexed_ids))))

    # Locate scenes that are not shelveable, or have already been shelved
    # and indexed
    # TODO: Speed up, multiprocess?
    logger.info('Parsing XML files and verifying checksums...')
    skip_scenes = []
    unshelveable_count = 0
    shelved_count = 0
    indexed_count = 0
    bad_checksum_count = 0
    for ps in tqdm(scenes, desc='Parsing XML files and verifying checksums:'):
        # Check if scene is shelveable or has been shelved and indexed
        # first to avoid verifying checksums for scenes that don't are
        # unshelveable or don't need to be reshelved
        if not ps.shelveable:
            try:
                logger.warning('UNSHELVABLE: {}'.format(ps.scene_path))
                logger.debug('Scene exists: {}'.format(ps.scene_path.exists()))
                logger.debug('XML Path: {}'.format(ps.xml_path))
                logger.debug('XML parseable: {}'.format(ps.xml_valid))
                logger.debug('Instrument: {}'.format(ps.instrument))
                logger.debug('Product Type: {}'.format(ps.product_type))
                logger.debug('Bundle type: {}'.format(ps.bundle_type))
                logger.debug('Acquired: {}'.format(ps.acquisition_datetime))
                logger.debug('Strip ID: {}'.format(ps.strip_id))
            except Exception as e:
                logger.debug(e)
            unshelveable_count += 1
            skip_scenes.append(ps)
            continue
        if ps.is_shelved:
            shelved_count += 1
        if ps.identifier in indexed_ids:
            ps.indexed = True
            indexed_count += 1
        if ps.is_shelved and ps.indexed:
            skip_scenes.append(ps)
            continue
        if verify_checksums:
            if not ps.verify_checksum():
                logger.warning('Invalid checksum: '
                               '{}'.format(ps.scene_path))
                bad_checksum_count += 1
                skip_scenes.append(ps)

    # Report counts of scenes that will not be shelved
    logger.info('Already shelved scenes found:  {:,}'.format(shelved_count))
    logger.info('Already indexed scenes found:  {:,}'.format(indexed_count))
    logger.info('Unshelveable (bad attributes): {:,}'.format(unshelveable_count))
    logger.info('Bad checksums:                 {:,}'.format(bad_checksum_count))

    # Remove skippable scenes from directory to shelve (optionally move)
    if len(skip_scenes) > 0:
        logger.info('Total scenes not being shelved: '
                    '{:,}\n'.format(len(skip_scenes)))
        handle_unshelveable(skip_scenes,
                            transfer_method=transfer_method,
                            move_unshelveable=move_unshelveable,
                            remove_sources=remove_sources,
                            dryrun=dryrun)

        # Remove skippable scenes from list of scenes to shelve
        for unsh_ps in skip_scenes:
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
        # TODO: verify required meta files are present in scene files,
        #  using list of expected files? [_metadata.json, .xml, etc]
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
    pbar = tqdm(srcs_dsts)
    for src, dst in pbar:
        # Log the current order directory being parsed
        current_order = src.relative_to(input_directory).parts[0] # logging only
        if current_order != prev_order:
            logger.debug('Shelving order directory: {}'.format(current_order))
            pbar.set_description('Shelving order directory: '
                                 '{}'.format(current_order))
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

    # Remove source files that were shelved
    if remove_sources:
        logger.info('Removing source files...')
        for src, dst in tqdm(srcs_dsts, desc='Removing source files'):
            if dryrun:
                continue
            # Confirm shelved location exists before removing
            if dst.exists():
                try:
                    os.remove(src)
                except Exception as e:
                    logger.error('Error removing {}'.format(src))
                    logger.error(e)
            else:
                logger.warning('Skipping removal of source file - shelved '
                               'location could not be found: {}'.format(src))

    # TODO not sure what files would be remaining, but this removes them (or
    #  moves them, according to move_unshelvable)
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
    logger.info('Building index rows for shelveable scenes: '
                '{:,}'.format(len(scenes)))
    gdf = gpd.GeoDataFrame([s.index_row for s in scenes if s.shelveable],
                           geometry='geometry',
                           crs='epsg:4326')

    logger.info('Indexing shelveable scenes: {:,}'.format(len(scenes)))
    with Postgres() as db_src:
        db_src.insert_new_records(gdf,
                                  table=index_tbl,
                                  dryrun=dryrun)


def main(args):
    # Parse arguments, convert to pathlib.Path objects
    input_directory = Path(args.input_directory)
    destination_directory = Path(args.destination_directory)
    scene_manifests_exist = args.scene_manifests_exist
    move_unshelveable = (Path(args.move_unshelveable)
                         if args.move_unshelveable is not None
                         else None)
    verify_checksums = not args.skip_checksums
    transfer_method = args.transfer_method
    remove_sources = args.remove_sources
    cleanup = args.cleanup
    run_indexer = args.index_scenes
    logdir = args.logdir
    dryrun = args.dryrun
    # Alternative routine arguments
    generate_manifests_only = args.generate_manifests_only
    manage_unshelveable_only = args.manage_unshelveable_only

    if logdir:
        logfile = create_logfile_path('shelve_scenes', logdir)
        logger = create_logger(__name__, 'fh', 'DEBUG', filename=logfile)
        for sl in subloggers:
            create_logger(sl, 'fh', 'DEBUG', filename=logfile)

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

    # Locate and shelve all shelveable scenes and move or delete unshelveable
    # scenes. Returns PlanetScene objects which were shelveable, which are
    # parsed by index_scenes
    scenes = shelve_scenes(input_directory,
                           destination_directory=destination_directory,
                           scene_manifests_exist=scene_manifests_exist,
                           verify_checksums=verify_checksums,
                           move_unshelveable=move_unshelveable,
                           manage_unshelveable_only=manage_unshelveable_only,
                           transfer_method=transfer_method,
                           remove_sources=remove_sources,
                           cleanup=cleanup,
                           dryrun=dryrun)

    # Add all scenes that were shelved to index
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
    # TODO: index_only option?
    parser.add_argument('--logdir', type=os.path.abspath,
                        help='Path to write logfile to.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing.')

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
