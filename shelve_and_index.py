import argparse
import os
from pathlib import Path
import platform
import shutil
import sys
import time
from typing import Union, List, Tuple, Callable

import geopandas as gpd
from tqdm import tqdm

# from lib.db import Postgres
from lib.lib import create_scene_manifests, PlanetScene, get_config, linux2win
from lib.logging_utils import create_logger, create_logfile_path

logger = create_logger(__name__, 'sh', 'INFO')

# External modules
sys.path.append(str(Path(__file__).parent / '..'))
try:
    from db_utils.db import Postgres, generate_sql
except ImportError as e:
    logger.error('db_utils module not found. It should be adjacent to '
                 'the planet_tools directory. Path: \n{}'.format(sys.path))
    sys.exit()

# Constants
WINDOWS = 'Windows'
LINUX = 'Linux'
COPY = 'copy'
LINK = 'link'
# Databse
SANDWICH = 'sandwich'
PLANET = 'planet'

# config.json keys
SHELVED_LOC = 'shelved_loc'

# Destination directory for shelving
PLANET_DATA_DIR = get_config(SHELVED_LOC)
if platform.system() == WINDOWS:
    PLANET_DATA_DIR = linux2win(PLANET_DATA_DIR)

# Filename of order-level manifests, used for locating
MANIFEST_JSON = 'manifest.json'

# Index table name and unique constraint
INDEX_TBL = 'scenes_onhand'
INDEX_UNIQUE_CONSTRAINT = get_config('db')['tables'][INDEX_TBL]['unique_id']
INDEX_GEOM = 'geometry'
INDEX_CRS = 'epsg:4326'


def determine_copy_fxn(transfer_method: str) -> Callable:
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
    if platform.system() == LINUX and transfer_method == LINK:
        copy_fxn = os.link
    elif platform.system() == WINDOWS:
        if transfer_method == LINK:
            logger.warning("Transfer method {} not valid on "
                           "{}, defaulting to 'copy'.".format(LINK, WINDOWS))
        copy_fxn = shutil.copy2

    return copy_fxn


def verify_args(input_directory: Union[str, Path],
                destination_directory: Union[str, Path],
                transfer_method: str) -> Tuple[Path, Path]:
    """
    Verify input arguments, converting string paths to
    pathlib.Path objects as necessary.

    Parameters
    ----------
    input_directory: str, Path
    destination_directory: str, Path
    transfer_method: str

    Returns
    -------
    tuple: Path(input_directory), Path(destination_directory)
    """
    # Convert to pathlib.Path objects if necessary
    if not isinstance(input_directory, Path):
        input_directory = Path(input_directory)
        if not input_directory.exists():
            logger.error('Input directory does not exist: '
                         '{}'.format(input_directory))
            sys.exit(-1)
    # Use default directory if destination directory not provided
    if not destination_directory:
        # Use default data directory
        destination_directory = PLANET_DATA_DIR
    if not isinstance(destination_directory, Path):
        destination_directory = Path(destination_directory)
        if not destination_directory.exists():
            logger.error('Destination directory does not exist: '
                         '{}'.format(destination_directory))
            sys.exit(-1)
    # Confirm platform and transfer method are compatible
    if platform.system() == WINDOWS and transfer_method == LINK:
        logger.error('Transfer method "{}" not compatible with {} '
                     'platforms. Please use "{}".'.format(LINK, WINDOWS, COPY))
        sys.exit(-1)

    return input_directory, destination_directory


def create_all_scene_manifests(directory: Union[Path, str]) -> List[Path]:
    """
    Finds all order manifests ('manifest.json') in the given directory,
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
    if not isinstance(directory, Path):
        directory = Path(directory)

    # Get all order manifests
    order_manifests = set(directory.rglob(MANIFEST_JSON))
    logger.info('Order manifests found: '
                '{}'.format(len(order_manifests)))
    logger.debug('Order manifests found:\n'
                 '{}'.format('\n'.join([str(m) for m in order_manifests])))

    # Iterate over order-manifests and create scene-manifests for each scene
    logger.info('Creating scene manifests...')
    pbar = tqdm(order_manifests, desc='Creating scene manifests')
    scene_manifests = []
    for mm in pbar:
        pbar.set_description('Creating scene manifests for: '
                             '{}'.format(mm.parent.name))
        # Create scene manifests (*_manifest.json) from a order manifest
        scene_manifests_order = create_scene_manifests(mm, overwrite=False)
        scene_manifests.extend(scene_manifests_order)

    return scene_manifests


def create_scenes(scene_manifests: list,
                  destination_directory: Path) -> List[PlanetScene]:
    """
    Creates a list of PlanetScene's from a list of scene
    manifests.
    Parameters
    ----------
    scene_manifests: list
        List of paths to scene manifest files
    destination_directory: Path
        Parent path to use for shelveing. Often the PGC shelved
        location, but also used for copying scenes to other
        locations.

    Returns
    -------
    list: list of PlanetScene objects

    """
    logger.info('Locating scene manifests...')

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
            logger.error('No scenes found. Are order manifests '
                         '("manifest.json") present in input_directory?\n'
                         'Input_directory: {}'.format(input_directory))
            sys.exit()

    logger.info('Scenes loaded: {:,}'.format(len(scenes)))

    return scenes


def identify_shelveable_indexable(scenes: List[PlanetScene]) -> Tuple[list]:
    """
    Identify which scenes are shelveable and/or indexable, and
    which are neither shelveable nor indexable.
    Shelveable scenes have both an XML file and scene manifest
    file present that can be parsed for all metadata necessary to
    create the shelved path, and don't exist at the shelved
    location.
    Indexable scenes require the same, plus that the scene is not
    already present in the index.
    Skippable scenes are those that do not have the necessary
    metadata file present, or are both shelved and indexed
    already, and thus can be skipped from subsequent shelving
    and indexing steps.

    Parameters
    ----------
    scenes: list
        List of PlanetScene objects

    Returns
    -------
    list, list, list: lists of PlanetScenes that can be shelved,
        indexed, and skipped respectively.
    """
    # Get all indexed IDs to skip reindexing
    logger.info('Loading indexed IDs...')
    with Postgres(host=SANDWICH, database=PLANET) as db_src:
        indexed_ids = set(
            db_src.get_values(table=INDEX_TBL,
                              columns=INDEX_UNIQUE_CONSTRAINT,
                              distinct=True)
        )
    logger.debug('Indexed IDs loaded: {:,}'.format((len(indexed_ids))))

    # Locate scenes that are not shelveable, or have already been shelved
    # and indexed
    # TODO: Speed up, multiprocess?
    logger.info('Parsing XML files and verifying checksums...')
    scenes2skip = []
    scenes2shelve = []
    scenes2index = []
    unshelveable_count = 0
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
            scenes2skip.append(ps)
            continue
        # Check if already shelved and/or already indexed. If both, skip the
        # scene. If only one or the other, continue processing.
        if not ps.is_shelved:
            scenes2shelve.append(ps)
        if ps.identifier not in indexed_ids:
            ps.indexed = False
            scenes2index.append(ps)
        else:
            ps.indexed = True
        if ps.is_shelved and ps.indexed:
            scenes2skip.append(ps)
            continue

        # Verify checksums
        if verify_checksums:
            if not ps.verify_checksum():
                logger.warning('Invalid checksum: '
                               '{}'.format(ps.scene_path))
                bad_checksum_count += 1
                scenes2skip.append(ps)

    # Report counts of scenes that will not be shelved
    logger.info('Already shelved scenes found:  {:,}'.format(len(scenes) - len(scenes2shelve)))
    logger.info('Already indexed scenes found:  {:,}'.format(len(scenes) - len(scenes2index)))
    logger.info('Unshelveable (bad attributes): {:,}'.format(unshelveable_count))
    logger.info('Bad checksums:                 {:,}'.format(bad_checksum_count))

    return scenes2shelve, scenes2index, scenes2skip


def handle_unshelveable(unshelveable: list,
                        transfer_method: str,
                        copy_unshelveable: str,
                        remove_sources: bool,
                        dryrun: bool = False) -> None:
    """
    Handle unshelveable scenes based on parameters specified.

    Parameters
    ----------
    unshelveable : list
        List of PlanetScene objects
    transfer_method : str
        Transfer method to use, one of 'link' or 'copy'
    copy_unshelveable : str
        If provided, unshelveable data will be moved to this location.
    remove_sources : bool
        If true, unshelveable data will be removed from original 
        locations (after moving if specified).
    dryrun: bool

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
            if copy_unshelveable:
                dst = copy_unshelveable / src.name
            else:
                dst = None
            unshelve_src_dst.append((src, dst))

    # Move unshelveable data
    if copy_unshelveable:
        logger.info('Moving unshelveable scenes and meta files to: '
                    '{}'.format(copy_unshelveable))
        for src, dst in unshelve_src_dst:
            if dryrun:
                continue
            if not dst.exists():
                copy_fxn(src, dst)

    # Remove sources
    if remove_sources:
        logger.info('Removing unshelveable scenes and meta files from original '
                    'locations...')
        for src, dst in unshelve_src_dst:
            if dryrun:
                continue
            try:
                os.remove(src)
            except Exception as e:
                logger.error('Unable to remove file: {}'.format(src))
                logger.error(e)


def shelve_scenes(scenes2shelve: List[PlanetScene],
                  transfer_method: str = COPY,
                  remove_sources: bool = False,
                  dryrun: bool = False) -> None:
    """
    Shelve scenes.

    Parameters
    ----------
    scenes2shelve: list
        List of PlanetScene objects.
    transfer_method: str
        One of 'copy' or 'link'
    remove_sources: bool
        Remove source files after copying and confirming they
        exist at the shelved location. In the case of
        unshelveable scenes, they will be deleted if this is
        True.
    dryrun: bool
        True to not perform copy.

    Returns
    -------

    """
    # Create list of tuples of (src, dst) where dst is shelved location for
    # all shelveable scenes
    logger.info('Determining shelved destinations...')
    srcs_dsts = []
    for ps in tqdm(scenes2shelve, desc='Determining shelved destinations'):
        # TODO: verify required meta files are present in scene files,
        #  using list of expected files? [_metadata.json, .xml, etc]
        try:
            # Create shelved destination paths for all files associated with
            # scene (_metadata.json, .xml, manifest.json, etc.)
            move_list = [(sf, ps.shelved_dir / sf.name) for sf in
                         ps.scene_files if sf is not None]
        except Exception as e:
            logger.error('Error locating scene files.')
            logger.error(e)
            for sf in ps.scene_files:
                logger.error(sf.name)
        srcs_dsts.extend(move_list)

    # Perform shelve by copying scenes to destination
    logger.info('Copying scene files to shelved locations...')
    copy_fxn = determine_copy_fxn(transfer_method)
    prev_order = None  # for logging only
    pbar = tqdm(srcs_dsts)
    for src, dst in pbar:
        # Log the current order directory being parsed
        # current_order = src.relative_to(input_directory).parts[0] # logging only
        current_order = src.parent.parent.parts[-1]
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

    # TODO: not sure what files would be remaining, but this removes them (or
    #  moves them, according to move_unshelvable)


def index_scenes(scenes2index: List[PlanetScene],
                 index_tbl: str = INDEX_TBL,
                 dryrun: bool = False) -> None:
    """
    Inserts scenes into planet.scenes_onhand

    Parameters
    ----------
    scenes2index: list
        List of PlanetScene objects.
    index_tbl: str
        The name of the table to insert into
    dryrun: bool
        Dryrun.

    Returns
    -------
    None
    """
    logger.info('Building index rows for scenes: '
                '{:,}'.format(len(scenes2index)))
    gdf = gpd.GeoDataFrame([s.index_row for s in scenes2index if s.shelveable],
                           geometry=INDEX_GEOM,
                           crs=INDEX_CRS)

    logger.info('Indexing shelveable scenes: {:,}'.format(len(scenes2index)))
    with Postgres(host=SANDWICH, database=PLANET) as db_src:
        db_src.insert_new_records(gdf,
                                  table=index_tbl,
                                  dryrun=dryrun)


def shelve_and_index(input_directory: Union[Path, str],
                     destination_directory: str = PLANET_DATA_DIR,
                     scene_manifests_exist: bool = False,
                     copy_unshelveable: bool = False,
                     verify_checksums: bool = False,
                     transfer_method: str = 'copy',
                     remove_sources: bool = False,
                     generate_manifests_only: bool = False,
                     manage_unshelveable_only: bool = False,
                     shelve_only: bool = False,
                     index_only: bool = False,
                     dryrun: bool = False) -> None:
    """
    Shelve all Planet scenes found in the input_directory. Scenes
    are located by their scene-level manifest files
    ([scene_identifier]_manifest.json), which are created from
    order-level manifest files, which are located by finding all
    files matching 'manifest.json' files in the input_directory.

    Parameters
    ----------
    input_directory : pathlib.Path, str
        Path to directory to parse for Planet scenes.
    destination_directory : pathlib.Path, str
        Path to parent directory under which to shelve scenes.
    scene_manifests_exist : bool
        Set to True if scene-level manifest files
        ([scene_identifier]_manifest.json files have already been
        created from order manifests.
    verify_checksums : bool
        True to compute checksums and verify against values in
        order-manifests (which are passed to scene-manifests.
    transfer_method : str
        'copy', 'link'
    remove_sources: bool
        Remove source files after copying and confirming they
        exist at the shelved location. In the case of
        unshelveable scenes, they will be deleted if this is
        True.
    copy_unshelveable: str, Path
        If None, unshelveable scenes are handled according to
        remove sources. Otherwise, unshelveable scenes are
        copied to this directory.
    manage_unshelveable_only : bool
        Determine unshelveable scenes and handle according to
        copy_unshelveable, then exit.
    dryrun : bool
        Locate scenes, determine if shelveable,

    Returns
    -------
    list : list of PlanetScene objects that were shelveable
    """
    # Verify arguments
    input_directory, destination_directory = verify_args(input_directory=input_directory,
                                                         destination_directory=destination_directory,
                                                         transfer_method=transfer_method)

    # Log routine information
    logger.info('Starting shelving and indexing routine...\n')
    logger.info('Source data location: {}'.format(input_directory))
    if generate_manifests_only:
        logger.info('Generating manifests only.')
    elif manage_unshelveable_only:
        logger.info('Manage unshelveable only.')
    elif index_only:
        logger.info('Index only.')
    else:
        logger.info('Destination directory: {}'.format(destination_directory))
        logger.info('Scene manifests exists: {}'.format(scene_manifests_exist))
        logger.info('Verify checksums: {}'.format(verify_checksums))
        logger.info('Move unshelveable: {}'.format(copy_unshelveable))
        logger.info('Transfer method: {}'.format(transfer_method))
        logger.info('Remove source files: {}'.format(remove_sources))
        logger.info('Dryrun: {}\n'.format(dryrun))
    # To allow cancelling if a parameter is not correct
    time.sleep(5)

    # Alternate routine - generate manifests only
    if generate_manifests_only and not dryrun:
        # Create scene manifests and exit
        logger.info('Creating scene manifests for all order manifests in: '
                    '{}'.format(input_directory))
        create_all_scene_manifests(input_directory)
        logger.info('Scene manifests created.')
        sys.exit()

    # Create scene-level manifests from order manifests
    if not scene_manifests_exist and not dryrun:
        scene_manifests = create_all_scene_manifests(input_directory)
    elif scene_manifests_exist:
        logger.info('Locating scene manifests...')
        scene_manifests = input_directory.rglob('*_manifest.json')

    # Create PlanetScene objects for each scene found in input directory
    scenes = create_scenes(scene_manifests=scene_manifests,
                           destination_directory=destination_directory)

    # Locate scenes that are shelveable and/or indexable, and those that are
    # not and should be skipped. There may (likely will) be repeated scenes
    # in scenes2shelve and scenes2index.
    scenes2shelve, scenes2index, scenes2skip = identify_shelveable_indexable(scenes=scenes)

    # Manage unshelveable scenes, i.e don't have valid checksum, associated
    # xml not found, etc., by either deleting or copying them
    if len(scenes2skip) > 0:
        logger.info('Total scenes being skipped entirely: '
                    '{:,}\n'.format(len(scenes2skip)))
        handle_unshelveable(scenes2skip,
                            transfer_method=transfer_method,
                            copy_unshelveable=copy_unshelveable,
                            remove_sources=remove_sources,
                            dryrun=dryrun)

    if manage_unshelveable_only:
        logger.info('Managing unshelveable scenes complete, exiting.')
        sys.exit()

    if not index_only:
        # Shelve scenes
        logger.info('Scenes to shelve: {:,}'.format(len(scenes2shelve)))
        if len(scenes2shelve) > 0:
            shelve_scenes(scenes2shelve=scenes2shelve,
                          copy_unshelveable=copy_unshelveable,
                          transfer_method=transfer_method,
                          remove_sources=remove_sources,
                          dryrun=dryrun)

    # Add all indexable scenes to index
    if (scenes2index is not None) and (len(scenes2index) != 0) \
            and not shelve_only:
        logger.info('Indexing scenes...')
        index_scenes(scenes2index, index_tbl=INDEX_TBL, dryrun=dryrun)

    logger.info('Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Routine linking shelving and indexing. In a standard run, "
                    "a directory is parsed for order manifests. The order "
                    "manifest's are parsed for sections corresponding to the "
                    "actual imagery (as opposed to the metadata files, masks, "
                    "etc.), and these sections are written to new 'scene "
                    "manifest' files following the naming convention of the "
                    "scenes. Next, all scenes are located based on the "
                    "presence of these scene manifest files, and their "
                    "metadata (XML and scene manifest) is parsed in order to "
                    "create both the shelved path and the row to add to the "
                    "index table. Next the copy to the shelving location is "
                    "performed, with options to remove the source files after "
                    "copying and/or to move any unshelveable scenes to an "
                    "alternate location. Finally, the new records are written "
                    "to the index table: planet.scenes",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    required_args = parser.add_argument_group('Required Arguments')
    common_args = parser.add_argument_group('Common Arguments')
    rare_args = parser.add_argument_group('Rarely Used Arguments')
    alt_routine_args = parser.add_argument_group('Alternative Routines')
    mut_alt_routine_args = alt_routine_args.add_mutually_exclusive_group()

    required_args.add_argument('-i', '--input_directory', type=os.path.abspath,
                               required=True,
                               help='Directory holding data to shelve, '
                                    'searched recursively for all '
                                    'order "manifest.json" files.')

    common_args.add_argument('-sme', '--scene_manifests_exist',
                             action='store_true',
                             help='Use to specify that scene manifests exist '
                                  'and recreating is not necessary or not '
                                  'possible, i.e. there are no '
                                  'order manifests.')
    common_args.add_argument('-tm', '--transfer_method',
                             choices=[LINK, COPY],
                             default=COPY,
                             help='Method to use for transfer.')
    common_args.add_argument('-rs', '--remove_sources', action='store_true',
                             help='Use flag to delete source files after '
                                  'shelving. Otherwise source files will be '
                                  'left in their original locations.')
    common_args.add_argument('-cu', '--copy_unshelveable',
                             type=os.path.abspath,
                             help='If provided, copy unshelveable files to '
                                  'this directory. Or')

    rare_args.add_argument('--destination_directory', type=os.path.abspath,
                           default=PLANET_DATA_DIR,
                           help='Change the default location for shelving. '
                                'The standard shelving structure will be '
                                'built under this directory. Useful for '
                                'debugging.')
    rare_args.add_argument('--skip_checksums', action='store_true',
                           help='Skip verifying checksums, all new scenes found '
                                'in data directory will be moved to destination.')

    # ALternative routines
    mut_alt_routine_args.add_argument('--generate_manifests_only',
                                      action='store_true',
                                      help='Only generate scene manifests from order '
                                           'manifests, do not perform copy operation. '
                                           'This is done as part of the copy routine, '
                                           'but this flag can be used to create scene '
                                           'manifests without copying.')
    mut_alt_routine_args.add_argument('--manage_unshelveable_only',
                                      action='store_true',
                                      help='Move or remove unshelveable data and exit.')
    mut_alt_routine_args.add_argument('--index_scenes_only', action='store_true',
                                      help='Index any scenes in input_directory '
                                           'and exit.')
    mut_alt_routine_args.add_argument('--shelve_only', action='store_true',
                                      help='Shelve any scenes in input_directory '
                                           'and exit.')

    parser.add_argument('--logdir', type=os.path.abspath,
                        help='Path to write logfile to.')
    parser.add_argument('--verbose', action='store_true',
                        help='Set logging level to DEBUG.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing.')

    args = parser.parse_args()

    # Parse arguments, convert to pathlib.Path objects
    input_directory = Path(args.input_directory)
    destination_directory = Path(args.destination_directory)
    scene_manifests_exist = args.scene_manifests_exist
    copy_unshelveable = (Path(args.copy_unshelveable)
                         if args.copy_unshelveable is not None
                         else None)
    verify_checksums = not args.skip_checksums
    transfer_method = args.transfer_method
    remove_sources = args.remove_sources

    # Alternative routine arguments
    generate_manifests_only = args.generate_manifests_only
    manage_unshelveable_only = args.manage_unshelveable_only
    shelve_only = args.shelve_only
    index_only = args.index_scenes_only

    logdir = args.logdir
    verbose = args.verbose
    dryrun = args.dryrun

    if logdir:
        logfile = create_logfile_path(__name__, logdir)
        logger = create_logger(__name__, 'fh', 'DEBUG', filename=logfile)
    if verbose:
        logger = create_logger(__name__, 'sh', 'DEBUG')

    shelve_and_index(input_directory=input_directory,
                     destination_directory=destination_directory,
                     scene_manifests_exist=scene_manifests_exist,
                     copy_unshelveable=copy_unshelveable,
                     verify_checksums=verify_checksums,
                     transfer_method=transfer_method,
                     remove_sources=remove_sources,
                     generate_manifests_only=generate_manifests_only,
                     manage_unshelveable_only=manage_unshelveable_only,
                     shelve_only=shelve_only,
                     index_only=index_only,
                     dryrun=dryrun,
                     )
