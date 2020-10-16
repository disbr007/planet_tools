import argparse
import datetime
import os
from pathlib import Path
import platform
import shutil
import sys
import time

from tqdm import tqdm

from lib import linux2win, create_scene_manifests
from lib import PlanetScene, find_planet_scenes
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# Constants
# Destination directory for shelving
planet_data_dir = Path(r'/mnt/pgc/data/sat/orig')
if platform.system() == 'Windows':
    planet_data_dir = Path(linux2win(str(planet_data_dir)))


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
                        dryrun):
    copy_fxn = determine_copy_fxn(transfer_method)
    logger.info('Creating list of unshelveable scenes and metadata files...')
    unshelve_src_dst = []
    for ps in unshelveable:
        for src in ps.scene_files:
            if move_unshelveable:
                dst = move_unshelveable / src.stem
            else:
                dst = None
            unshelveable.append((src, dst))
    if move_unshelveable:
        logger.info('Moving unshelveable scenes and meta files to: '
                    '{}'.format(move_unshelveable))
        for src, dst in unshelve_src_dst:
            if dryrun:
                continue
            copy_fxn(src, dst)
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


def shelve_scenes(data_directory, destination_directory=planet_data_dir,
                  scene_manifests_exist=True, verify_checksums=True,
                  transfer_method='copy', remove_sources=True,
                  locate_unshelveable=False, move_unshelveable=None,
                  manage_unshelveable_only=False,
                  dryrun=False):
    logger.info('Starting shelving routine...\n')
    logger.info('Source data location: {}'.format(data_directory))
    logger.info('Destination parent directory: '
                '{}'.format(destination_directory))
    logger.info('Scene manifests exists: {}'.format(scene_manifests_exist))
    logger.info('Verify checksums: {}'.format(verify_checksums))
    logger.info('Move unshelveable: {}'.format(move_unshelveable))
    logger.info('Transfer method: {}'.format(transfer_method))
    logger.info('Dryrun: {}\n'.format(dryrun))

    # Create scene-level manifests from master manifests
    if not scene_manifests_exist and not dryrun:
        create_all_scene_manifests(data_directory)
    # TODO: Is this an ok way to get all scene manifests?
    scene_manifests = data_directory.rglob('*_manifest.json')

    # Use manifests to create PlanetScene objects, this parses
    # the information in the manifest files into attributes
    # (scene_path, md5, bundle_type, etc.)
    scenes = [PlanetScene(sm) for sm in scene_manifests]
    if not scenes:
        if dryrun:
            logger.info('No scenes found. Create scene_manifests using '
                        'generate_manifests_only first to proceed with '
                        'rest of dryrun.')
        else:
            logger.error('No scenes found. Are master manifests '
                         '("manifest.json") present in data_directory?\n'
                         'data_directory: {}'.format(data_directory))
            sys.exit()

    # Verify checksum, or mark all as skip if not checking
    if verify_checksums:
        logger.info('Verifying scene checksums...')
        for ps in tqdm(scenes, desc='Verifying scene checksums...'):
            ps.verify_checksum()
    else:
        logger.info('Skipping checksum verification...')
        for ps in scenes:
            ps.skip_checksum = True

    # Locate scenes that are not shelveable, i.e don't have valid
    # checksum, associated xml not found, etc.
    if locate_unshelveable:
        logger.info('Locating unshelveable scenes...')
        unshelveable = []
        for ps in scenes:
            if not ps.is_shelveable():
                logger.warning('UNSHELVABLE: {}'.format(ps.scene_path))
                logger.debug('Checksum: {}'.format(ps.valid_md5))
                logger.debug('XML Path: {}'.format(ps.xml_path))
                logger.debug('Instrument: {}'.format(ps.instrument))
                logger.debug('Product Type: {}'.format(ps.product_type))
                logger.debug('Bundle type: {}'.format(ps.bundle_type))
                logger.debug('Acquired: {}'.format(ps.acquired_date))
                logger.debug('Strip ID: {}'.format(ps.strip_id))
                unshelveable.append(ps)
        # TODO: Move/delete unshelvable data and remove from scenes
        if len(unshelveable) > 0:
            handle_unshelveable(unshelveable, transfer_method=transfer_method,
                                dryrun=dryrun)

    if manage_unshelveable_only:
        logger.info('Managing unshelveable scenes complete, exiting.')
        sys.exit()

    # Create list of tuples of (src, dst) where dst is shelved location
    logger.info('Determining shelved destinations...')
    srcs_dsts = []
    for ps in tqdm(scenes, desc='Determining shelved destinations.'):
        move_list = [(sf, ps.shelved_dir / sf.name) for sf in ps.scene_files]
        srcs_dsts.extend(move_list)

    logger.info('Copying scenes to shelved locations...')
    # Determine copy function based on platform
    copy_fxn = determine_copy_fxn(transfer_method)
    prev_order = None  # for logging only
    for src, dst in tqdm(srcs_dsts):
        # TODO: This doesn't explicity go through each order
        #  directory in order - sort srcs_dsts by src order dir?
        # Log the current order directory being parsed
        current_order = src.relative_to(data_directory).parts[0]
        if current_order != prev_order:
            logger.info('Copying order directory: {}'.format(current_order))
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

    # Remove source files
    if remove_sources:
        logger.info('Removing source files...')
        for src, _dst in tqdm(srcs_dsts, desc='Removing source files'):
            if dryrun:
                continue
            try:
                os.remove(src)
            except Exception as e:
                logger.error('Error removing {}'.format(src))
                logger.error(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('--data_directory', type=os.path.abspath,
                        required=True,
                        help='Directory holding data to shelve.')
    parser.add_argument('--destination_directory', type=os.path.abspath,
                        default=planet_data_dir,
                        help='Base directory upon which to build filepath.')
    parser.add_argument('-sme', '--scene_manifests_exist', action='store_true',
                        help='Use to specify that scene manifests exist '
                             'and recreating is not necessary or not '
                             'possible, i.e. there are no master manifests)')
    parser.add_argument('--skip_checksums', action='store_true',
                        help='Skip verifying checksums, all new scenes found '
                             'in data directory will be moved to destination.')
    parser.add_argument('-tm', '--transfer_method', choices=['link', 'copy'],
                        default='copy',
                        help='Method to use for transfer.')
    parser.add_argument('--remove_sources', action='store_true',
                        help='Use flag to delete source files after shelving.')
    parser.add_argument('--locate_unshelveable', action='store_true',
                        help='Locate unshelveable data and handle accourding '
                             'to move_unshelveable argument.')
    parser.add_argument('--move_unshelveable', type=os.path.abspath,
                        help='Move unshelveable files to this location '
                             'rather than deleting.')
    parser.add_argument('--manage_unshelveable_only', action='store_true')
    parser.add_argument('--generate_manifests_only', action='store_true',
                        help='Only generate scene manifests from master '
                             'manifests, do not perform copy operation. This '
                             'is done as part of the copy routine, but this '
                             'flag can be used to create scene manifests '
                             'without copying.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing.')
    # TODO: --include_pan
    # TODO: --locate_unshelvable

    # For debugging
    # sys.argv = ['shelve_scenes.py',
    #             '--data_directory',
    #             r'V:\pgc\data\scratch\jeff\projects\planet\scratch'
    #             r'\test_order\1fb3047b-705e-4ed0-b900-a86110b82dca',
    #             '--skip_checksums',
    #             '--dryrun']

    args = parser.parse_args()

    data_directory = Path(args.data_directory)
    destination_directory = Path(args.destination_directory)
    scene_manifests_exist = args.scene_manifests_exist
    locate_unshelveable = args.locate_unshelveable
    move_unshelveable = (Path(args.move_unshelveable)
                         if args.move_unshelveable is not None else None)
    verify_checksums = not args.skip_checksums
    generate_manifests = args.generate_manifests
    transfer_method = args.transfer_method
    dryrun = args.dryrun

    # Verify arguments
    if not data_directory.exits():
        logger.error('Data directory does not exists: {}'.format(data_directory))
        sys.exit()
    if not destination_directory.exists():
        logger.error('Destination directory does not exist: {}'.format(destination_directory))
        sys.exit()
    if platform.system() == 'Windows' and transfer_method == 'link':
        logger.error('Transfer method "link" not compatible with Windows '
                     'platforms. Please use "copy".')
        sys.exit()

    # Just create scene manifests and exist
    if generate_manifests and not dryrun:
        logger.info('Creating scene manifests for all master manifests in: {}'.format(data_directory))
        create_all_scene_manifests(data_directory)
        sys.exit()
    
    shelve_scenes(data_directory, destination_directory,
                  scene_manifests_exist=scene_manifests_exist,
                  verify_checksums=verify_checksums,
                  locate_unshelveable=locate_unshelveable,
                  move_unshelveable=move_unshelveable,
                  transfer_method=transfer_method,
                  dryrun=dryrun)
