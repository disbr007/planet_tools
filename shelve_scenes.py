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
# from index_utils import scene_file_from_manifest, #verify_scene_md5
#                         attributes_from_xml, bundle_item_types_from_manifest
from lib import PlanetScene # find_scene_meta_files,
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# Args
# TODO: Add location on V: and make terranova compatable
# data_directory = Path(r'E:\disbr007\projects\planet\data')


# Constants
# Destination directory for shelving
planet_data_dir = Path(r'/mnt/pgc/data/sat/orig')  # /planet?
if platform.system() == 'Windows':
    planet_data_dir = Path(linux2win(str(planet_data_dir)))
# Location to move unshelveable data to
unshelveable_data_dir = Path(r'/mnt/pgc/data/scratch/jeff/projects/planet/unshelveable')

# # XML date format
# date_format = '%Y-%m-%dT%H:%M:%S+00:00'
# # XML attribute keys
# k_identifier = 'identifier'
# k_instrument = 'instrument'
# k_productType = 'productType'
# k_acquired = 'acquisitionDateTime'


def create_all_scene_manifests(directory):
    """
    Finds all master manifests ("*/manifest.json") in the given directory, then
    parses each for the sections corresponding to scenes and creates new
    scene-level ([identifier]_manifest.json) files for each scene.
    """
    # Get all master manifests
    master_manifests = set(directory.rglob('manifest.json'))
    logger.info('Master manifests found: {}'.format(len(master_manifests)))
    logger.debug('Master manifests found:\n{}'.format('\n'.join([str(m) for m in master_manifests])))
    # Create manifests for each scene
    logger.info('Creating scene manifests...')
    pbar = tqdm(master_manifests, desc='Creating scene manifests...')
    for mm in pbar:
        pbar.set_description('Creating scene manifests for: {}'.format(mm.parent.name))
        # Create scene manifests (*_manifest.json) from a master manifest
        create_scene_manifests(mm, overwrite=False)


# def verify_scene_checksums(scene_manifests):
#     """
#     Scene manifests contain the checksum hash that the image should match. This
#     takes a list of scene manifests and calculates the checksum for the associated
#     scene files, then returns only those scenes that have checksums that match.
#     """
#     logger.info('Verifying scene checksums: {}'.format(len(scene_manifests)))
#     scene_verification = []
#     for sm in tqdm(scene_manifests, desc='Verifying scene checksums...'):
#         scene, verified = verify_scene_md5(sm)
#         scene_verification.append((scene, verified))
#         # TODO: REMOVE time.sleep
#         time.sleep(0.001)
#
#     verified_scenes = [scene for scene, verified in scene_verification if verified]
#     logger.info('Verified scenes: {}'.format(len(verified_scenes)))
#
#     return verified_scenes


def create_move_list(verified_scenes, destination_directory=planet_data_dir):
    """
    Finds the files associated with each scene in verified scenes, then
    creates tuples of (src, dst) according to storage schema:
    intrument/productType/bundle_type/item_type/yyyy/mm_mmm/dd/strip_id
    """
    logger.info('Creating destination filepaths...')
    # Create list of (source file path, destination file path)
    srcs_dsts = []
    for ps in tqdm(verified_scenes, desc='Creating destination filepaths'):
        try:
            # Loops over all files associated with a scene (*.xml, *_manifest.json, etc.)
            for src in ps.scene_files:
                dst = ps.shelved_dir / src.name
                srcs_dsts.append((src, dst))

            # Find scene files (*udm.tif, *manifest.json, *metadata.xml, *rpc.txt)
            scene_files = ps.scene_files
            # scene_files.append(ps.scene_path)

            # # TODO: make this a function: create_shelved_destination(scene, xml, manifest)
            # # Get attributes from xml file
            # attributes = attributes_from_xml(xml)
            # strip_id = attributes[k_identifier].split('_')[1]
            # # Get asset type, bundle type
            # bundle_type, item_type = bundle_item_types_from_manifest(manifest)
            # # Get date
            # acquired_date = datetime.datetime.strptime(attributes[k_acquired],
            #                                            date_format)
            # # Create month str, like 08_aug
            # month_str = '{}_{}'.format(acquired_date.month,
            #                            acquired_date.strftime('%b').lower())
            # # Create shelved destinations
            # dst_dir = destination_directory / Path(os.path.join(attributes[k_instrument],
            #                                                     attributes[k_productType],
            #                                                     bundle_type,
            #                                                     item_type,
            #                                                     acquired_date.strftime('%Y'),
            #                                                     month_str,
            #                                                     acquired_date.strftime('%d'),
            #                                                     strip_id))
        except Exception as e:
            logger.error('Error parsing scene files for: {}'.format(ps.scene_path))
            logger.error(e)

    return srcs_dsts


def shelve_scenes(data_directory, destination_directory=planet_data_dir,
                  master_manifests=True, verify_checksums=True,
                  transfer_method='copy', locate_unshelveable=False,
                  dryrun=False):
    logger.info('Starting shelving routine...\n')
    logger.info('Source data location: {}'.format(data_directory))
    logger.info('Destination parent directory: {}'.format(destination_directory))
    logger.info('Master manifests exists: {}'.format(master_manifests))
    logger.info('Verify checksums: {}'.format(verify_checksums))
    logger.info('Transfer method: {}'.format(transfer_method))
    logger.info('Dryrun: {}\n'.format(dryrun))

    if master_manifests and not dryrun:
        create_all_scene_manifests(data_directory)
    # TODO: Is this an ok way to get all scene manifests?
    scene_manifests = data_directory.rglob('*_manifest.json')

    # Use manifests to create PlanetScene objects, this parses
    # the information in the manifest files into attributes
    # (scene_path, md5, bundle_type, etc.)
    scenes = [PlanetScene(sm) for sm in scene_manifests]

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
        logger.info('Locating unshelveable data...')
        unshelveable = []
        for ps in scenes:
            if not ps.is_shelveable():
                logger.debug('Unshelveable: {}'.format(ps.scene_path))
                logger.debug('Checksum: {}'.format(ps.valid_md5))
                logger.debug('XML Path: {}'.format(ps.xml_path))
                logger.debug('Instrument: {}'.format(ps.instrument))
                logger.debug('Product Type: {}'.format(ps.product_type))
                logger.debug('Bundle type: {}'.format(ps.bundle_type))
                logger.debug('Acquired: {}'.format(ps.acquired_date))
                logger.debug('Strip ID: {}'.format(ps.strip_id))
                unshelveable.extend(ps.scene_files)
        # TODO: Move/delete unshelvable data

    # Create list of tuples of (src, dst) where dst is shelved location
    srcs_dsts = []
    for ps in tqdm(scenes, desc='Creating shelved destinations'):
        move_list = [(sf, ps.shelved_dir / sf.name) for sf in ps.scene_files]
        srcs_dsts.extend(move_list)

    logger.info('Copying scenes to shelved locations...')
    prev_order = None
    for src, dst in tqdm(srcs_dsts):
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
            if platform.system() == 'Linux' and transfer_method == 'link':
                copy_fxn = os.link
            elif transfer_method == 'copy':
                copy_fxn = shutil.copy2
            try:
                copy_fxn(src, dst)
            except Exception as e:
                logger.error('Error copying:\n{}\n\t-->{}'.format(src, dst))
                logger.error(e)
        else:
            logger.debug('Destination exists, skipping: {}'.format(dst))
        prev_order = current_order


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('--data_directory', type=os.path.abspath,
                        help='Directory holding data to shelve.')
    parser.add_argument('--destination_directory', type=os.path.abspath,
                        default=planet_data_dir,
                        help='Base directory upon which to build filepath.')
    parser.add_argument('-sme', '--scene_manifests_exist', action='store_true',
                        help='Use to specify that scene manifests exist '
                             'and recreating is not necessary or not '
                             'possible, i.e. there are no master manifests)')
    parser.add_argument('--skip_checksums', action='store_true',
                        help='Skip verifying checksums, all new scenes found in '
                             'data directory will be moved to destination.')
    parser.add_argument('-tm', '--transfer_method', choices=['link', 'copy'],
                        default='copy',
                        help='Method to use for transfer.')
    parser.add_argument('--locate_unshelveable', action='store_true',
                        help='Locate unshelveable data and delete.')
    parser.add_argument('--generate_manifests', action='store_true',
                        help='Only generate scene manifests from master manifests, '
                             'do not perform copy operation. This is done as part '
                             'of the copy routine, but this flag can be used to '
                             'create scene manifests without copying.')
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
    master_manifests = not args.scene_manifests_exist
    verify_checksums = not args.skip_checksums
    generate_manifests = args.generate_manifests
    transfer_method = args.transfer_method
    dryrun = args.dryrun

    # Just create scene manifests and exist
    if generate_manifests and not dryrun:
        logger.info('Creating scene manifests for all master manifests in: {}'.format(data_directory))
        create_all_scene_manifests(data_directory)
        sys.exit()
    
    shelve_scenes(data_directory, destination_directory,
                  master_manifests=master_manifests,
                  verify_checksums=verify_checksums,
                  transfer_method=transfer_method,
                  dryrun=dryrun)
