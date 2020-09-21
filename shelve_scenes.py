import argparse
import datetime
import os
from pathlib import Path
import platform
import shutil
import time

from tqdm import tqdm

from lib import linux2win
from index_utils import create_scene_manifests, scene_file_from_manifest, verify_scene_md5, \
                        attributes_from_xml, bundle_item_types_from_manifest
from lib import find_scene_meta_files
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# Args
data_directory = Path(r'E:\disbr007\projects\planet\data')


# Constants
planet_data_dir = Path(r'/mnt/pgc/data/sat/orig')  # /planet?
if platform.system() == 'Windows':
    planet_data_dir = Path(linux2win(str(planet_data_dir)))
# XML date format
date_format = '%Y-%m-%dT%H:%M:%S+00:00'
# XML attribute keys
k_identifier = 'identifier'
k_instrument = 'instrument'
k_productType = 'productType'
k_acquired = 'acquisitionDateTime'


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
    scene_manifests = []
    pbar = tqdm(master_manifests, desc='Creating scene manifests...')
    for mm in pbar:
        pbar.set_description('Creating scene manifests for: {}'.format(mm.parent.name))
        # This will return scene-level manifest paths, whether they were just
        # created or already existed
        sms = create_scene_manifests(mm, overwrite=False)
        scene_manifests.extend(sms)

    logger.info('Scene manifests found: {}'.format(len(scene_manifests)))

    return scene_manifests


def verify_scene_checksums(scene_manifests):
    """
    Scene manifests contain the checksum hash that the image should match. This
    takes a list of scene manifests and calculates the checksum for the associated
    scene files, then returns only those scenes that have checksums that match.
    """
    logger.info('Verifying scene checksums: {}'.format(len(scene_manifests)))
    scene_verification = []
    for sm in tqdm(scene_manifests, desc='Verifying scene checksums...'):
        scene, verified = verify_scene_md5(sm)
        scene_verification.append((scene, verified))
        # TODO: REMOVE time.sleep
        time.sleep(0.001)

    verified_scenes = [scene for scene, verified in scene_verification if verified]
    logger.info('Verified scenes: {}'.format(len(verified_scenes)))
    
    return verified_scenes


def create_move_list(verified_scenes, destination_directory=planet_data_dir):
    """
    Finds the files associated with each scene in verified scenes, then
    creates tuples of (src, dst) according to storage schema:
    intrument/productType/bundle_type/item_type/yyyy/mm_mmm/dd/strip_id
    """
    logger.info('Creating destination filepaths...')
    # Create list of (source file path, destination file path)
    srcs_dsts = []
    for scene in tqdm(verified_scenes, desc='Creating destination filepaths'):
        # Find scene files (*udm.tif, *manifest.json, *metadata.xml, *rpc.txt)
        scene_files = find_scene_meta_files(scene)
        scene_files.append(scene)
        # TODO: Identify meta files with regex?
        xml = [f for f in scene_files if f.match('*.xml')][0]
        manifest = [f for f in scene_files if f.match('*manifest.json')][0]
        # Get attributes from xml file
        attributes = attributes_from_xml(xml)
        strip_id = attributes[k_identifier].split('_')[1]
        # Get asset type, bundle type
        bundle_type, item_type = bundle_item_types_from_manifest(manifest)
        # Get date
        acquired_date = datetime.datetime.strptime(attributes[k_acquired],
                                                   date_format)
        # Create month str, like 08_aug
        month_str = '{}_{}'.format(acquired_date.month,
                                   acquired_date.strftime('%b').lower())
        # Create shelved destinations
        dst_dir = destination_directory / Path(os.path.join(attributes[k_instrument],
                                                            attributes[k_productType],
                                                            bundle_type,
                                                            item_type,
                                                            acquired_date.strftime('%Y'),
                                                            month_str,
                                                            acquired_date.strftime('%d'),
                                                            strip_id))

        for src in scene_files:
            dst = dst_dir / src.name
            srcs_dsts.append((src, dst))

    return srcs_dsts


def shelve_scenes(data_directory, destination_directory=planet_data_dir,
                  master_manifests=True, verify_checksums=True):
    data_directory = Path(data_directory)
    if master_manifests:
        scene_manifests = create_all_scene_manifests(data_directory)
    else:
        # TODO: Is this an ok way to get all scene manifests?
        scene_manifests = data_directory.rglob('*_manifest.json')

    if verify_checksums:
        verified_scenes = verify_scene_checksums(scene_manifests)
    else:
        verified_scenes = [scene_file_from_manifest(sm) for sm in scene_manifests]

    srcs_dsts = create_move_list(verified_scenes, destination_directory)

    logger.info('Copying scenes to shelved locations...')
    for src, dst in tqdm(srcs_dsts):
        if not dst.parent.exists():
            os.makedirs(dst.parent)
        if not dst.exists():
            # logger.debug('Copying {}\n\t->{}'.format(src, dst))
            try:
                # TODO: Add linking?
                shutil.copy2(src, dst)
                # TODO: Remove src?
            except:
                logger.warning('Error copying: {}\n\t->{}'.format(src, dst))
                raise
        else:
            logger.debug('Destination exists, skipping: {}'.format(dst))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--data_directory', type=os.path.abspath,
                        help='Directory holding data to shelve.')
    parser.add_argument('--destination_directory', type=os.path.abspath,
                        default=planet_data_dir,
                        help='Base directory upon which to build filepath.')
    parser.add_argument('-sme', '--scene_manifests_exist', action='store_true',
                        help='Use to specify that scene manifests exist'
                             'and recreating is not necessary '
                             '(or possible - no master manifests)')
    parser.add_argument('--skip_checksums', action='store_true',
                        help='Skip verifying checksums, all scenes found in '
                             'data directory will be moved to destination.')
    
    args = parser.parse_args()
    master_manifests = not args.scene_manifests_exist
    verify_checksums = not args.skip_checksums
    
    shelve_scenes(args.data_directory, args.destination_directory,
                  master_manifests=master_manifests,
                  verify_checksums=verify_checksums)
