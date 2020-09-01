import argparse
import datetime
from pathlib import Path
import os
import shutil
import time

from tqdm import tqdm

from index_utils import create_scene_manifests, verify_scene_md5, \
                        attributes_from_xml, bundle_item_types_from_manifest
from scene_parsing import find_scene_meta_files
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# Args
data_directory = Path(r'E:\disbr007\projects\planet\data')


# Constants
planet_data_dir = Path(r'/mnt/pgc/data/sat/orig')  # /planet?
# XML date format
date_format = '%Y-%m-%dT%H:%M:%S+00:00'
# XML attribute keys
k_identifier = 'identifier'
k_instrument = 'instrument'
k_productType = 'productType'
k_acquired = 'acquisitionDateTime'


def create_all_scene_manifests(directory):
    # Get all master manifests
    master_manifests = set(directory.rglob('manifest.json'))
    logger.info('Master manifests found: {}'.format(len(master_manifests)))
    # Create manifests for each scene
    logger.info('Creating scene manifests...')
    pbar = tqdm(master_manifests, desc='Creating scene manifests...')
    scene_manifests = []
    for mm in pbar:
        pbar.set_description('Creating scene manifests for: {}'.format(mm.parent.name))
        sms = create_scene_manifests(mm, overwrite=True)
        scene_manifests.extend(sms)

    # Get scene manifests (remove master manifests -- now done above
    # scene_manifests = set(directory.rglob('*manifest.json')) - master_manifests
    logger.info('Scene manifests found: {}'.format(len(scene_manifests)))

    return scene_manifests


def verify_scene_checksums(scene_manifests):
    # Verify checksums
    logger.info('Verifying scene checksums: {}'.format(len(scene_manifests)))
    scene_verification = []
    pbar = tqdm(scene_manifests, desc='Verifying scene checksums...')
    for sm in pbar:
        scene, verified = verify_scene_md5(sm)
        scene_verification.append((scene, verified))
        # TODO: REMOVE time.sleep
        time.sleep(0.001)

    verified_scenes = [scene for scene, verified in scene_verification if verified]
    logger.info('Verified scenes: {}'.format(len(verified_scenes)))
    
    return verified_scenes


def create_move_list(verified_scenes, destination_directory):
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


def shelve_scenes(data_directory, destination_directory=planet_data_dir):
    data_directory = Path(data_directory)
    scene_manifests = create_all_scene_manifests(data_directory)
    verified_scenes = verify_scene_checksums(scene_manifests)
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
            logger.debug('Destination exists, skipping: {}'.format(dst.relative_to(planet_data_dir)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--data_directory', type=os.path.abspath,
                        help='Directory holding data to shelve.')
    parser.add_argument('--destination_directory', type=os.path.abspath,
                        default=planet_data_dir,
                        help='Base directory upon which to build filepath.')
    
    args = parser.parse_args()
    
    shelve_scenes(args.data_directory, args.destination_directory)
