from pathlib import Path
import json
import glob
import datetime
import os

from tqdm import tqdm

from index_utils import create_scene_manifests, verify_scene_md5, \
    attributes_from_xml, bundle_item_types_from_manifest
from scene_parsing import find_scene_meta_files
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'DEBUG')

directory = Path(r'E:\disbr007\projects\planet\data')
planet_data_dir = Path(r'/mnt/pgc/data/sat/orig')

# XML date format
date_format = '%Y-%m-%dT%H:%M:%S+00:00'
# XML attribute keys
k_identifier = 'identifier'
k_instrument = 'instrument'
k_productType = 'productType'
k_acquired = 'acquisitionDateTime'




master_manifests = set(directory.rglob('manifest.json'))
# Create manifests for each scene
logger.info('Creating scene manifests...')
pbar = tqdm(master_manifests, desc='Creating scene manifests...')
for mm in pbar:
    pbar.set_description('Creating scene manifests for: {}'.format(mm.parent))
    create_scene_manifests(mm, overwrite=False)

# Get scene manifests (remove master manifests
scene_manifests = set(directory.rglob('*manifest.json')) - master_manifests


# Verify checksums
logger.info('Verifying scene checksums...')
scene_verification = []
pbar = tqdm(scene_manifests, desc='Verifying scene checksums...')
for sm in pbar:
    scene, verified = verify_scene_md5(sm)
    scene_verification.append((scene, verified))


verified_scenes = [scene for scene, verified in scene_verification
                   if verified]

srcs_dsts = []
for scene in verified_scenes:
    # Find scene files
    scene_meta_files = find_scene_meta_files(scene)
    # TODO: Identify meta files with regex?
    xml = [f for f in scene_meta_files if f.match('*.xml')][0]
    manifest = [f for f in scene_meta_files if f.match('*manifest.json')][0]
    # Get attributes
    attributes = attributes_from_xml(xml)
    strip_id = attributes[k_identifier].split('_')[1]
    # Get asset type, bundle type
    bundle_type, item_type = bundle_item_types_from_manifest(manifest)
    # Get date
    acquired_date = datetime.datetime.strptime(attributes[k_acquired],
                                               date_format)
    month_str = '{}_{}'.format(acquired_date.month,
                               acquired_date.strftime('%b').lower())
    # Create shelved destinations
    dst_dir = planet_data_dir / Path(os.path.join(attributes[k_instrument],
                                                  attributes[k_productType],
                                                  bundle_type,
                                                  item_type,
                                                  acquired_date.strftime('%Y'),
                                                  month_str,
                                                  acquired_date.strftime('%d'),
                                                  strip_id))

    scene_files = [f for f in scene_meta_files]
    scene_files.append(scene)
    for src in scene_files:
        dst = dst_dir / src.name
        srcs_dsts.append((src, dst))

    break


