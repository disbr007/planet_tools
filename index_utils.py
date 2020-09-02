import hashlib
import json
from pathlib import Path, PurePath
from tqdm import tqdm
import xml.etree.ElementTree as ET

from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'DEBUG')

# Manifest Keys
k_files = 'files'
k_media_type = 'media_type'
k_path = 'path'
k_annotations = 'annotations'
k_asset_type = 'planet/asset_type'
k_bundle_type = 'planet/bundle_type'
k_item_type = 'planet/item_type'
k_digests = 'digests'
k_md5 = 'md5'

# Manifest Constants
manifest_suffix = 'manifest'  # suffix to append ti filenames for individual manifest files
udm = 'udm'  # included in unusable data mask files paths
image = 'image'  # start of media_type that indicates imagery


def tag_uri_and_name(elem):
    if elem.tag[0] == '{':
        uri, _, name = elem.tag[1:].partition('}')
    else:
        uri = None
        name = elem.tag

    return uri, name


def add_renamed_attributes(node, renamer, root, attributes):
    elems = root.find('.//{}'.format(node))
    for e in elems:
        uri, name = tag_uri_and_name(e)
        if name in renamer.keys():
            name = renamer[name]
        if e.text.strip() != '':
            # print('{}: {}'.format(name, e.text))
            attributes[name] = e.text


def attributes_from_xml(xml):
    with open(xml, 'rt') as src:
        tree = ET.parse(xml)
        root = tree.getroot()

    # Nodes where all values can be processed as-is
    nodes_process_all = ['{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}EarthObservationMetaData',
                         '{http://www.opengis.net/gml}TimePeriod',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Sensor',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Acquisition',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}ProductInformation',
                         '{http://earth.esa.int/opt}cloudCoverPercentage',
                         '{http://earth.esa.int/opt}cloudCoverPercentageQuotationMode',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}unusableDataPercentage']

    # Nodes with repeated attribute names -> rename according to dicts
    platform_node =   '{http://earth.esa.int/eop}Platform'
    instrument_node = '{http://earth.esa.int/eop}Instrument'
    mask_node =       '{http://earth.esa.int/eop}MaskInformation'

    platform_renamer =  {'shortName': 'platform'}
    insrument_renamer = {'shortName': 'instrument'}
    mask_renamer =      {'fileName': 'mask_filename',
                         'type': 'mask_type',
                         'format': 'mask_format',
                         'referenceSystemIdentifier': 'mask_referenceSystemIdentifier'}

    rename_nodes = [(platform_node,   platform_renamer),
                    (instrument_node, insrument_renamer),
                    (mask_node,       mask_renamer)]

    # Bands Node - conflicting attribute names -> add band number: "band1_radiometicScaleFactor"
    bands_node = '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}bandSpecificMetadata'
    band_number_node = '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}bandNumber'

    attributes = dict()
    # Add attributes that are processed as-is
    for node in nodes_process_all:
        elems = root.find('.//{}'.format(node))
        for e in elems:
            uri, name = tag_uri_and_name(e)
            if e.text.strip() != '':
                # print('{}: {}'.format(name, e.text))
                attributes[name] = e.text

    # Add attributes that require renaming
    for node, renamer in rename_nodes:
        add_renamed_attributes(node, renamer, root=root, attributes=attributes)

    # Process band metadata
    bands_elems = root.findall('.//{}'.format(bands_node))
    for band in bands_elems:
        band_uri = '{{{}}}'.format(tag_uri_and_name(band)[0])
        band_number = band.find('.//{}'.format(band_number_node)).text
        band_renamer = {tag_uri_and_name(e)[1]: 'band{}_{}'.format(band_number, tag_uri_and_name(e)[1])
                        for e in band
                        if tag_uri_and_name(e)[1] != 'bandNumber'}
        for e in band:
            band_uri, name = tag_uri_and_name(e)
            if name in band_renamer.keys():
                name = band_renamer[name]
            # Remove quotes from field names (some have them, some do not)
            name = name.replace('"', '').replace("'", '')
            if e.text.strip() != '' and name != 'bandNumber':
                attributes[name] = e.text

    return attributes


def write_scene_manifest(scene_manifest, master_manifest,
                         manifest_suffix=manifest_suffix,
                         overwrite=False):
    """Write the section of the parent manifest for an individual scene
    to a new file with that scenes name."""
    scene_path = Path(scene_manifest[k_path])
    scene_mani_name = '{}_{}.json'.format(scene_path.stem, manifest_suffix)
    scene_mani_path = master_manifest.parent / scene_path.parent / scene_mani_name
    exists = scene_mani_path.exists()
    if not exists or (exists and overwrite):
        logger.debug('Writing manifest for: {}'.format(scene_path.stem))
        with open(scene_mani_path, 'w') as scene_mani_out:
            json.dump(scene_manifest, scene_mani_out)
    elif exists and not overwrite:
        logger.debug('Scene manifest exists, skipping.')

    return scene_mani_path


def get_scene_manifests(master_manifest):
    """
    Parse a parent manifest file for the sections corresponding
    to scenes.
    """
    with open(master_manifest, 'r') as src:
        mani = json.load(src)

    # Get metadata for all images
    scene_manifests = []
    for f in mani[k_files]:
        # TODO: Identify imagery dictionaries better
        if f[k_media_type].startswith(image) and udm not in f[k_path]:
            scene_manifests.append(f)

    return scene_manifests


def create_scene_manifests(master_manifest, overwrite=False):
    """
    Create scene manifest files for each scene section in the master manifest.
    """
    logger.info('Locating scene manifests within master manifest: {}'.format(master_manifest))
    scene_manifests = get_scene_manifests(master_manifest)
    logger.info('Scene manifests found: {}'.format(len(scene_manifests)))

    scene_manifest_files = []
    pbar = tqdm(scene_manifests, desc='Writing manifest.json files for each scene')
    for sm in pbar:
        sf = master_manifest.parent / Path(sm[k_path])
        if not sf.exists():
            # logger.warning('Scene file in manifest.json not found: {}'.format(sf))
            continue
        else:
            logger.info('Scene file found')
        scene_manifest_file = write_scene_manifest(sm, master_manifest, overwrite=overwrite)
        scene_manifest_files.append(scene_manifest_file)

    return scene_manifest_files


def bundle_item_types_from_manifest(scene_manifest_file):
    """
    Get bundle type and item type from scene manfest file.
    """
    with open(scene_manifest_file) as src:
        sm = json.load(src)

    # asset_type = sm[k_annotations][k_asset_type]
    bundle_type = sm[k_annotations][k_bundle_type]
    item_type = sm[k_annotations][k_item_type]
    # md5 = sm[k_digests][k_md5]

    return bundle_type, item_type


def create_file_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def verify_scene_md5(manifest_file):
    # TODO: Speed up -- parallel?
    with open(manifest_file, 'r') as src:
        manifest_contents = json.load(src)
        scene_file = manifest_file.parent / Path(manifest_contents[k_path]).name

    file_md5 = create_file_md5(scene_file)
    manifest_md5 = manifest_contents[k_digests][k_md5]
    if file_md5 == manifest_md5:
        verified = True
    else:
        logger.warning('Verification of md5 checksum failed: {} != {}'.format(file_md5, manifest_md5))
        verified = False

    # TODO: Remove this - just for speed of debugging
    # verified = True

    return scene_file, verified
