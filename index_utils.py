import hashlib
import json
from pathlib import Path, PurePath
from tqdm import tqdm
import xml.etree.ElementTree as ET

from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

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


# def tag_uri_and_name(elem):
#     if elem.tag[0] == '{':
#         uri, _, name = elem.tag[1:].partition('}')
#     else:
#         uri = None
#         name = elem.tag
#
#     return uri, name


# def add_renamed_attributes(node, renamer, root, attributes):
#     elems = root.find('.//{}'.format(node))
#     for e in elems:
#         uri, name = tag_uri_and_name(e)
#         if name in renamer.keys():
#             name = renamer[name]
#         if e.text.strip() != '':
#             # print('{}: {}'.format(name, e.text))
#             attributes[name] = e.text


# def attributes_from_xml(xml):
#     with open(xml, 'rt') as src:
#         tree = ET.parse(xml)
#         root = tree.getroot()
#
#     # Nodes where all values can be processed as-is
#     nodes_process_all = ['{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}EarthObservationMetaData',
#                          '{http://www.opengis.net/gml}TimePeriod',
#                          '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Sensor',
#                          '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Acquisition',
#                          '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}ProductInformation',
#                          '{http://earth.esa.int/opt}cloudCoverPercentage',
#                          '{http://earth.esa.int/opt}cloudCoverPercentageQuotationMode',
#                          '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}unusableDataPercentage']
#
#     # Nodes with repeated attribute names -> rename according to dicts
#     platform_node =   '{http://earth.esa.int/eop}Platform'
#     instrument_node = '{http://earth.esa.int/eop}Instrument'
#     mask_node =       '{http://earth.esa.int/eop}MaskInformation'
#
#     platform_renamer =  {'shortName': 'platform'}
#     insrument_renamer = {'shortName': 'instrument'}
#     mask_renamer =      {'fileName': 'mask_filename',
#                          'type': 'mask_type',
#                          'format': 'mask_format',
#                          'referenceSystemIdentifier': 'mask_referenceSystemIdentifier'}
#
#     rename_nodes = [(platform_node,   platform_renamer),
#                     (instrument_node, insrument_renamer),
#                     (mask_node,       mask_renamer)]
#
#     # Bands Node - conflicting attribute names -> add band number: "band1_radiometicScaleFactor"
#     bands_node = '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}bandSpecificMetadata'
#     band_number_node = '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}bandNumber'
#
#     attributes = dict()
#     # Add attributes that are processed as-is
#     for node in nodes_process_all:
#         elems = root.find('.//{}'.format(node))
#         for e in elems:
#             uri, name = tag_uri_and_name(e)
#             if e.text.strip() != '':
#                 # print('{}: {}'.format(name, e.text))
#                 attributes[name] = e.text
#
#     # Add attributes that require renaming
#     for node, renamer in rename_nodes:
#         add_renamed_attributes(node, renamer, root=root, attributes=attributes)
#
#     # Process band metadata
#     bands_elems = root.findall('.//{}'.format(bands_node))
#     for band in bands_elems:
#         band_uri = '{{{}}}'.format(tag_uri_and_name(band)[0])
#         band_number = band.find('.//{}'.format(band_number_node)).text
#         band_renamer = {tag_uri_and_name(e)[1]: 'band{}_{}'.format(band_number, tag_uri_and_name(e)[1])
#                         for e in band
#                         if tag_uri_and_name(e)[1] != 'bandNumber'}
#         for e in band:
#             band_uri, name = tag_uri_and_name(e)
#             if name in band_renamer.keys():
#                 name = band_renamer[name]
#             # Remove quotes from field names (some have them, some do not)
#             name = name.replace('"', '').replace("'", '')
#             if e.text.strip() != '' and name != 'bandNumber':
#                 attributes[name] = e.text
#
#     return attributes

#
# def scene_file_from_manifest(scene_manifest):
#     """Use the directory name of the scene manifest file + look up the path
#     to the scene file and use just the file name. Just the file name is
#     used as the path in the manifest is relative to the order directory,
#     which may not always be where the scene file is. Using the parent
#     directory of the manifest ensures the correct scene path is located
#     as long as the scene manifest is alongside the scene file."""
#     with open(scene_manifest, 'r') as src:
#         manifest_contents = json.load(src)
#         scene_file = scene_manifest.parent / Path(manifest_contents[k_path]).name
#
#     return scene_file





# def bundle_item_types_from_manifest(scene_manifest_file):
#     """
#     Get bundle type and item type from scene manfest file.
#     """
#     with open(scene_manifest_file) as src:
#         sm = json.load(src)
#
#     # asset_type = sm[k_annotations][k_asset_type]
#     bundle_type = sm[k_annotations][k_bundle_type]
#     item_type = sm[k_annotations][k_item_type]
#     # md5 = sm[k_digests][k_md5]
#
#     return bundle_type, item_type


# def create_file_md5(fname):
#     hash_md5 = hashlib.md5()
#     with open(fname, "rb") as f:
#         for chunk in iter(lambda: f.read(4096), b""):
#             hash_md5.update(chunk)
#
#     return hash_md5.hexdigest()


# def verify_scene_md5(manifest_file):
#     # TODO: Speed up -- parallel?
#     scene_file = scene_file_from_manifest(scene_manifest=manifest_file)
#
#     file_md5 = create_file_md5(scene_file)
#
#     with open(manifest_file) as f:
#         manifest_contents = json.load(f)
#     manifest_md5 = manifest_contents[k_digests][k_md5]
#     if file_md5 == manifest_md5:
#         verified = True
#     else:
#         logger.warning('Verification of md5 checksum failed: {} != {}'.format(file_md5, manifest_md5))
#         verified = False
#
#     return scene_file, verified
#

