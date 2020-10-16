import argparse
import copy
import datetime
import hashlib
import json
import os
from pathlib import Path
import pathlib
import platform
import re
import sys
import time
import xml.etree.ElementTree as ET

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
from tqdm import tqdm

from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# Shelve parent directory
# TODO: move this to a config file?
planet_data_dir = Path(r'/mnt/pgc/data/sat/orig')  # /planet?
# if platform.system() == 'Windows':
#     planet_data_dir = Path(linux2win(str(planet_data_dir)))

# Constants
windows = 'Windows'
linux = 'Linux'
# Keys in metadata.json
k_location = 'location'
k_scenes_id = 'id'
k_order_id = 'order_id'
# New fields that are created
k_filename = 'filename'
k_relative_loc = 'rel_location'
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
image = 'image'  # start of media_type that indicates imagery in master manifest

def win2linux(path):
    lp = path.replace('V:', '/mnt').replace('\\', '/')

    return lp


def linux2win(path):
    wp = path.replace('/mnt', 'V:').replace('/', '\\')

    return wp


def get_platform_location(path):
    # TODO: Remove? Not being used as far as I can tell
    if platform.system() == 'Linux':
        pl = win2linux(path)
    elif platform.system() == 'Windows':
        pl = linux2win(path)

    return pl


def type_parser(filepath):
    '''
    takes a file path (or dataframe) in and determines whether it is a dbf,
    excel, txt, csv (or df), ADD SUPPORT FOR SHP****
    '''
    if isinstance(filepath, pathlib.PurePath):
        fp = str(filepath)
    else:
        fp = copy.deepcopy(filepath)
    if type(fp) == str:
        ext = os.path.splitext(fp)[1]
        if ext == '.csv':
            with open(fp, 'r') as f:
                content = f.readlines()
                for row in content[0]:
                    if len(row) == 1:
                        file_type = 'id_only_txt'  # txt or csv with just ids
                    elif len(row) > 1:
                        file_type = 'csv'  # csv with columns
                    else:
                        logger.error('Error reading number of rows in csv.')
        elif ext == '.txt':
            file_type = 'id_only_txt'
        elif ext in ('.xls', '.xlsx'):
            file_type = 'excel'
        elif ext == '.dbf':
            file_type = 'dbf'
        elif ext == '.shp':
            file_type = 'shp'
        elif ext == '.geojson':
            file_type = 'geojson'
    elif isinstance(fp, (gpd.GeoDataFrame, pd.DataFrame)):
        file_type = 'df'
    else:
        logger.error('Unrecognized file type. Type: {}'.format(type(fp)))

    return file_type


def read_ids(ids_file, field=None, sep=None, stereo=False):
    '''Reads ids from a variety of file types. Can also read in stereo ids from applicable formats
    field: field name, irrelevant for text files, but will search for this name if ids_file is .dbf or .shp
    '''

    # Determine file type
    file_type = type_parser(ids_file)

    if file_type in ('dbf', 'df', 'gdf', 'shp', 'csv', 'excel', 'geojson') and not field:
        logger.error('Must provide field name with file type: {}'.format(file_type))
        sys.exit()

    # Text file
    if file_type == 'id_only_txt':
        ids = []
        with open(ids_file, 'r') as f:
            content = f.readlines()
            for line in content:
                if sep:
                    # Assumes id is first
                    the_id = line.split(sep)[0]
                    the_id = the_id.strip()
                else:
                    the_id = line.strip()
                ids.append(the_id)

    # csv
    elif file_type in ('csv'):
        df = pd.read_csv(ids_file, sep=sep, )
        ids = list(df[field])

    # dbf, gdf, dbf
    elif file_type in ('dbf'):
        df = gpd.read_file(ids_file)
        ids = list(df[field])

    # SHP
    elif file_type == 'shp':
        df = gpd.read_file(ids_file)
        ids = list(df[field])
    # GEOJSON
    elif file_type == 'geojson':
        df = gpd.read_file(ids_file, driver='GeoJSON')
        ids = list(df[field])
    # GDF, DF
    elif file_type in ('gdf', 'df'):
        ids = list(ids_file[field])

    # Excel
    elif file_type == 'excel':
        df = pd.read_excel(ids_file, squeeze=True)
        ids = list(df[field])

    else:
        logger.error('Unsupported file type... {}'.format(file_type))

    return ids


def datetime2str_df(df, date_format='%Y-%m-%d %H:%M:%S'):
    # Convert datetime columns to str
    date_cols = df.select_dtypes(include=['datetime64']).columns
    for dc in date_cols:
        df[dc] = df[dc].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))


def write_gdf(gdf, out_footprint, out_format=None, date_format=None):
    if not isinstance(out_footprint, pathlib.PurePath):
        out_footprint = Path(out_footprint)

    # Remove datetime - specifiy datetime if desired format
    if not gdf.select_dtypes(include=['datetime64']).columns.empty:
        datetime2str_df(gdf, date_format=date_format)
    logger.debug('Writing to file: {}'.format(out_footprint))
    if not out_format:
        out_format = out_footprint.suffix.replace('.', '')
        if not out_format:
            # If still no extension, check if gpkg (package.gpkg/layer)
            out_format = out_footprint.parent.suffix
            if not out_format:
                logger.error('Could not recognize out format from file extension: {}'.format(out_footprint))

    # Write out in format specified
    if out_format == 'shp':
        gdf.to_file(out_footprint)
    elif out_format == 'geojson':
        gdf.to_file(out_footprint,
                    driver='GeoJSON')
    elif out_format == 'gpkg':
        gdf.to_file(out_footprint.parent, layer=out_footprint.stem,
                    driver='GPKG')
    else:
        logger.error('Unrecognized format: {}'.format(out_format))

    # logger.debug('Writing complete.')


def parse_group_args(parser, group_name):
    # Get just arguments in given group from argparse.ArgumentParser() as Namespace object
    args = parser.parse_args()
    arg_groups = {}
    for group in parser._action_groups:
        group_dict = {a.dest: getattr(args, a.dest, None) for a in group._group_actions}
        arg_groups[group.title] = argparse.Namespace(**group_dict)
    parsed_attribute_args = arg_groups[group_name]

    return parsed_attribute_args


def id_from_scene(scene, scene_levels=['1B', '3B']):
    """scene : pathlib.Path"""
    if not isinstance(scene, pathlib.PurePath):
        scene = Path(scene)
    scene_name = scene.stem
    scene_id = None
    for lvl in scene_levels:
        if lvl in scene_name:
            scene_id = scene_name.split('_{}_'.format(lvl))[0]
    if not scene_id:
        logger.error('Could not parse scene ID with any level '
                     'in {} from {}'.format(scene_levels, scene_name))

    return scene_id


def gdf_from_metadata(scene_md_paths, relative_directory=None,
                      relative_locs=False, pgc_locs=True,
                      rel_loc_style='W'):
    rows = []
    for sm in tqdm(scene_md_paths):
        # Get scene paths
        scene_path = sm[0]
        metadata_path = sm[1]

        logger.debug('Parsing metdata for: {}\{}'.format(scene_path.parent.stem,
                                                         scene_path.stem))
        metadata = json.load(open(metadata_path))
        properties = metadata['properties']

        # Create paths for both Windows and linux
        # Keep only Linux - Use windows for checking existence if code run on windows
        if platform.system() == windows:
            wl = str(scene_path)
            tn = win2linux(str(scene_path))
            if os.path.exists(wl):
                add_row = True
            else:
                add_row = False
        elif platform.system() == linux:
            tn = str(scene_path)
            # wl = linux2win(str(scene_path))
            if os.path.exists(tn):
                add_row = True
            else:
                add_row = False
        if pgc_locs:
            properties[k_location] = tn
        if relative_locs:
            rl = Path(scene_path).relative_to(Path(relative_directory))
            if rel_loc_style == 'W' and platform.system() == 'Linux':
                properties[k_relative_loc] = linux2win(str(rl))
            elif rel_loc_style == 'L' and platform.system() == 'Windows':
                properties[k_relative_loc] = win2linux(str(rl))
            else:
                properties[k_relative_loc] = str(rl)
            oid = Path(metadata_path).relative_to(relative_directory).parts[0]
        properties[k_scenes_id] = metadata['id']
        # properties[k_order_id] = oid
        properties[k_filename] = scene_path.name

        try:
            # TODO: Figure out why some footprints are multipolygon - handle better
            if metadata['geometry']['type'] == 'Polygon':
                properties['geometry'] = Polygon(metadata['geometry']['coordinates'][0])
            elif metadata['geometry']['type'] == 'MultiPolygon':
                logger.warning('Skipping MultiPolygon geometry')
                add_row = False
                # properties['geometry'] = MultiPolygon([Polygon(metadata['geometry']['coordinates'][i][0])
                #                                        for i in range(len(metadata['geometry']['coordinates']))])
        except Exception as e:
            logger.error('Geometry error, skipping add scene: {}'.format(properties[k_scenes_id]))
            logger.error('Metadata file: {}'.format(metadata_path))
            logger.error('Geometry: {}'.format(metadata['geometry']))
            logger.error(e)
            add_row = False

        if add_row:
            rows.append(properties)
        else:
            logger.warning('Scene could not be found (or had bad geometry), '
                           'skipping adding:\n{}\tat {}'.format(metadata['id'], scene_path))

    if len(rows) == 0:
        # TODO: Address how to actually deal with not finding any features - sys.exit()?
        logger.warning('No features to convert to GeoDataFrame.')

    gdf = gpd.GeoDataFrame(rows, geometry='geometry', crs='epsg:4326')

    return gdf


# def metadata_path_from_scene(scene):
#     par_dir = scene.parent
#     sid = id_from_scene(scene)
#     metadata_path = par_dir / '{}_metadata.json'.format(sid)
#
#     return metadata_path


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
        # TODO: Identify imagery sections better
        if f[k_media_type].startswith(image) and udm not in f[k_path]:
            scene_manifests.append(f)

    return scene_manifests


def create_scene_manifests(master_manifest, overwrite=False):
    """
    Create scene manifest files for each scene section in the master manifest.
    """
    logger.debug('Locating scene manifests within master manifest\n{}'.format(master_manifest))
    scene_manifests = get_scene_manifests(master_manifest)
    logger.debug('Scene manifests found: {}'.format(len(scene_manifests)))

    received_date = time.strftime('%Y-%m-%dT%H:%M%SZ', time.localtime(os.path.getmtime(master_manifest)))

    scene_manifest_files = []
    pbar = tqdm(scene_manifests, desc='Writing manifest.json files for each scene')
    for sm in pbar:
        sf = master_manifest.parent / Path(sm[k_path])
        if not sf.exists():
            logger.warning('Scene file location in manifest.json not found, skipping: {}'.format(sf))
            continue
        else:
            logger.debug('Scene file found')
        sm['received_datetime'] = received_date
        scene_manifest_file = write_scene_manifest(sm, master_manifest, overwrite=overwrite)
        scene_manifest_files.append(scene_manifest_file)

    return scene_manifest_files


# def find_scene_files(data_directory):
#     """Locate scene files based on previously created scene level
#     *_manifest.json files."""
#     scene_files = []
#     # Find all manifest files, using the underscore will prevent
#     # master manifests from being found.
#     all_manifests = Path(data_directory).rglob('*_manifest.json')
#     for manifest in all_manifests:
#         with open(str(manifest), 'r') as src:
#             data = json.load(src)
#             # Find the scene path within the manifest
#             scene_path = data['path']
#             scene_files.append(scene_path)
#
#     return scene_files


def create_file_md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()


def verify_scene_md5(manifest_md5, scene_file):
    logger.debug('Verifying md5 checksum for scene: {}'.format(scene_file))
    file_md5 = create_file_md5(scene_file)
    if file_md5 == manifest_md5:
        verified = True
    else:
        logger.warning('Verification of md5 checksum failed: {} != {}'.format(file_md5, manifest_md5))
        verified = False

    return verified


def tag_uri_and_name(elem):
    """Parse XML elements into uris and names"""
    if elem.tag[0] == '{':
        uri, _, name = elem.tag[1:].partition('}')
    else:
        uri = None
        name = elem.tag

    return uri, name


def add_renamed_attributes(node, renamer, root, attributes):
    """Parse XML elements that would overwrite other attributes
    renaming them before adding them to the attributes dict"""
    elems = root.find('.//{}'.format(node))
    for e in elems:
        uri, name = tag_uri_and_name(e)
        if name in renamer.keys():
            name = renamer[name]
        if e.text.strip() != '':
            attributes[name] = e.text


class PlanetScene:
    """A class to represent a Planet scene, including metadata
    file paths, attributes, etc.

    Parameters
    ---------
    manifest : str
        Path to scene-level manifest file
    exclude_meta : list
        Optional, list of strings used to remove files that
        otherwise would be matched by self.metafiles.
        E.g: ['_pan', '_metadata.json']
    """
    def __init__(self, manifest, exclude_meta=None):
        self.manifest = Path(manifest)
        self.exclude_meta = exclude_meta
        if self.manifest.suffix != '.json':
            logger.error('Must pass *_manifest.json file.')
        # Parse manifest for attributes
        with open(str(self.manifest), 'r') as src:
            data = json.load(src)
            # Find the scene path within the manifest
            _digests = data['digests']
            _annotations = data['annotations']
            self.scene_path = self.manifest.parent / Path(data['path']).name
            self.media_type = data['size']
            self.md5 = _digests['md5']
            self.sha256 = _digests['sha256']
            self.asset_type = _annotations['planet/asset_type']
            self.bundle_type = _annotations['planet/bundle_type']
            self.item_id = _annotations['planet/item_id']
            self.item_type = _annotations['planet/item_type']
            self.received_datetime = data['received_datetime']

        # Determine "scene name" - the scene name without post processing suffixes,
        # used when searching for metadata files
        # e.g.: _SR
        if len(self.scene_path.stem.split('_')) < 6:
            self.scene_name = self.scene_path.stem
        elif len(self.scene_path.stem.split('_')) == 6:
            self.scene_name = '_'.join(self.scene_path.stem.split('_')[0:5])

        # Empty attributes calculated from methods
        self._shelveable = None
        # self.shelved_dir = None
        # self.shelved_location = None
        self._meta_files = None
        self._metadata_json = None
        self._xml_path = None
        self._scene_files = None
        self._valid_md5 = None
        self._xml_attributes = None # Keep?
        # Attributes from .xml
        self._identifier = None
        self._acquistion_type = None
        self._product_type = None
        self._instrument = None
        self._acquisition_datetime = None
        self._serial_identifier = None
        self._geometry = None
        self._centroid = None
        self._center_x = None
        self._center_y = None
        # Attributes from _metadata.json
        self._strip_id = None
        # Misc.
        self.skip_checksum = None
        self._index_row = None
        self._footprint_row = None

        # Bundle types that have been tested for compatibility with naming conventions
        self.supported_bundle_types = ['analytic', 'analytic_sr',
                                       'basic_analytic', 'basic_analytic_nitf',
                                       'basic_uncalibrated_dn', 'basic_uncalibrated_dn_ntif',
                                       'uncalibrated_dn']
        # Ensure bundle_type is suppported
        if self.bundle_type not in self.supported_bundle_types:
            logger.error('Bundle type not tested: {}'.format(self.bundle_type))

    @property
    def metadata_json(self):
        if self._metadata_json is None:
            self._metadata_json = self.scene_path.parent \
                                 / '{}_metadata.json'.format(id_from_scene(self.scene_path))

        return self._metadata_json

    @ property
    def strip_id(self):
        if self._strip_id is None:
            if self.metadata_json.exists():
                with open(self.metadata_json) as src:
                    content = json.load(src)
                    self._strip_id = content['properties']['strip_id']
        return self._strip_id

    @property
    def meta_files(self):
        """Locate metadata files for the scene, not including the scene itself and
        excluding any subtrings passed in self.exlude_meta"""
        if self._meta_files is None:
            logger.debug('Locating metadata files for: {}'.format(self.scene_path))
            self._meta_files = [f for f in
                                self.scene_path.parent.rglob(
                                    '{}*'.format(self.scene_name)
                                )
                                if f != self.scene_path]
            if self.metadata_json.exists():
                self._meta_files.append(self.metadata_json)
            else:
                logger.debug('Metadata JSON not found for: {}'.format(self.scene_path))
            if self.exclude_meta:
                self._meta_files = [f for f in self._meta_files
                                    if not any([em in str(f) for em in self.exclude_meta])]
        return self._meta_files

    @property
    def scene_files(self):
        if self._scene_files is None:
            self._scene_files = self.meta_files + [self.scene_path]
        return self._scene_files

    @property
    def valid_md5(self):
        if self._valid_md5 is None and not self.skip_checksum:
            self._valid_md5 = verify_scene_md5(self.md5, self.scene_path)
        return self._valid_md5

    @property
    def xml_path(self):
        if self._xml_path is None:
            xml_expr = re.compile('.*.xml')
            xml_matches = list(p for p in self.meta_files if xml_expr.match(str(p)))
            if len(xml_matches) == 1:
                self._xml_path = xml_matches[0]
            elif len(xml_matches) == 0:
                logger.warning('XML not located.')
            else:
                logger.warning('Multiple potential XML matches '
                               'found for scene: {}'.format(self.scene_path))
        return self._xml_path

    @property
    def xml_attributes(self):
        if self._xml_attributes is None:
            # XML date format
            date_format = '%Y-%m-%dT%H:%M:%S+00:00'
            # XML attribute keys
            k_identifier = 'identifier'
            k_instrument = 'instrument'
            k_productType = 'productType'
            k_acquired = 'acquisitionDateTime'

            logger.debug('Parsing attributes from xml: {}'.format(self.xml_path))
            with open(self.xml_path, 'rt') as src:
                tree = ET.parse(self.xml_path)
                root = tree.getroot()

            # Nodes where all values can be processed as-is
            nodes_process_all = [
                '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}EarthObservationMetaData',
                '{http://www.opengis.net/gml}TimePeriod',
                '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Sensor',
                '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Acquisition',
                '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}ProductInformation',
                '{http://earth.esa.int/opt}cloudCoverPercentage',
                '{http://earth.esa.int/opt}cloudCoverPercentageQuotationMode',
                '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}unusableDataPercentage',
                ]

            # Nodes with repeated attribute names -> rename according to dicts
            platform_node = '{http://earth.esa.int/eop}Platform'
            instrument_node = '{http://earth.esa.int/eop}Instrument'
            mask_node = '{http://earth.esa.int/eop}MaskInformation'
            geom_node = '{http://www.opengis.net/gml}LinearRing'
            centroid_node = '{http://www.opengis.net/gml}Point'

            platform_renamer = {'shortName': 'platform',
                                'serialIdentifier': 'serialIdentifier'}
            insrument_renamer = {'shortName': 'instrument'}
            mask_renamer = {'fileName': 'mask_filename',
                            'type': 'mask_type',
                            'format': 'mask_format',
                            'referenceSystemIdentifier': 'mask_referenceSystemIdentifier'}
            geom_renamer = {'coordinates': 'geometry'}
            centroid_renamer = {'pos': 'centroid'}

            rename_nodes = [(platform_node, platform_renamer),
                            (instrument_node, insrument_renamer),
                            (mask_node, mask_renamer),
                            (geom_node, geom_renamer),
                            (centroid_node, centroid_renamer)]

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

            # Convert geometry to shapely Polygon
            points = attributes['geometry'].split()
            pts = [(pt.split(',')) for pt in points]
            pts = [tuple(pt.split(',')) for pt in points]
            pts = [tuple(float(x) for x in p) for p in pts]
            self._geometry = Polygon(pts)

            # Convert center point to shapely Point
            self._centroid = Point(float(x) for x in attributes['centroid'].split())
            self._center_x = self._centroid.x
            self._center_y = self._centroid.y

            self._xml_attributes = attributes
            self._identifier = attributes[k_identifier]
            self._acquired_date = datetime.datetime.strptime(attributes[k_acquired], date_format)
            self._instrument = attributes[k_instrument]
            self._product_type = attributes[k_productType]

        return self._xml_attributes

    @property
    def geometry(self):
        if self._geometry is None:
            self._geometry = self.xml_attributes['geometry']
        return self._geometry

    @property
    def centroid(self):
        if self._centroid is None:
            self._centroid = self.xml_attributes['centroid']
        return self._centroid

    @property
    def center_x(self):
        if self._center_x is None:
            self._center_x = self.xml_attributes['centroid'].x
        return self._center_x

    @property
    def center_y(self):
        if self._center_y is None:
            self.center_y = self.xml_attributes['centroid'].y
        return self._center_y

    @property
    def identifier(self):
        if self._identifier is None:
            self._identifier = self.xml_attributes['identifier']
        return self._identifier

    @property
    def acquisition_datetime(self):
        if self._acquisition_datetime is None:
            self._acquisition_datetime = datetime.datetime.strptime(
                                         self.xml_attributes['acquisitionDateTime'],
                                         '%Y-%m-%dT%H:%M:%S+00:00')
        return self._acquisition_datetime

    @property
    def instrument(self):
        if self._instrument is None:
            self._instrument = self.xml_attributes['instrument']
        return self._instrument

    def serial_identifier(self):
        if self._serial_identifier is None:
            self._serial_identifier = self.xml_attributes['serialIdentifier']

    @property
    def product_type(self):
        if self._product_type is None:
            self._product_type = self.xml_attributes['productType']
        return self._product_type

    @property
    def shelveable(self):
        required_atts = [(self.valid_md5 or self.skip_checksum),
                         self.xml_path.exists(),
                         self.instrument, self.product_type,
                         self.bundle_type, self.item_type,
                         self.acquired_date, self.strip_id]
        if not all([ra for ra in required_atts]):
            logger.warning('Scene unshelveable: {}\n'.format(self.scene_path))
            for ra in required_atts:
                if not ra:
                    logger.warning("{}: {}".format(ra, ra is True))
            self._shelveable = False
        else:
            self._shelveable = True

        return self._shelveable

    @property
    def shelved_dir(self, destination_parent=planet_data_dir):
        acquired_year = self.acquisition_datetime.strftime('%Y')
        # Create month str, like 08_aug
        month_str = '{}_{}'.format(self.acquisition_datetime.month,
                                   self.acquisition_datetime.strftime('%b')
                                   .lower())
        acquired_day = self.acquisition_datetime.strftime('%d')
        return destination_parent / Path(os.path.join(self.instrument,
                                                      self.product_type,
                                                      self.bundle_type,
                                                      self.item_type,
                                                      acquired_year,
                                                      month_str,
                                                      acquired_day,
                                              # TODO: What is final subdir
                                                      self.serial_identifier
                                                      ))

    @property
    def shelved_location(self):
        return self.shelved_dir / self.scene_path.name

    @property
    def index_row(self):
        if self._index_row is None:
            self._index_row = copy.deepcopy(self.xml_attributes)
            self._index_row['id'] = self.item_iddis
            self._index_row['bundle_type'] = self.bundle_type
            self._index_row['geometry'] = self.geometry.wkt
            self._index_row['centroid'] = self.centroid.wkt
            self._index_row['center_x'] = self._center_x
            self._index_row['center_y'] = self._center_y
            self._index_row['received_datetime'] = self.received_datetime
            self._index_row['shelved_loc'] = self.shelved_location
            self._index_row['loc'] = self.scene_path
        return self._index_row

    @property
    def footprint_row(self):
        # TODO: write, such that this only includes attributes
        #  we want in footprints
        if self._footprint_row is None:
            self._footprint_row = copy.deepcopy(self.index_row)
        return self._footprint_row


def find_planet_scenes(directory, exclude_meta=None):
    if not isinstance(directory, pathlib.PurePath):
        directory = Path(directory)
    manifest_files = directory.rglob('*_manifest.json')

    planet_scenes = [PlanetScene(mf, exclude_meta=exclude_meta) for mf in manifest_files]

    return planet_scenes


