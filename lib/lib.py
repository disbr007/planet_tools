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
import shapely
from shapely.geometry import Point, Polygon
from tqdm import tqdm

from .logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# TODO: clean up logging around unshelveable scenes
#  (lots of repeated "XML path: None", "XML Path not located")

# Shelve parent directory
# TODO: move this to a config file
planet_data_dir = Path(r'/mnt/pgc/data/sat/orig')  # /planet?
if platform.system() == 'Windows':
    planet_data_dir = Path(r'V:\pgc\data\sat\orig')
image = 'image'

config_file = Path(__file__).parent.parent / "config" / "config.json"
# config_file = r'C:\code\planet_stereo\config\config.json'

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
# suffix to append to filenames for individual manifest files
manifest_suffix = 'manifest'
# included in unusable data mask files paths
udm = 'udm'
# start of media_type in master manifest that indicates imagery


def get_config(param):
    config_params = json.load(open(config_file))
    try:
        config = config_params[param]
    except KeyError:
        print('Config parameter not found: {}'.format(param))
        print('Available configs:\n{}'.format('\n'.join(config.keys())))

    return config


# def linux2win(path):
#     wp = Path(str(path).replace('/mnt', 'V:').replace('/', '\\'))
#     return wp


def win2linux(path):
    lp = path.replace('V:', r'/mnt').replace('\\', '/')

    return lp


def linux2win(path):
    wp = path.replace(r'//mnt', 'V:')
    wp = wp.replace(r'\mnt', 'V:')
    wp = wp.replace('/', '\\')

    return wp


def get_platform_location(path):
    # TODO: Remove? Not being used as far as I can tell
    if platform.system() == 'Linux':
        pl = win2linux(path)
    elif platform.system() == 'Windows':
        pl = linux2win(path)

    return pl


def type_parser(filepath):
    """
    takes a file path (or dataframe) in and determines whether it is a dbf,
    excel, txt, csv (or df), ADD SUPPORT FOR SHP****
    """
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
    """Reads ids from a variety of file types. Can also read in stereo ids from
     applicable formats
    field: field name, irrelevant for text files, but will search for this name
     if ids_file is .dbf or .shp
    """

    # Determine file type
    file_type = type_parser(ids_file)

    if file_type in ('dbf', 'df', 'gdf', 'shp',
                     'csv', 'excel', 'geojson') and not field:
        logger.error('Must provide field name with file type: '
                     '{}'.format(file_type))
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


def determine_driver(src):
    if not isinstance(src, pathlib.PurePath):
        src = Path(src)

    out_format = src.suffix.replace('.', '')
    if not out_format:
        # If still no extension, check if gpkg (package.gpkg/layer)
        out_format = src.parent.suffix
        if not out_format:
            logger.error('Could not recognize out format from file '
                         'extension: {}'.format(src))
    if out_format == 'shp':
        driver = 'ESRI Shapefile'
    elif out_format == 'geojson':
        driver = 'GeoJSON'
    elif out_format == 'gpkg':
        driver = 'GPKG'
    else:
        logger.error('Unrecognized format: {}'.format(out_format))

    return driver


def get_geometry_cols(gdf):
    """Gets all columns in a geodataframe that are of type geometry.
    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to find geometry columns in

    Returns
    -------
    list : Names of columns that are of type 'geometry'
    """
    shapely_geoms = (shapely.geometry.collection.GeometryCollection,
                     shapely.geometry.linestring.LineString,
                     shapely.geometry.polygon.LinearRing,
                     shapely.geometry.multilinestring.MultiLineString,
                     shapely.geometry.multipoint.MultiPoint,
                     shapely.geometry.multipolygon.MultiPolygon,
                     shapely.geometry.point.Point,
                     shapely.geometry.polygon.Polygon)
    geom_cols = []
    for col in gdf.columns:
        if type(gdf[col].values[0]) in shapely_geoms:
            geom_cols.append(col)

    return geom_cols


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
                logger.error('Could not recognize out format from file '
                             'extension: {}'.format(out_footprint))

    # Write out in format specified
    driver = determine_driver(out_footprint)
    if out_format == 'gpkg':
        gdf.to_file(out_footprint.parent, layer=out_footprint.stem,
                    driver='GPKG')
    else:
        gdf.to_file(out_footprint, driver=driver)


def parse_group_args(parser, group_name):
    # Get just arguments in given group from argparse.ArgumentParser() as
    # Namespace object
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

        logger.debug('Parsing metdata for: '
                     '{}\{}'.format(scene_path.parent.stem, scene_path.stem))
        metadata = json.load(open(metadata_path))
        properties = metadata['properties']

        # Create paths for both Windows and linux
        # Keep only Linux - Use windows for checking existence if code run on
        # windows
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
                properties['geometry'] = Polygon(metadata['geometry']
                                                 ['coordinates'][0])
            elif metadata['geometry']['type'] == 'MultiPolygon':
                logger.warning('MultiPolygon geometry found. Not yet supported'
                               ' - skipping.')
                # logger.warning('Parsing MultiPolygon geometry not fully '
                #                'tested.')
                add_row = False
                # properties['geometry'] = MultiPolygon(
                #     [Polygon(metadata['geometry']['coordinates'][i][0])
                #      for i in range(len(metadata['geometry']
                #      ['coordinates']))])
        except Exception as e:
            logger.error('Geometry error, skipping add scene: '
                         '{}'.format(properties[k_scenes_id]))
            logger.error('Metadata file: {}'.format(metadata_path))
            logger.error('Geometry: {}'.format(metadata['geometry']))
            logger.error(e)
            add_row = False

        if add_row:
            rows.append(properties)
        else:
            logger.warning('Scene could not be found (or had bad geometry), '
                           'skipping adding:\n{}\tat {}'.format(metadata['id'],
                                                                scene_path))

    if len(rows) == 0:
        # TODO: Address how to actually deal with not finding any features
        #   sys.exit()?
        logger.warning('No features to convert to GeoDataFrame.')

    gdf = gpd.GeoDataFrame(rows, geometry='geometry', crs='epsg:4326')

    return gdf


def write_scene_manifest(scene_manifest, master_manifest,
                         manifest_suffix=manifest_suffix,
                         overwrite=False):
    """Write the section of the parent manifest for an individual scene
    to a new file with that scenes name."""
    scene_path = Path(scene_manifest[k_path])
    scene_mani_name = '{}_{}.json'.format(scene_path.stem, manifest_suffix)
    scene_mani_path = (master_manifest.parent / scene_path.parent /
                       scene_mani_name)
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
        # TODO: Identify imagery sections of master manifest better
        if f[k_media_type].startswith(image) and udm not in f[k_path]:
            scene_manifests.append(f)

    return scene_manifests


def create_scene_manifests(master_manifest, overwrite=False):
    """
    Create scene manifest files for each scene section in the master manifest.
    """
    logger.debug('Locating scene manifests within master manifest\n'
                 '{}'.format(master_manifest))
    scene_manifests = get_scene_manifests(master_manifest)
    logger.debug('Scene manifests found: {}'.format(len(scene_manifests)))

    # TODO: Make this date format match the xml date format
    received_date = time.strftime('%Y-%m-%dT%H:%M:%S+00:00',
                                  time.localtime(os.path.getmtime(
                                      master_manifest)))

    scene_manifest_files = []
    pbar = tqdm(scene_manifests,
                desc='Writing scene-level manifest.json files')
    for sm in pbar:
        # TODO: is it worth checking for existence of scene files here?
        #  Any missing scene files are found when determining if shelveable
        # Check for existence of scene file (image file)
        # sf = master_manifest.parent / Path(sm[k_path])
        # if not sf.exists():
        #     logger.warning('Scene file location in master manifest not found: '
        #                    '{}'.format(sf))
        # else:
        #     logger.debug('Scene file found')
        sm['received_datetime'] = received_date
        scene_manifest_file = write_scene_manifest(sm, master_manifest,
                                                   overwrite=overwrite)
        scene_manifest_files.append(scene_manifest_file)

    return scene_manifest_files


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
        logger.warning('Verification of md5 checksum failed for scene: '
                       '{}'.format(scene_file))
        verified = False

    return verified


def verify_all_checksums(scenes, verify_checksums=True):
    # Verify checksum, or mark all as skip if not checking
    if verify_checksums:
        logger.info('Verifying scene checksums...')
        for ps in tqdm(scenes, desc='Verifying scene checksums...'):
            ps.verify_checksum()
    else:
        logger.info('Skipping checksum verification...')
        for ps in scenes:
            ps.skip_checksum = True


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
    alt_shelved_parent : str
        Alternative path to build shelved directories on, if not
        passed, default data directory is used. Useful for "shelving"
        in other locations, i.e. for deliveries.
    """
    def __init__(self, manifest, exclude_meta=None,
                 shelved_parent=None):
        self.manifest = Path(manifest)
        self.exclude_meta = exclude_meta
        # Parent directory upon which shelved path is built
        if shelved_parent:
            self.shelved_parent = shelved_parent
        else:
            self.shelved_parent = planet_data_dir

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

        # Determine "scene name" - the scene name without post processing
        # suffixes used when searching for metadata files, e.g.: _SR
        self.scene_name = self.scene_path.stem
        scene_sfxs = ['_SR', '_DN', ]
        if any([sfx in self.scene_name for sfx in scene_sfxs]):
            for sfx in scene_sfxs:
                self.scene_name = self.scene_name.replace(sfx, '')

        # Empty attributes calculated from methods
        self._shelveable = None
        self._shelved_dir = None
        self._shelved_location = None
        self._is_shelved = None
        self._meta_files = None
        self._metadata_json = None
        self._xml_path = None
        self._scene_files = None
        self._valid_checksum = None
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
        # TODO: Setting xml_valid to None then updating to True in
        #  xml_attributes() not working (same for strip_id_found)
        self.xml_valid = True
        self.strip_id_found = True
        self.skip_checksum = None
        self.indexed = None
        self._index_row = None
        self._footprint_row = None

        # Bundle types that have been tested for compatibility with naming
        # conventions
        self.supported_bundle_types = ['analytic', 'analytic_sr',
                                       'basic_analytic', 'basic_analytic_nitf',
                                       'basic_uncalibrated_dn',
                                       'basic_uncalibrated_dn_ntif',
                                       'uncalibrated_dn']
        # Ensure bundle_type is suppported
        if self.bundle_type not in self.supported_bundle_types:
            logger.warning('Bundle type not tested: '
                           '{}'.format(self.bundle_type))

    @property
    def metadata_json(self):
        if self._metadata_json is None:
            self._metadata_json = self.scene_path.parent \
                                 / '{}_metadata.json'.format(id_from_scene(self.scene_path))

        return self._metadata_json

    @ property
    def strip_id(self):
        if self._strip_id is None and self.strip_id_found is not False:
            if self.metadata_json.exists():
                try:
                    with open(self.metadata_json) as src:
                        content = json.load(src)
                        self._strip_id = content['properties']['strip_id']
                        self.strip_id_found = True
                except Exception as e:
                    self.strip_id_found = False
                    logger.warning('Error getting strip_id for scene: '
                                   '{}'.format(self.scene_path))
                    logger.error('Error message: {}'.format(e))
        return self._strip_id

    @property
    def meta_files(self):
        """Locate metadata files for the scene, not including the scene itself and
        excluding any subtrings passed in self.exlude_meta"""
        if self._meta_files is None:
            logger.debug('Locating metadata files for: '
                         '{}'.format(self.scene_path))
            self._meta_files = [f for f in
                                self.scene_path.parent.rglob(
                                    '{}*'.format(self.scene_name)
                                )
                                if f != self.scene_path]
            if self.metadata_json.exists():
                self._meta_files.append(self.metadata_json)
            else:
                logger.debug('Metadata JSON not found for: '
                             '{}'.format(self.scene_path))
            if self.exclude_meta:
                self._meta_files = [f for f in self._meta_files
                                    if not any([em in str(f)
                                                for em in self.exclude_meta])]
        return self._meta_files

    @property
    def scene_files(self):
        if self._scene_files is None:
            self._scene_files = self.meta_files + [self.scene_path]
        return self._scene_files

    @property
    def valid_checksum(self):
        if self._valid_checksum is None:
            self._valid_checksum = verify_scene_md5(self.md5, self.scene_path)
        return self._valid_checksum

    def verify_checksum(self):
        if not self.skip_checksum:
            return self.valid_checksum

    @property
    def xml_path(self):
        if self._xml_path is None:
            xml_expr = re.compile('.*.xml')
            xml_matches = list(p for p in self.meta_files if xml_expr.match(str(p)))
            if len(xml_matches) == 1:
                if xml_matches[0].exists():
                    self._xml_path = xml_matches[0]
            elif len(xml_matches) == 0:
                logger.debug('XML not located.')
                logger.debug('Scene metafiles: {}'.format([str(mf) for mf in
                                                           self.meta_files]))
            else:
                logger.debug('Multiple potential XML matches '
                             'found for scene: {}'.format(self.scene_path))
        return self._xml_path

    @property
    def xml_attributes(self):
        if self._xml_attributes is None and self.xml_valid is not False:
            if self.xml_path is not None:
                # XML date format
                date_format = '%Y-%m-%dT%H:%M:%S+00:00'
                # XML attribute keys
                k_identifier = 'identifier'
                k_instrument = 'instrument'
                k_productType = 'productType'
                k_acquired = 'acquisitionDateTime'

                logger.debug('Parsing attributes from xml: '
                             '{}'.format(self.xml_path))
                try:
                    with open(self.xml_path, 'rt') as src:
                        tree = ET.parse(self.xml_path)
                        root = tree.getroot()
                except Exception as e:
                    logger.error('Error reading XML metadata file: '
                                 '{}'.format(self.xml_path))
                    self.xml_valid = False
                    return

                # Nodes where all values can be processed as-is
                nodes_process_all = [
                    '{http://schemas.planet.com/ps/v1'
                    '/planet_product_metadata_geocorrected_level}'
                    'EarthObservationMetaData',
                    '{http://www.opengis.net/gml}'
                    'TimePeriod',
                    '{http://schemas.planet.com/ps/v1'
                    '/planet_product_metadata_geocorrected_level}'
                    'Sensor',
                    '{http://schemas.planet.com/ps/v1'
                    '/planet_product_metadata_geocorrected_level}'
                    'Acquisition',
                    '{http://schemas.planet.com/ps/v1'
                    '/planet_product_metadata_geocorrected_level}'
                    'ProductInformation',
                    '{http://earth.esa.int/opt}'
                    'cloudCoverPercentage',
                    '{http://earth.esa.int/opt}'
                    'cloudCoverPercentageQuotationMode',
                    '{http://schemas.planet.com/ps/v1'
                    '/planet_product_metadata_geocorrected_level}'
                    'unusableDataPercentage',
                    ]

                # Nodes with repeated attribute names
                # -> rename according to dicts
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
                                'referenceSystemIdentifier':
                                    'mask_referenceSystemIdentifier'}
                geom_renamer = {'coordinates': 'geometry'}
                centroid_renamer = {'pos': 'centroid'}

                rename_nodes = [(platform_node, platform_renamer),
                                (instrument_node, insrument_renamer),
                                (mask_node, mask_renamer),
                                (geom_node, geom_renamer),
                                (centroid_node, centroid_renamer)]

                # Bands Node - conflicting attribute names
                # -> add band number: "band1_radiometicScaleFactor"
                bands_node = ('{http://schemas.planet.com/ps/v1'
                              '/planet_product_metadata_geocorrected_level}'
                              'bandSpecificMetadata')
                band_number_node = ('{http://schemas.planet.com/ps/v1'
                                    '/planet_product_metadata_geocorrected_'
                                    'level}'
                                    'bandNumber')

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
                    add_renamed_attributes(node, renamer, root=root,
                                           attributes=attributes)

                # Process band metadata
                bands_elems = root.findall('.//{}'.format(bands_node))
                for band in bands_elems:
                    band_uri = '{{{}}}'.format(tag_uri_and_name(band)[0])
                    band_number = (band.find('.//{}'.format(band_number_node)).
                                   text)
                    band_renamer = {tag_uri_and_name(e)[1]:
                                    'band{}_{}'.format(band_number,
                                                       tag_uri_and_name(e)[1])
                                    for e in band
                                    if tag_uri_and_name(e)[1] != 'bandNumber'}
                    for e in band:
                        band_uri, name = tag_uri_and_name(e)
                        if name in band_renamer.keys():
                            name = band_renamer[name]
                        # Remove quotes from field names (some have them, some
                        # do not)
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
                self._centroid = Point(float(x) for x in
                                       attributes['centroid'].split())
                self._center_x = self._centroid.x
                self._center_y = self._centroid.y

                self._xml_attributes = attributes
                self._identifier = attributes[k_identifier]
                self._acquisition_datetime = datetime.datetime.strptime(
                                             attributes[k_acquired],
                                             date_format)
                self._instrument = attributes[k_instrument]
                self._product_type = attributes[k_productType]

                # Mark xml as valid
                self.xml_valid = True

            else:
                self._xml_attributes = None
                self.xml_valid = False

        return self._xml_attributes

    @property
    def geometry(self):
        if self._geometry is None and self.xml_attributes is not None:
            self._geometry = self.xml_attributes['geometry']
        return self._geometry

    @property
    def centroid(self):
        if self._centroid is None and self.xml_attributes is not None:
            self._centroid = self.xml_attributes['centroid']
        return self._centroid

    @property
    def center_x(self):
        if self._center_x is None and self.xml_attributes is not None:
            self._center_x = self.xml_attributes['centroid'].x
        return self._center_x

    @property
    def center_y(self):
        if self._center_y is None and self.xml_attributes is not None:
            self.center_y = self.xml_attributes['centroid'].y
        return self._center_y

    @property
    def identifier(self):
        if self._identifier is None and self.xml_attributes is not None:
            self._identifier = self.xml_attributes['identifier']
        return self._identifier

    @property
    def acquisition_datetime(self):
        if (self._acquisition_datetime is None and
                self.xml_attributes is not None):
            self._acquisition_datetime = datetime.datetime.strptime(
                                         self.xml_attributes['acquisitionDateTime'],
                                         '%Y-%m-%dT%H:%M:%S+00:00')
        return self._acquisition_datetime

    @property
    def instrument(self):
        if self._instrument is None and self.xml_attributes is not None:
            self._instrument = self.xml_attributes['instrument']
        return self._instrument

    @property
    def serial_identifier(self):
        if self._serial_identifier is None and self.xml_attributes is not None:
            self._serial_identifier = self.xml_attributes['serialIdentifier']
        return self._serial_identifier

    @property
    def product_type(self):
        if self._product_type is None and self.xml_attributes is not None:
            self._product_type = self.xml_attributes['productType']
        return self._product_type

    @property
    def shelveable(self):
        """True if all attributes necessary to shelve are found."""
        required_atts = [self.scene_path.exists(),
                         self.xml_path,
                         self.xml_valid,
                         self.instrument,
                         self.product_type,
                         self.bundle_type,
                         self.item_type,
                         self.acquisition_datetime,
                         self.strip_id,
                         self.strip_id_found,
                         # (self.verify_checksum() or self.skip_checksum)
                         ]
        if not all([ra for ra in required_atts]):
            logger.debug('Scene unshelveable: {}\n'.format(self.scene_path))
            logger.debug('Scene exists: {}'.format(self.scene_path.exists()))
            logger.debug('XML path: {}'.format(self.xml_path))
            logger.debug('XML path exists: {}'.format(self.xml_path.exists()
                                                      if self.xml_path else
                                                      None))
            logger.debug('XML parseable: {}'.format(self.xml_valid))
            logger.debug('Instrument: {}'.format(self.instrument))
            logger.debug('Product type: {}'.format(self.product_type))
            logger.debug('Bundle type: {}'.format(self.bundle_type))
            logger.debug('Item type: {}'.format(self.item_type))
            logger.debug('Strip ID: {}'.format(self.strip_id))
            logger.debug('Acquistion datetime: '
                         '{}'.format(self.acquisition_datetime))
            # logger.debug('Checksum: {}'.format(self.verify_checksum() if not
            #                                    self.skip_checksum else
            #                                    self.skip_checksum))
            self._shelveable = False
        else:
            self._shelveable = True

        return self._shelveable

    @property
    def shelved_dir(self):
        if self._shelved_dir is None:
            acquired_year = self.acquisition_datetime.strftime('%Y')
            # Create month str, like 08_aug
            month_str = self.acquisition_datetime.strftime('%m')
            acquired_day = self.acquisition_datetime.strftime('%d')
            # acquired_hour = self.acquisition_datetime.strftime('%H')
            self._shelved_dir = self.shelved_parent.joinpath(
                                self.instrument.replace('.', ''),
                                self.product_type,
                                self.bundle_type,
                                self.item_type,
                                acquired_year,
                                month_str,
                                acquired_day,
                                self.strip_id)
        return self._shelved_dir

    @property
    def shelved_location(self):
        return self.shelved_dir / self.scene_path.name

    @ property
    def is_shelved(self):
        if self._is_shelved is None:
            self._is_shelved = self.shelved_location.exists()
        return self._is_shelved

    @property
    def index_row(self):
        if self._index_row is None:
            # Get all attributes in XML, add others - unsorted
            uns_index_row = copy.deepcopy(self.xml_attributes)
            uns_index_row['id'] = self.item_id
            uns_index_row['strip_id'] = self.strip_id
            uns_index_row['bundle_type'] = self.bundle_type
            uns_index_row['geometry'] = self.geometry
            uns_index_row['centroid'] = self.centroid
            uns_index_row['center_x'] = self._center_x
            uns_index_row['center_y'] = self._center_y
            uns_index_row['received_datetime'] = self.received_datetime
            uns_index_row['shelved_loc'] = str(self.shelved_location)
            # Reorder fields
            field_order = ['id', 'identifier', 'strip_id',
                           'acquisitionDateTime', 'bundle_type', 'center_x',
                           'center_y']
            self._index_row = {k: uns_index_row[k] for k in field_order}
            for k, v in uns_index_row.items():
                if k not in self._index_row.keys():
                    self._index_row[k] = v

            # Make lowercase fields
            # TODO: Do this when parsing
            self._index_row = {k.lower(): v for k, v in self._index_row.items()}

            # TODO: Remove to add centroid back in
            # self._index_row.pop('centroid')

        return self._index_row

    @property
    def footprint_row(self, rel_to=None):
        # TODO: write, such that this only includes attributes
        #  we want in footprints
        if self._footprint_row is None:
            self._footprint_row = copy.deepcopy(self.index_row)
            self._footprint_row.pop('shelved_loc', None)
            if rel_to:
                self._footprint_row[k_relative_loc] = str(
                    self.scene_path.relative_to(rel_to))
        return self._footprint_row


def find_planet_scenes(directory, exclude_meta=None,
                       shelved_parent=None):
    if not isinstance(directory, pathlib.PurePath):
        directory = Path(directory)
    manifest_files = directory.rglob('*_manifest.json')

    planet_scenes = [PlanetScene(mf, exclude_meta=exclude_meta,
                                 shelved_parent=shelved_parent)
                     for mf in manifest_files]

    return planet_scenes
