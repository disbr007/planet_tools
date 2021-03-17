import argparse
import copy
import datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
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
import lib.constants as constants

logger = create_logger(__name__, 'sh', 'INFO')

# External modules
sys.path.append(str(Path(__file__).parent.parent / '..'))
try:
    from db_utils.db import Postgres, generate_sql, check_where, \
        make_identifier, intersect_aoi_where
except ImportError as e:
    logger.error('db_utils module not found. It should be adjacent to '
                 'the planet_tools directory. Path: \n{}'.format(sys.path))
    sys.exit()

config_file = Path(__file__).parent.parent / "config" / "config.json"


# For identifying scene ids from file names
SCENE_LEVELS = ['1B', '3B']

# Shelving parent directory
PLANET_DATA_DIR = PurePosixPath(
    json.load(open(config_file))[constants.DOWNLOAD_LOC])


def get_config(param):
    try:
        config_params = json.load(open(config_file))
    except FileNotFoundError:
        print('Config file not found at: {}'.format(config_file))
        print('Please create a config.json file based on the example.')

    try:
        config = config_params[param]
    except KeyError:
        print('Config parameter not found: {}'.format(param))
        print('Available configs:\n{}'.format('\n'.join(config_params.keys())))

    return config


def win2linux(path):
    lp = path.replace('V:', r'/mnt').replace('\\', '/')
    return lp


def linux2win(path):
    wp = path.replace(r'//mnt', 'V:')
    wp = wp.replace(r'/mnt', 'V:')
    wp = wp.replace('/', '\\')
    return wp


def get_platform_location(path):
    if platform.system() == constants.LINUX:
        pl = win2linux(path)
    elif platform.system() == constants.WINDOWS:
        pl = linux2win(path)
    return pl


def type_parser(filepath):
    """
    Takes a file path (or dataframe) in and determines whether
    it is a dbf, shp, excel, txt, csv (or dataframe)****
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
        sys.exit(-1)

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
    elif file_type == 'csv':
        df = pd.read_csv(ids_file, sep=sep, )
        ids = list(df[field])

    # dbf
    elif file_type == 'dbf':
        df = gpd.read_file(ids_file)
        ids = list(df[field])

    # shp
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
        df[dc] = df[dc].apply(lambda x: x.strftime(date_format))


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


def write_gdf(gdf, out_footprint, out_format=None,
              date_format='%Y-%m-%d %H:%M:%S'):
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
    if out_format == 'geojson' and gdf.crs != 'epsg:4326':
        logger.warning('Attempting to write GeoDataFrame that is not in '
                       'EPSG:4326 to GeoJSON -> Reprojecting before writing.')
        gdf = gdf.to_crs('epsg:4326')
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


def id_from_scene(scene, scene_levels=SCENE_LEVELS):
    """
    Get the scene id from a given scene's file path
    scene: pathlib.Path
    """
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


def write_scene_manifest(scene_manifest: dict, master_manifest: Path,
                         manifest_suffix: str = constants.MANIFEST_SUFFIX,
                         overwrite: bool = False):
    """
    Write the section of the parent manifest for an individual scene
    to a new file with that scenes name.
    """
    scene_path = Path(scene_manifest[constants.PATH])
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


def get_scene_manifests(master_manifest: str) -> list:
    """
    Parse an order manifest file for the sections corresponding
    to scenes.
    Parameters
    ---------
    master_manifest: str
        Path to order manifest
    Returns
    ---------
    list: list of sections of order manifest corresponding to
        scene image files
    """
    with open(master_manifest, 'r') as src:
        mani = json.load(src)

    # Get metadata for all images
    scene_manifests = []
    for f in mani[constants.FILES]:
        # TODO: DANGER if something changes with order manifest structure
        if f[constants.MEDIA_TYPE].startswith(constants.IMAGE) and constants.UDM not in f[constants.PATH]:
            scene_manifests.append(f)

    return scene_manifests


def create_scene_manifests(master_manifest, overwrite=False):
    """
    Create scene manifest files for each scene section in the master manifest.
    """
    logger.debug('Locating scene manifests within master manifest\n'
                 '{}'.format(master_manifest))
    scene_manifests = get_scene_manifests(master_manifest)
    logger.debug('Scene manifests found: {:,}'.format(len(scene_manifests)))

    # TODO: Make this date format match the xml date format
    received_date = time.strftime('%Y-%m-%dT%H:%M:%S+00:00',
                                  time.localtime(os.path.getmtime(
                                      master_manifest)))

    scene_manifest_files = []
    # pbar = tqdm(scene_manifests,
                # desc='Writing scene-level manifest.json files'
                # )
    # for sm in pbar:
    for sm in scene_manifests:
        sm[constants.RECEIVED_DATETIME] = received_date
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
    """
    Parse XML elements that would overwrite other attributes to
    rename them before adding them to the attributes dict
    """
    elems = root.find('.//{}'.format(node))
    for e in elems:
        uri, name = tag_uri_and_name(e)
        if name in renamer.keys():
            name = renamer[name]
        if e.text.strip() != '':
            attributes[name] = e.text


def find_planet_scenes(directory, exclude_meta=None,
                       shelved_parent=None):
    if not isinstance(directory, pathlib.PurePath):
        directory = Path(directory)
    manifest_files = directory.rglob('*_manifest.json')

    planet_scenes = [PlanetScene(mf, exclude_meta=exclude_meta,
                                 shelved_parent=shelved_parent)
                     for mf in manifest_files]

    return planet_scenes


def locate_manifest(scene):
    pass


def stereo_pair_sql(aoi=None, date_min=None, date_max=None, ins=None,
                    date_diff_min=None, date_diff_max=None,
                    view_angle_diff=None,
                    ovlp_perc_min=None, ovlp_perc_max=None,
                    off_nadir_diff_min=None, off_nadir_diff_max=None,
                    limit=None, orderby=False, orderby_asc=False,
                    remove_id_tbl=None, remove_id_tbl_col=None,
                    remove_id_src_cols=None,
                    geom_col=constants.OVLP_GEOM,
                    columns='*'):
    """
    Create SQL statment to select stereo pairs based on passed
    arguments.
    """
    # Ensure properly quoted identifiers
    remove_id_tbl = make_identifier(remove_id_tbl)

    # TODO: make this a loop over a dict of fields and argss
    where = ""
    if date_min:
        where = check_where(where)
        where += "{} >= '{}'".format(constants.ACQUIRED1, date_min)
    if date_max:
        where = check_where(where)
        where += "{} <= '{}'".format(constants.ACQUIRED1, date_max)
    if ins:
        where = check_where(where)
        where += "{0} = '{1}' AND {2} = '{1}'".format(constants.INSTRUMENT1, ins, constants.INSTRUMENT2)
    if date_diff_max:
        where = check_where(where)
        where += "{} <= {}".format(constants.DATE_DIFF, date_diff_max)
    if date_diff_min:
        where = check_where(where)
        where += "{} <= {}".format(constants.DATE_DIFF, date_diff_min)
    if off_nadir_diff_max:
        where = check_where(where)
        where += "{} <= {}".format(constants.OFF_NADIR_DIFF, off_nadir_diff_max)
    if off_nadir_diff_min:
        where = check_where(where)
        where += "{} >= {}".format(constants.OFF_NADIR_DIFF, off_nadir_diff_min)
    if view_angle_diff:
        where = check_where(where)
        where += "{} >= {}".format(constants.VIEW_ANGLE_DIFF, view_angle_diff)
    if ovlp_perc_min:
        where = check_where(where)
        where += "{} >= {}".format(constants.OVLP_PERC, ovlp_perc_min)
    if ovlp_perc_max:
        where = check_where(where)
        where += "{} >= {}".format(constants.OVLP_PERC, ovlp_perc_max)
    if isinstance(aoi, gpd.GeoDataFrame):
        where = check_where(where)
        where += intersect_aoi_where(aoi, geom_col=geom_col)

    if columns != '*' and geom_col not in columns:
        columns.append(geom_col)

    sql_statement = generate_sql(layer=constants.STEREO_CANDIDATES,
                                 columns=columns,
                                 where=where, limit=limit, orderby=orderby,
                                 orderby_asc=orderby_asc,
                                 remove_id_tbl=remove_id_tbl,
                                 remove_id_tbl_col=remove_id_tbl_col,
                                 remove_id_src_cols=remove_id_src_cols)

    return sql_statement

class PlanetScene:
    def __init__(self, source, exclude_meta=None,
                 shelved_parent=None,
                 scene_file_source=False):
        """A class to represent a Planet scene, including metadata
        file paths, attributes, etc.

        Parameters
        ---------
        source : str
            Path to source file to create PlanetScene objects.
            Preference is to pass scene-level manifest file as all
            metadata can be parsed if the manifest (and .xml) is
            present. However, the path to the scene file itself can be
            passed and the associated manifest will attempt to be
            located. If it cannot be located,
            self.scene_manifest_present will be marked False, and an
            error message will be logged.
            TODO: if self.scene_manifest_present == False, allow
              PlanetScene to be created and limit the available attributes.
        exclude_meta : list
            Optional, list of strings used to remove files that
            otherwise would be matched by self.metafiles.
            E.g: ['_pan', '_metadata.json']
        shelved_parent : str
            Alternative path to build shelved directories on, if not
            passed, default data directory is used. Useful for "shelving"
            in other locations, i.e. for deliveries.
        """
        # TODO: Refactor so that a scene tif or metadata can be passed
        #  as source. Some methods won't be available, but this class
        #  can still be used for locating scene metadata files,
        #  structuring directories by other available metadata, footprinting,
        #  etc.
        if scene_file_source:
            # Try to locate manifest
            source = Path(source)
            manifest = Path('{}_manifest.json'.format(source.parent /
                                                      source.stem))
            if manifest.exists():
                source = manifest
                self.scene_manifest_present = True
            else:
                self.scene_manifest_present = False
                logger.error('Could not locate scene-manifest associated '
                             'with:\n {}'.format(source))

        self.manifest = Path(source)
        if self.manifest.suffix != '.json':
            logger.error('Must pass *_manifest.json file.')

        self.exclude_meta = exclude_meta
        # Parent directory upon which shelved path is built
        if shelved_parent:
            self.shelved_parent = shelved_parent
        else:
            self.shelved_parent = PLANET_DATA_DIR

        # Parse source for attributes
        # TODO: fix how these attributes are parsed currently from
        #  scene-level manifest
        with open(str(self.manifest), 'r') as src:
            data = json.load(src)
            # Find the scene path within the source
            _digests = data[constants.DIGESTS]
            _annotations = data[constants.ANNOTATIONS]
            self.scene_path = self.manifest.parent / \
                              Path(data[constants.PATH]).name
            self.media_type = data[constants.SIZE]
            self.md5 = _digests[constants.MD5]
            self.sha256 = _digests[constants.SHA256]
            self.asset_type = _annotations[constants.PLANET_ASSET_TYPE]
            self.bundle_type = _annotations[constants.PLANET_BUNDLE_TYPE]
            self.item_id = _annotations[constants.PLANET_ITEM_ID]
            self.item_type = _annotations[constants.PLANET_ITEM_TYPE]
            self.received_datetime = data[constants.RECEIVED_DATETIME]

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
            uns_index_row[constants.ID] = self.item_id
            uns_index_row[constants.STRIP_ID] = self.strip_id
            uns_index_row[constants.BUNDLE_TYPE] = self.bundle_type
            uns_index_row[constants.GEOMETRY] = self.geometry
            uns_index_row[constants.CENTROID] = self.centroid
            uns_index_row[constants.CENTER_X] = self._center_x
            uns_index_row[constants.CENTER_Y] = self._center_y
            uns_index_row[constants.RECEIVED_DATETIME] = self.received_datetime
            uns_index_row[constants.SHELVED_LOC] = str(self.shelved_location)
            # Use only linux paths in index - /mnt/pgc/data/.., not V:\pgc\data\...
            if platform.system() == constants.WINDOWS:
                uns_index_row[constants.SHELVED_LOC] = \
                    linux2win(uns_index_row[constants.SHELVED_LOC])

            # Reorder fields
            field_order = [constants.ID,
                           constants.IDENTIFIER,
                           constants.STRIP_ID,
                           constants.ACQUISTIONDATETIME,
                           constants.BUNDLE_TYPE,
                           constants.CENTER_X,
                           constants.CENTER_Y]
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

    def get_footprint_row(self, rel_to=None):
        if self._footprint_row is None:
            self._footprint_row = copy.deepcopy(self.index_row)
            self._footprint_row.pop(constants.SHELVED_LOC, None)
            if rel_to:
                self._footprint_row[constants.REL_LOCATION] = str(
                    self.scene_path.relative_to(rel_to))
        return self._footprint_row

    # @property
    # def footprint_row(self, rel_to=None):
    #     TODO: write, such that this only includes attributes
    #      we want in footprints
        # if self._footprint_row is None:
        #     self._footprint_row = copy.deepcopy(self.index_row)
        #     self._footprint_row.pop(constants.SHELVED_LOC, None)
        #     if rel_to:
        #         self._footprint_row[constants.REL_LOCATION] = str(
        #             self.scene_path.relative_to(rel_to))
        # return self._footprint_row



# TODO: clean up logging around unshelveable scenes
#  (lots of repeated "XML path: None", "XML Path not located")
