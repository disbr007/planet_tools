import argparse
import json
import os
import pathlib
from pathlib import Path
import platform
from tqdm import tqdm

import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

windows = 'Windows'
linux = 'Linux'
location = 'location'
relative_loc = 'rel_location'
scenes_id = 'id'
order_id = 'order_id'
filename = 'filename'


def win2linux(path):
    lp = path.replace('\\', '/').replace('V:', '/mnt')

    return lp


def linux2win(path):
    wp = path.replace('/', '\\').replace('/mnt', 'V:')

    return wp


def get_platform_location(path):
    if platform.system() == 'Linux':
        pl = win2linux(path)
    elif platform.system() == 'Windows':
        pl = linux2win(path)

    return pl


def id_from_scene(scene, scene_levels=['1B']):
    """scene : pathlib.Path"""
    if not isinstance(scene, pathlib.PurePath):
        scene = Path(scene)
    scene_name = scene.stem
    scene_id = None
    for lvl in scene_levels:
        if lvl in scene_name:
            scene_id = scene_name.split('_{}_'.format(lvl))[0]
    if not scene_id:
        x=1
        # logger.error('Could not parse scene ID with any level in {}: {}'.format(scene_levels, scene_name))

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
            properties[location] = tn
        if relative_locs:
            rl = Path(scene_path).relative_to(Path(relative_directory))
            if rel_loc_style == 'W' and platform.system() == 'Linux':
                properties[relative_loc] = linux2win(str(rl))
            elif rel_loc_style == 'L' and platform.system() == 'Windows':
                properties[relative_loc] = win2linux(str(rl))
            else:
                properties[relative_loc] = str(rl)
            oid = Path(metadata_path).relative_to(relative_directory).parts[0]
        properties[scenes_id] = metadata['id']
        # properties[order_id] = oid
        properties[filename] = scene_path.name

        try:
            # TODO: Figure out why some footprints are multipolygon
            if metadata['geometry']['type'] == 'Polygon':
                properties['geometry'] = Polygon(metadata['geometry']['coordinates'][0])
            elif metadata['geometry']['type'] == 'MultiPolygon':
                logger.warning('Skipping MultiPolygon geometry')
                add_row = False
                # properties['geometry'] = MultiPolygon([Polygon(metadata['geometry']['coordinates'][i][0])
                #                                        for i in range(len(metadata['geometry']['coordinates']))])
        except Exception as e:
            # TODO: Look for geometry in scenes table if bad geom in metadata
            logger.error('Geometry error, skipping add scene: {}'.format(properties[scenes_id]))
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


def metadata_path_from_scene(scene):
    par_dir = scene.parent
    sid = id_from_scene(scene)
    metadata_path = par_dir / '{}_metadata.json'.format(sid)

    return metadata_path


def find_scene_files(data_directory):
    scenes_to_parse = []
    for root, dirs, files in os.walk(data_directory):
        for f in files:
            if f.endswith('.tif') and not f.endswith('_udm.tif'):
                scene = Path(root) / f
                scenes_to_parse.append(scene)

    return scenes_to_parse
