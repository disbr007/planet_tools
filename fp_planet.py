import argparse
import os
from pathlib import Path

import shapely
import geopandas as gpd

from lib.logging_utils import create_logger
from lib.lib import write_gdf, find_planet_scenes

logger = create_logger(__name__, 'sh', 'INFO')

choices_format = ['shp', 'gpkg', 'geojson']


def main(args):
    out_footprint = Path(args.out_footprint)
    out_format = args.format
    parse_directory = args.input_directory
    relative_directory = args.relative_directory
    rel_loc_style = args.rel_loc_style

    if not relative_directory:
        relative_directory = parse_directory

    logger.info('Searching for scenes in: {}'.format(parse_directory))
    planet_scenes = find_planet_scenes(parse_directory)
    logger.info('Found {:,} scenes to parse...'.format(len(planet_scenes)))

    # TODO: convert to using PlanetScenes generate footprints
    rows = [ps.footprint_row(rel_to=relative_directory)
            for ps in planet_scenes]
    gdf = gpd.GeoDataFrame(rows)
    gdf['geometry'] = gdf.geometry.apply(lambda x: shapely.wkt.loads(x))
    gdf.crs = 'epsg:4326'

    logger.info('Footprint created with {:,} records.'.format(len(gdf)))

    write_gdf(gdf, out_footprint=out_footprint, out_format=out_format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_directory', type=os.path.abspath,
                        required=True,
                        help='Directory to parse for scenes.')
    parser.add_argument('-o', '--out_footprint', type=os.path.abspath,
                        required=True,
                        help='Path to write footprint.')
    parser.add_argument('-f', '--format', type=str, choices=choices_format,
                        help='Format of footprint to write. If gpkg, specify'
                             ' as: package.gpkg/layer_name.')
    parser.add_argument('-r', '--relative_directory', type=os.path.abspath,
                        help='Path to create filepaths relative to in '
                             'footprint.')
    parser.add_argument('--rel_loc_style', type=str, choices=['W', 'L'],
                        help='System style for paths in footprint.')

    # import sys
    # sys.argv = [r'C:\code\planet_stereo\fp_planet.py',
    #             '-i',
    #             r'V:\pgc\data\scratch\jeff\projects\planet\deliveries'
    #             r'\2020oct14_multilook\2019\10\09',
    #             '-o',
    #             r'V:\pgc\data\scratch\jeff\projects\planet\deliveries'
    #             r'\2020oct14_multilook\2020oct14_multilook.geojson']

    args = parser.parse_args()

    main(args)
