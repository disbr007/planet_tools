import argparse
import os
from pathlib import Path

from lib import write_gdf
from logging_utils.logging_utils import create_logger
from scene_parsing import find_scene_files, metadata_path_from_scene, gdf_from_metadata

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
    scene_files = find_scene_files(parse_directory)
    logger.info('Found {:,} scenes to parse...'.format(len(scene_files)))

    scenes_metadatas = [(s, metadata_path_from_scene(s)) for s in scene_files]
    logger.info('Found {:,} associated metadata files.'.format(len(scenes_metadatas)))

    gdf = gdf_from_metadata(scenes_metadatas, relative_directory=relative_directory,
                            relative_locs=True, pgc_locs=False,
                            rel_loc_style=rel_loc_style)
    logger.info('Footprint created with {:,} records.'.format(len(gdf)))

    write_gdf(gdf, out_footprint=out_footprint, out_format=out_format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--out_footprint', required=True, type=os.path.abspath,
                        help='Path to write footprint.')
    parser.add_argument('-f', '--format', type=str, choices=choices_format,
                        help='Format of footprint to write. If gpkg, specify as: package.gpkg/layer_name.')
    parser.add_argument('-i', '--input_directory', type=os.path.abspath, required=True,
                        help='Directory to parse for scenes.')
    parser.add_argument('-r', '--relative_directory', type=os.path.abspath, default=os.getcwd(),
                        help='Path to create filepaths relative to in footprint.')
    parser.add_argument('--rel_loc_style', type=str, choices=['W', 'L'],
                        help='System style for paths in footprint.')

    args = parser.parse_args()
    print(args)
    main(args)
