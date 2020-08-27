from pprint import pprint
import argparse
import os
import json

import geopandas as gpd

from lib import parse_group_args
from search_utils import filter_from_arg, create_search_request, create_saved_search, get_search_count, \
    create_master_attribute_filter, create_master_geom_filter
from logging_utils.logging_utils import create_logger


if __name__ == '__main__':
    # Groups
    att_group = 'Attributes'

    # Defaults

    # Choices
    choices_instruments = ['PS2', 'PSB.SD', 'PS2.SD']
    choices_quality_category = ['standard', 'test']

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)

    attribute_args = parser.add_argument_group(att_group)

    parser.add_argument('-n', '--name', type=str, help='Name of search to create')

    attribute_args.add_argument('--min_date', type=str,)
    attribute_args.add_argument('--max_date', type=str,)
    attribute_args.add_argument('--max_cc', type=float, )
    attribute_args.add_argument('--min_ulx', type=float, )
    attribute_args.add_argument('--max_ulx', type=float, )
    attribute_args.add_argument('--min_uly', type=float, )
    attribute_args.add_argument('--max_uly', type=float, )
    attribute_args.add_argument('--min_sun_azimuth', type=float)
    attribute_args.add_argument('--max_sun_azimuth', type=float)
    attribute_args.add_argument('--max_sun_elevation', type=float,)
    attribute_args.add_argument('--min_sun_elevation', type=float,)
    attribute_args.add_argument('--max_usable_data', type=float,)
    attribute_args.add_argument('--provider', type=str, nargs='+',
                                choices=['planetscope', 'rapideye'])
    attribute_args.add_argument('--satellite_id', type=str, nargs='+', )
    attribute_args.add_argument('--instrument', type=str, nargs='+',
                                choices=choices_instruments, )
    attribute_args.add_argument('--strip_id', type=str, nargs='+', )
    attribute_args.add_argument('--quality_category', type=str, nargs='+',
                                choices=choices_quality_category)
    attribute_args.add_argument('--ground_control', type=bool, nargs='+')

    parser.add_argument('--aoi', type=os.path.abspath,
                        help='Path to AOI vector file to use for selection.')

    parser.add_argument('-it', '--item_types', nargs='*', required=True,
                        help='Item types to search. E.g.: PSScene3Band, PSScene4Band')
    parser.add_argument('-af', '--asset_filter', )
    parser.add_argument('-f', '--filters', action='append', nargs='*',
                        # metavar=('filter_type', 'field_name', 'config'),
                        help="""Add any raw filters. Filter types and syntax:\n
                        'DateRangeFilter' 'acquired'   [compare] [yyyy-mm-dd]\n
                        'NumberInFilter'  [field_name] [value]\n
                        'StringInFilter'  [field_name] [value]\n
                        'GeometryFilter'  [path]\n
                        'RangeFilter'     [field]      [compare] [value]""")
    parser.add_argument('-lf', '--load_filter', type=os.path.abspath,
                        help='Base filter to load, upon which any provided filters will be added.')
    parser.add_argument('--get_count', action='store_true',
                        help="Pass to get total count for the newly created saved search.")
    parser.add_argument('--overwrite_saved', action='store_true',
                        help='Pass to overwrite a saved search of the same name.')
    parser.add_argument('--save_filter', nargs='?', type=os.path.abspath, const='default.json',
                        help='Path to save filter (json).')
    parser.add_argument('-d', '--dryrun', action='store_true',
                        help='Do not actually create the saved search.')
    parser.add_argument('-v', '--verbose', action='store_true')


    args = parser.parse_args()

    if args.verbose:
        log_lvl = 'DEBUG'
    else:
        log_lvl = 'INFO'

    logger = create_logger(__name__, 'sh', log_lvl)
    logger.info('Creating saved search...')

    name = args.name
    aoi = args.aoi
    item_types = args.item_types
    filters = args.filters
    load_filter = args.load_filter
    get_count = args.get_count
    overwrite_saved = args.overwrite_saved
    save_filter = args.save_filter
    dryrun = args.dryrun

    search_filters = []
    # Parse attribute arguments to filter
    att_group_args = parse_group_args(parser=parser, group_name=att_group)
    if any([kwa[1] for kwa in att_group_args._get_kwargs()]):
        master_attribute_filter = create_master_attribute_filter(att_group_args)
        search_filters.append(master_attribute_filter)
    # Parse AOI to filter
    if aoi:
        aoi_attribute_filter = create_master_geom_filter(vector_file=aoi)
        search_filters.append(aoi_attribute_filter)
    # Parse raw filters
    if filters:
        search_filters.extend([filter_from_arg(f) for f in filters])
    # Parse any provided filters
    if load_filter:
        addtl_filter = json.load(open(load_filter))
        search_filters.append(addtl_filter)

    # Create search request using the filters created abov
    sr = create_search_request(name=name, item_types=item_types, search_filters=search_filters)
    if save_filter:
        if os.path.basename(save_filter) == 'default.json':
            save_filter = os.path.join(os.path.dirname(__file__), 'config', 'search_filters', '{}.json'.format(name))
        logger.debug('Saving filter to: {}'.format(save_filter))
        with open(save_filter, 'w') as src:
            json.dump(sr, src)

    if logger.level == 10:
        # pprint was happening even when logger.level = 20 (INFO)
        logger.debug('Search request:\n{}'.format(pprint(sr)))

    if get_count:
        total_count = get_search_count(sr)
        logger.info('Count for new search "{}": {:,}'.format(name, total_count))

    if not dryrun:
        # Submit search request as saved search
        ss_id = create_saved_search(search_request=sr, overwrite_saved=overwrite_saved)
        if ss_id:
            logger.info('Successfully created new search. Search ID: {}'.format(ss_id))
