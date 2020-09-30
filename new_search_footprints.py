import argparse
import os
from pathlib import Path

from create_search import create_search, parse_group_args
from get_search_footprints import get_search_footprints
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO',)

def new_search_footprints(args, att_group_args):
    logger.info('Creating search...')
    ssid = create_search(args, att_group_args)
    if ssid:
        logger.info('Getting footprints')
        get_search_footprints(args, search_id=ssid)
    else:
        logger.warning('No saved search ID returned - no footprints to retrieve.')
    
    
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

    parser.add_argument('--months', type=str, nargs='+',
                        help='Month as zero-padded number, e.g. 04')
    parser.add_argument('--month_min_day', nargs=2, action='append',
                        help='Mimumum day to include in a given month: eg. 12 20'
                             'Can be repeated multiple times.')
    parser.add_argument('--month_max_day', nargs=2, action='append',
                        help='Maximum day to include in a given month: eg. 12 20'
                             'Can be repeated multiple times.')
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
    parser.add_argument('-af', '--asset_filter', action='append',
                        help='Asset filter to include.')
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
    parser.add_argument('--not_on_hand', action='store_true',
                        help='Remove on hand IDs from search.')
    parser.add_argument('--fp_not_on_hand', action='store_true',
                        help='Remove IDs from search if footprint is on hand.')
    # parser.add_argument('--get_count', action='store_true',
    #                     help="Pass to get total count for the newly created saved search.")
    parser.add_argument('--overwrite_saved', action='store_true',
                        help='Pass to overwrite a saved search of the same name.')
    parser.add_argument('--save_filter', nargs='?', type=os.path.abspath, const='default.json',
                        help='Path to save filter (json).')

    # get_search_footprints args
    parser.add_argument('-op', '--out_path', type=os.path.abspath,
                        help='Path to write selected scene footprints to.')
    parser.add_argument('-od', '--out_dir', type=os.path.abspath,
                        help="""Directory to write scenes footprint to -
                        the search request name will be used for the filename.""")
    parser.add_argument('--to_tbl', type=str,
                        help="""Insert search results into this table.""")

    parser.add_argument('-d', '--dryrun', action='store_true',
                        help='Do not actually create the saved search.')
    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()
    # Parse attribute arguments to filter
    att_group_args = parse_group_args(parser=parser, group_name=att_group)

    new_search_footprints(args, att_group_args)

