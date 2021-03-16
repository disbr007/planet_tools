import argparse
import os

from lib.lib import parse_group_args
from lib.search import create_search, get_search_footprints
from lib.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO',)


def search4footprints(**kwargs):
    logger.info('Creating search...')
    ssid, search_count = create_search(**kwargs)
    if ssid:
        logger.info('Getting footprints: {}'.format(search_count))
        get_search_footprints(search_id=ssid, **kwargs)
    else:
        logger.warning('No saved search ID returned - no footprints to '
                       'retrieve.')
    
    
if __name__ == '__main__':
    # Choices
    choices_instruments = ['PS2', 'PSB.SD', 'PS2.SD']
    choices_quality_category = ['standard', 'test']

    parser = argparse.ArgumentParser(
        description="Search for footprints from the Planet archives. " 
        "Search can be specified using attribue arguments, or by using " 
        "the --filters argument and specified syntax. Functionality "
        "provided to save the created filter out as a .json file, which "
        "can later be used with the --load_filter argument. Resulting "
        "footprints can be written out as vector file to --out_path, or "
        "--out_dir with the created search name. Footprints can also be "
        "written directly to 'scenes' database table by using --to_scenes_tbl.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    required_args = parser.add_argument_group('Required Arguments')
    attribute_args = parser.add_argument_group('Attribute Arguments')

    required_args.add_argument('-n', '--name', type=str,
                               help='Name of search to create')

    parser.add_argument('--ids', type=os.path.abspath,
                        help='Text file of IDs to search for.')
    parser.add_argument('--months', type=str, nargs='+',
                        help='Month as zero-padded number, e.g. 04')
    parser.add_argument('--month_min_day', nargs=2, action='append',
                        help='Mimumum day to include in a given month: '
                             'eg. 12 20. Can be repeated multiple times.')
    parser.add_argument('--month_max_day', nargs=2, action='append',
                        help='Maximum day to include in a given month: '
                             'eg. 12 20. Can be repeated multiple times.')
    attribute_args.add_argument('--min_date', type=str,
                                help='Minimum date to include: YYYY-MM-DD')
    attribute_args.add_argument('--max_date', type=str,
                                help='Maximum date to include: YYYY-MM-DD')
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
                        help='Item types to search. E.g.: PSScene3Band, '
                             'PSScene4Band')
    parser.add_argument('-af', '--asset_filter', action='append',
                        help='Asset filter to include. E.g.: basic_analytic')
    parser.add_argument('-f', '--filters', action='append', nargs='*',
                        # metavar=('filter_type', 'field_name', 'config'),
                        help="Add any raw filters. Filter types and syntax:\n"
                             "'DateRangeFilter' 'acquired'   [compare] [yyyy-mm-dd]\n"
                             "'NumberInFilter'  [field_name] [value]\n"
                             "'StringInFilter'  [field_name] [value]\n"
                             "'GeometryFilter'  [path]\n"
                             "'RangeFilter'     [field]      [compare] [value]")
    parser.add_argument('-lf', '--load_filter', type=os.path.abspath,
                        help='Base filter to load, upon which any provided '
                             'filters will be added.')

    # parser.add_argument('--not_on_hand', action='store_true',
    #                     help='Remove on hand IDs from search.')
    # parser.add_argument('--fp_not_on_hand', action='store_true',s
    #                     help='Remove IDs from search if footprint is on hand.')

    parser.add_argument('--get_count_only', action='store_true',
                        help="Pass to only get total count for the newly "
                             "created saved search without retreiving "
                             "footprints.")

    parser.add_argument('--overwrite_saved', action='store_true',
                        help='Pass to overwrite a saved search (on the Planet '
                             'API) of the same '
                             'name.')
    parser.add_argument('--save_filter', nargs='?', type=os.path.abspath,
                        const='default.json',
                        help='Path to save filter (json) locally. The save will '
                             'be saved with or without this argument in your '
                             'Planet account unless you use --get_count_only')
    # Writing arguments
    parser.add_argument('-op', '--out_path', type=os.path.abspath,
                        help='Path to write selected scene footprints to.')
    parser.add_argument('-od', '--out_dir', type=os.path.abspath,
                        help="Directory to write scenes footprint to. The "
                             "search request name will be used for the filename.")
    parser.add_argument('--to_scenes_tbl', action='store_true',
                        help="Insert search results into the scenes table.")
    parser.add_argument('-d', '--dryrun', action='store_true',
                        help='Do not actually create the saved search.')
    parser.add_argument('-v', '--verbose', action='store_true')

    # For debugging
    # import sys
    # sys.argv = [__file__,
    #             '-n', 'mcdv_test',
    #             '--it', 'PSScene4Band',
    #             '-af', 'basic_analytic',
    #             '--aoi', r'V:\pgc\data\scratch\jeff\projects\planet\aois'
    #                      r'\taylor_valley\Taylor_Valley_check.shp',
    #             '-op', r'C:\temp\test_fps.shp',
    #             '--overwrite_saved',
    #             '--get_count_only']

    args = parser.parse_args()
    # Parse attribute arguments to handke seperately
    attrib_args = parse_group_args(parser=parser,
                                   group_name='Attribute Arguments')
    attrib_args = {k: v for k, v in attrib_args._get_kwargs()}

    kwargs = {'name': args.name,
              'aoi': args.aoi,
              'item_types': args.item_types,
              'ids': args.ids,
              'months': args.months,
              'month_min_day_args': args.month_min_day,
              'month_max_day_args': args.month_max_day,
              'filters': args.filters,
              'asset_filters': args.asset_filter,
              'load_filter': args.load_filter,
              # 'not_on_hand': args.not_on_hand,
              # 'fp_not_on_hand': args.fp_not_on_hand,
              'get_count_only': args.get_count_only,
              'overwrite_saved': args.overwrite_saved,
              'save_filter': args.save_filter,
              'dryrun': args.dryrun,
              'attrib_args': attrib_args,
              'out_path': args.out_path,
              'out_dir': args.out_dir,
              'to_scenes_tbl': args.to_scenes_tbl,
              'dryrun': args.dryrun,
              }

    # TODO: verify date arguments are valid dates - planet API request will
    #  fail if something like 2020-06-31 is provided. (June has 30 days)
    search4footprints(**kwargs)
