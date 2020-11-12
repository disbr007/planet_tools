import argparse
import json
import os
import requests

from pprint import pprint

from lib.logging_utils import create_logger
from lib.search import get_all_searches, delete_saved_search


def list_searches(session, verbose=False):
    all_searches = get_all_searches(session)
    if verbose:
        logger.debug('Saved searches:\n')
        logger.debug(pprint(all_searches))
    logger.info('Saved searches:\n{}'.format('\n'.join(['{}: {}'.format(all_searches[s]['name'], s) for s in all_searches])))


def write_searches(session, out_json):
    all_searches = get_all_searches(session)
    logger.info('Writing saved searches to: {}'.format(out_json))
    with open(out_json, 'w') as oj:
        json.dump(all_searches, oj)

# CLI create_search
# if __name__ == '__main__':
#     # Groups
#     att_group = 'Attributes'
#
#     # Defaults
#
#     # Choices
#     choices_instruments = ['PS2', 'PSB.SD', 'PS2.SD']
#     choices_quality_category = ['standard', 'test']
#
#     parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
#
#     attribute_args = parser.add_argument_group(att_group)
#
#     parser.add_argument('-n', '--name', type=str, help='Name of search to create')
#
#     parser.add_argument('--months', type=str, nargs='+',
#                         help='Month as zero-padded number, e.g. 04')
#     parser.add_argument('--month_min_day', nargs=2, action='append',
#                         help='Mimumum day to include in a given month: eg. 12 20 '
#                              'Can be repeated multiple times.')
#     parser.add_argument('--month_max_day', nargs=2, action='append',
#                         help='Maximum day to include in a given month: eg. 12 20'
#                              'Can be repeated multiple times.')
#     attribute_args.add_argument('--min_date', type=str,)
#     attribute_args.add_argument('--max_date', type=str,)
#     attribute_args.add_argument('--max_cc', type=float, )
#     attribute_args.add_argument('--min_ulx', type=float, )
#     attribute_args.add_argument('--max_ulx', type=float, )
#     attribute_args.add_argument('--min_uly', type=float, )
#     attribute_args.add_argument('--max_uly', type=float, )
#     attribute_args.add_argument('--min_sun_azimuth', type=float)
#     attribute_args.add_argument('--max_sun_azimuth', type=float)
#     attribute_args.add_argument('--max_sun_elevation', type=float,)
#     attribute_args.add_argument('--min_sun_elevation', type=float,)
#     attribute_args.add_argument('--max_usable_data', type=float,)
#     attribute_args.add_argument('--provider', type=str, nargs='+',
#                                 choices=['planetscope', 'rapideye'])
#     attribute_args.add_argument('--satellite_id', type=str, nargs='+', )
#     attribute_args.add_argument('--instrument', type=str, nargs='+',
#                                 choices=choices_instruments, )
#     attribute_args.add_argument('--strip_id', type=str, nargs='+', )
#     attribute_args.add_argument('--quality_category', type=str, nargs='+',
#                                 choices=choices_quality_category)
#     attribute_args.add_argument('--ground_control', type=bool, nargs='+')
#
#     parser.add_argument('--aoi', type=os.path.abspath,
#                         help='Path to AOI vector file to use for selection.')
#
#     parser.add_argument('-it', '--item_types', nargs='*', required=True,
#                         help='Item types to search. E.g.: PSScene3Band, PSScene4Band')
#     parser.add_argument('-af', '--asset_filter', action='append',
#                         help='Asset filter to include. Can be repeated E.g.: '
#                              '"basic_analytic" "analytic_sr", etc.')
#     parser.add_argument('-f', '--filters', action='append', nargs='*',
#                         # metavar=('filter_type', 'field_name', 'config'),
#                         help="""Add any raw filters. Filter types and syntax:\n
#                         'DateRangeFilter' 'acquired'   [compare] [yyyy-mm-dd]\n
#                         'NumberInFilter'  [field_name] [value]\n
#                         'StringInFilter'  [field_name] [value]\n
#                         'GeometryFilter'  [path]\n
#                         'RangeFilter'     [field]      [compare] [value]""")
#     parser.add_argument('-lf', '--load_filter', type=os.path.abspath,
#                         help='Base filter to load, upon which any provided filters will be added.')
#     parser.add_argument('--not_on_hand', action='store_true',
#                         help='Remove on hand IDs from search.')
#     parser.add_argument('--fp_not_on_hand', action='store_true',
#                         help='Remove IDs from search if footprint is on hand.')
#     # parser.add_argument('--get_count', action='store_true',
#     #                     help="Pass to get total count for the newly created saved search.")
#     parser.add_argument('--overwrite_saved', action='store_true',
#                         help='Pass to overwrite a saved search of the same name.')
#     parser.add_argument('--save_filter', nargs='?', type=os.path.abspath, const='default.json',
#                         help='Path to save filter (json).')
#     parser.add_argument('-d', '--dryrun', action='store_true',
#                         help='Do not actually create the saved search.')
#     parser.add_argument('-v', '--verbose', action='store_true')
#
#
#     args = parser.parse_args()
#     # Parse attribute arguments to filter
#     att_group_args = parse_group_args(parser=parser, group_name=att_group)
#
#     create_search(args, att_group_args)

###
if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-sl', '--searches_list', action='store_true',
                        help='List all saved searches as name: id')
    parser.add_argument('-sj', '--searches_json', type=os.path.abspath,
                        help='Write searches of all searches to file passed. Specify "verbose" for full parameters.')
    parser.add_argument('-di', '--delete_search_id', type=str,
                        help='Delete the passed ID')
    parser.add_argument('-dn', '--delete_search_name', type=str,
                        help='Delete the passed search name.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Set logging to debug. This will print full search parameters.')
    parser.add_argument('-dr', '--dryrun', action='store_true',
                        help='Run commands without executing.')

    args = parser.parse_args()

    seraches_list = args.searches_list
    searches_json = args.searches_json
    delete_search_id = args.delete_search_id
    delete_search_name = args.delete_search_name
    verbose = args.verbose
    dryrun = args.dryrun

    if verbose:
        log_lvl = 'DEBUG'
    else:
        log_lvl = 'INFO'
    logger = create_logger(__name__, 'sh', log_lvl)

    s = requests.Session()
    s.auth = (os.getenv('PL_API_KEY'), '')

    if seraches_list:
        list_searches(session=s, verbose=verbose)
    if searches_json:
        write_searches(session=s, out_json=searches_json)
    if delete_search_name:
        logger.info('Deleting search by name: {}'.format(delete_search_name))
        delete_saved_search(session=s, search_name=args.delete_search_name, dryrun=dryrun)
    if delete_search_id:
        logger.info('Deleting search by ID: {}'.format(delete_search_id))
        delete_saved_search(session=s, search_id=args.delete_search_id, dryrun=dryrun)
