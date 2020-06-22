from pprint import pprint
import argparse
import os
import json

import geopandas as gpd

from manage_searches import split_filter_arg, create_search_request, create_saved_search, get_search_count
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-n', '--name', type=str, help='Name of search to create')
    parser.add_argument('-it', '--item_types', nargs='*',
                        help='Item types to search. E.g.: PSScene3Band, PSScene4Band')
    parser.add_argument('-f', '--filters', action='append', nargs='*',
                        # metavar=('filter_type', 'field_name', 'config'),
                        help="Help.")
    parser.add_argument('-lf', '--load_filter', type=os.path.abspath,
                        help='Base filter to load, upon which nay provided filters will be added.')
    parser.add_argument('--get_count', action='store_true',
                        help="Pass to get total count for the newly created saved search.")
    parser.add_argument('--overwrite_saved', action='store_true',
                        help='Pass to overwrite a saved search of the same name.')
    parser.add_argument('-d', '--dryrun', action='store_true',
                        help='Do not actually create the saved search.')

    args = parser.parse_args()

    name = args.name
    item_types = args.item_types
    filters = args.filters
    load_filter = args.load_filter
    get_count = args.get_count
    overwrite_saved = args.overwrite_saved
    dryrun = args.dryrun

    search_filters = [split_filter_arg(f) for f in filters]
    if load_filter:
        addtl_filter = json.load(open(load_filter))
        search_filters.append(addtl_filter)

    sr = create_search_request(name=name, item_types=item_types, search_filters=search_filters)
    if get_count:
        total_count = get_search_count(sr)
        logger.info('Count for new search: {:,}'.format(total_count))

    if not dryrun:
        ss_id = create_saved_search(search_request=sr, overwrite_saved=overwrite_saved)
        if ss_id:
            logger.info('Successfilly created new search. Search ID: {}'.format(ss_id))

