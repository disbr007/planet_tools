from pprint import pprint
import argparse
import json
import os
import requests

from search_utils import get_search_count, get_all_searches, delete_saved_search
from logging_utils.logging_utils import create_logger


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
