import argparse
import os

import geopandas as gpd

from db_utils import Postgres
from lib import determine_driver
from logging_utils.logging_utils import create_logger


logger = create_logger(__name__, 'sh', 'INFO')


def ingest_footprints(footprints_path, tbl='scenes', dryrun=False):
    logger.info('Reading footprints...')
    driver = determine_driver(footprints_path)
    footprints = gpd.read_file(footprints_path, driver=driver)
    logger.info('Footprints loaded: {}'.format(len(footprints)))

    if

    logger.info('Inserting footprints into {}'.format(tbl))
    with Postgres('sandwich-pool.planet') as db_src:
        logger.info('Starting count for {}: '
                    '{}'.format(tbl, db_src.get_table_count(tbl)))
        db_src.insert_new_records(footprints, table=tbl,
                                  unique_on=('id', 'item_type'),
                                  date_cols=['acquired'],
                                  dryrun=dryrun)
        logger.info('New count for {}: {}'.format(tbl,
                                                  db_src.get_table_count(tbl)))
    logger.info('Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input_footprints', type=os.path.abspath,
                        help='Path to footprints to upload.')
    parser.add_argument('-t', '--to_table', type=str,
                        help='Table on sandwich-pool.planet to upload to.')
    parser.add_argument('-d', '--dryrun', action='store_true')
    
    args = parser.parse_args()

    footprints_path = args.input_footprints
    tbl = args.to_table
    dryrun = args.dryrun

    ingest_footprints(footprints_path=footprints_path, tbl=tbl, dryrun=dryrun)
