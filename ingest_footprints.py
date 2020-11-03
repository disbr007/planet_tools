import argparse
import os
import sys

import geopandas as gpd

from db_utils import Postgres
from lib import determine_driver
from logging_utils.logging_utils import create_logger


logger = create_logger(__name__, 'sh', 'INFO')


def ingest_footprints(footprints_path, tbl=None, dryrun=False):
    logger.info('Reading footprints: {}'.format(footprints_path))
    driver = determine_driver(footprints_path)
    footprints = gpd.read_file(footprints_path, driver=driver)
    logger.info('Footprints loaded: {:,}'.format(len(footprints)))

    logger.info('Inserting footprints into {}'.format(tbl))
    with Postgres('sandwich-pool.planet') as db_src:
        logger.info('Starting count for {}: '
                    '{:,}'.format(tbl, db_src.get_table_count(tbl)))

        # Check that fields in footprints are in target table
        logger.info('Verifying fields match...')
        src_fields = list(footprints)
        tbl_fields = db_src.get_layer_columns(tbl)
        if not all([f in tbl_fields for f in src_fields]):
            logger.error('Field mismatch between footprints and {}:\n'
                         'Footprints:\n{}\n{}\n{}'.format(tbl,
                                                          sorted(src_fields),
                                                          tbl,
                                                          sorted(tbl_fields)))
            logger.error('Skipping {}'.format(footprints_path))
            return

        db_src.insert_new_records(footprints, table=tbl,
                                  unique_on=('id', 'item_type'),
                                  geom_cols=[footprints.geometry.name],
                                  dryrun=dryrun)
        logger.info('New count for {}: '
                    '{:,}'.format(tbl, db_src.get_table_count(tbl)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input_footprints', type=os.path.abspath,
                        required=True,
                        nargs='+',
                        help='Path to footprints to upload.')
    parser.add_argument('-t', '--to_table', type=str,
                        required=True,
                        help='Table on sandwich-pool.planet to upload to.')
    parser.add_argument('-d', '--dryrun', action='store_true')
    
    args = parser.parse_args()

    footprints_paths = args.input_footprints
    tbl = args.to_table
    dryrun = args.dryrun

    for fp in footprints_paths:
        ingest_footprints(footprints_path=fp, tbl=tbl, dryrun=dryrun)
    logger.info('Done.')
