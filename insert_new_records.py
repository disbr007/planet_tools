import argparse
import os

import fiona
import geopandas as gpd
import pandas as pd

from db_utils import Postgres
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'DEBUG')


planet_db = 'sandwich-pool.planet'
scenes_tbl = 'scenes'
scenes_tbl_id = 'id'

records_path = r'V:\pgc\data\scratch\jeff\projects\planet\scenes\PSScene4Band_2019oct_2020jun.geojson'
date_cols = ['acquired']
dryrun = False


def read_records(records_path):
    try:
        records = gpd.read_file(records_path)
    except fiona.errors.DriverError:
        try:
            records = pd.read_table(records)
        except Exception as e:
            logger.error('Error reading in file: {}'.format(records_path))

    return records


def insert_new_records(records, database, table, unique_id,
                       date_cols=['acquired'], date_format='%Y-%m-%dT%H:%M:%S',
                       dryrun=dryrun):

    with Postgres(database) as db:
        db.insert_new_records(records=records, table=table, unique_id=uid,
                              date_cols=date_cols,
                              date_format=date_format,
                              dryrun=dryrun)


def main(records_path, database, table, uid, date_cols, date_format, dryrun):
    records = read_records(records_path)
    insert_new_records(records, dst_db=database, dst_tbl=table, tbl_id=uid,
                       date_cols=date_cols, date_format=date_format)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-r', '--records', type=os.path.abspath, required=True,
                        help='Path to vector file containing records to add.')
    parser.add_argument('-d', '--database', type=str, default='sandwich-pool.planet', required=True,
                        help='Database containing table to insert records into.')
    parser.add_argument('-t', '--table', type=str, required=True,
                        help='Name of table to insert records into.')
    parser.add_argument('-id', type=str, required=True,
                        help='Unique identifier in table and records to use to identify duplicates.')
    parser.add_argument('--date_columns', type=str, nargs='+', default=['acquired'],
                        help='Names of columns to convert to datetimes.')
    parser.add_argument('--date_format', type=str, default='%Y-%m-%dT%H:%M:%S',
                        help='Datetime.datetime format of date_columns.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing changes to table.')

    args = parser.parse_args()

    records_path = args.records
    database = args.database
    table = args.table
    uid = args.id
    date_cols = args.date_columns
    date_format = args.date_format
    dryrun = args.dryrun

    main(records_path=records_path, database=database, table=table, uid=uid,
         date_cols=date_cols, date_format=date_format, dryrun=dryrun)