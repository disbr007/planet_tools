import argparse
import datetime
import os
import sys

import pandas as pd
from tqdm import tqdm

from db_utils import Postgres, generate_sql
# import db_utils.insert_new_records as insert_new_records
from logging_utils.logging_utils import create_logger


# export_dir = r'V:\pgc\data\scratch\jeff\projects\planet\scenes\paris_export\pl_fp_2019oct01_2020jun30\nasa_csv_PE-33905'
# onhand_scenes_only = True  # Load only for scenes that are on hand
# new_only = True  # Load only new records
# dryrun = False
# logfile = r'C:\temp\ingest_stereo.log'
# ext = '.csv'

logger = create_logger(__name__, 'sh', 'INFO')

planet_db = 'sandwich-pool.planet'
off_nadir_tbl = 'off_nadir'
off_nadir_tbl_id = 'scene_name'
scenes_tbl = 'scenes'
scenes_tbl_id = 'id'


def ingest_off_nadir(export_dir, onhand_scenes_only=True, new_only=True, ext='.csv', dryrun=False):
    logger.info("Reading off-nadir csvs...")

    with Postgres(planet_db) as db:
        if off_nadir_tbl in db.list_db_tables():
            tbl_exists = True
        else:
            tbl_exists = False
            logger.info('Table "{}" not found. Will be created.'.format(off_nadir_tbl))

        if tbl_exists:
            logger.info('Starting count for {}.{}: {:,}'.format(planet_db, off_nadir_tbl,
                                                                db.get_table_count(off_nadir_tbl)))
        all_dfs = []
        csvs = [os.path.join(export_dir, f) for f in os.listdir(export_dir)
                if f.endswith(ext)]
        logger.info('Reading {} files in {}'.format(ext, export_dir))
        pbar = tqdm(csvs, desc='Reading files')
        for csv in pbar:
            csv_df = pd.read_csv(csv)
            # Columns are quoted... remove
            csv_df.rename(columns={c: c.replace('"', '') for c in csv_df.columns}, inplace=True)
            csv_df.rename(columns={c: c.replace(".", '_') for c in csv_df.columns}, inplace=True)
            all_dfs.append(csv_df)
            # records_to_add = pd.concat([records_to_add, csv_df])
            pbar.set_description('Reading files')

        logger.debug('Merging records from each file, removing duplicates...')
        records_to_add = pd.concat(all_dfs)
        records_to_add.drop_duplicates(subset=off_nadir_tbl_id, inplace=True)
        logger.info('Records found: {:,}'.format(len(records_to_add)))
        if onhand_scenes_only:
            logger.info('Locating records where scene is on hand...')
            sql = generate_sql(layer=scenes_tbl, columns=[scenes_tbl_id])
            onhand_scenes = set(db.sql2df(sql=sql, columns=scenes_tbl_id)[scenes_tbl_id])
            records_to_add = records_to_add[records_to_add[off_nadir_tbl_id].isin(onhand_scenes)]
            logger.info('Remaining records to add: {:,}'.format(len(records_to_add)))
        if tbl_exists and new_only:
            logger.info('Locating new records...')
            sql = generate_sql(layer=off_nadir_tbl, columns=[off_nadir_tbl_id])
            existing_off_nadirs = set(db.sql2df(sql=sql, columns=[off_nadir_tbl_id])[off_nadir_tbl_id])
            records_to_add = records_to_add[~records_to_add[off_nadir_tbl_id].isin(existing_off_nadirs)]
            logger.info('Remaining records to add: {:,}'.format(len(records_to_add)))

        if len(records_to_add) == 0:
            logger.debug('No records to add.')
            sys.exit()

        # Add records to table
        logger.info('Adding new records...')
        db.insert_new_records(records_to_add, off_nadir_tbl, unique_id=off_nadir_tbl_id, dryrun=dryrun)
        logger.info('New records added. New table count: {:,}'.format(db.get_table_count(off_nadir_tbl)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=""""Ingest Planet off-nadir exports from a given 
    directory. By default, this routine will only add those records that have a corresponding id 
    in the "scenes" table.""")

    parser.add_argument('-i', '--input_directory', type=os.path.abspath,
                        help='Path to directory holding csvs exported by Planet.')
    parser.add_argument('-noh', '--not_on_hand', action='store_true',
                        help='Use to load all new records found, not just those with '
                             'a corresponding scene in "scene" table.')
    parser.add_argument('-all', '--all_records', action='store_true',
                        help='Load all off-nadir files found in directory, not just new.')
    parser.add_argument('-ext', '--extension', type=str, nargs='+', default=['.csv'],
                        help='Extension(s) of off-nadir files to load.')
    parser.add_argument('--logfile', type=os.path.abspath)

    args = parser.parse_args()

    export_dir = args.input_directory
    onhand_scenes_only = not args.not_on_hand
    new_only = not args.all_records
    ext = tuple(args.extension)
    logfile = args.logfile

    if not logfile:
        now = datetime.datetime.now().strftime('%Y%b%d_%H%m%S')
        logfile = os.path.join(r'V:\pgc\data\scratch\jeff\projects\planet\logs',
                               '{}{}.log'.format(os.path.splitext(os.path.basename(__file__))[0], now))
    logger = create_logger(__name__, 'fh', 'DEBUG', filename=logfile)

    ingest_off_nadir(export_dir=export_dir, onhand_scenes_only=onhand_scenes_only, new_only=new_only, ext=ext)
