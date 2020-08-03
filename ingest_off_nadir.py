import os
import sys

import pandas as pd
from tqdm import tqdm

from db_utils import Postgres, generate_sql
# import db_utils.insert_new_records as insert_new_records
from logging_utils.logging_utils import create_logger


export_dir = r'V:\pgc\data\scratch\jeff\projects\planet\scenes\paris_export\pl_fp_2019oct01_2020jun30\nasa_csv_PE-33905'
onhand_scenes_only = True  # Load only for scenes that are on hand
new_only = True  # Load only new records
dryrun = False
logfile = r'C:\temp\ingest_stereo.log'
ext = '.csv'


logger = create_logger(__name__, 'sh', 'DEBUG')
# logger = create_logger(__name__, 'fh', 'DEBUG',
#                        filename=logfile)

planet_db = 'sandwich-pool.planet'
off_nadir_tbl = 'off_nadir'
off_nadir_tbl_id = 'scene_name'
scenes_tbl = 'scenes'
scenes_tbl_id = 'id'

logger.info("Reading off-nadir csvs...")


# Create dataframe of records to add
with Postgres(planet_db) as db:
    logger.info('Starting count for {}.{}: {:,}'.format(planet_db, off_nadir_tbl,
                                                      db.get_table_count(off_nadir_tbl)))
    all_dfs = []

    csvs = [os.path.join(export_dir, f) for f in os.listdir(export_dir)
            if f.endswith(ext)]
    remaining_csvs = len(csvs)
    pbar = tqdm(csvs, desc='Remaining CSVs: {:,}'.format(remaining_csvs))
    for csv in pbar:
        csv_df = pd.read_csv(csv)
        remaining_csvs -= 1
        all_dfs.append(csv_df)
        # records_to_add = pd.concat([records_to_add, csv_df])
        pbar.set_description('Remaining CSVs: {:,}'.format(remaining_csvs))

    logger.ifo('')
    records_to_add = pd.concat(all_dfs)
    logger.info('Records found: {:,}'.format(len(records_to_add)))
    if onhand_scenes_only:
        logger.info('Locating records where scene is on hand...')
        sql = generate_sql(layer=scenes_tbl, columns=[scenes_tbl_id])
        onhand_scenes = set(db.sql2df(sql=sql, columns=scenes_tbl_id)[scenes_tbl_id])
        records_to_add = records_to_add[records_to_add[off_nadir_tbl_id].isin(onhand_scenes)]
        logger.info('Remaining records to add: {:,}'.format(len(records_to_add)))
    if new_only:
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
