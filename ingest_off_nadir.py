import argparse
import numpy as np
import os
import sys

import pandas as pd
from tqdm import tqdm

from lib.db import Postgres, generate_sql
from lib.lib import get_config
from lib.logging_utils import create_logger, create_logfile_path


logger = create_logger(__name__, 'sh', 'INFO')

db_config = get_config('db')
# planet_db = 'sandwich-pool.planet'
planet_db = db_config['db_config']['host']
off_nadir_tbl = 'off_nadir'
# off_nadir_tbl_id = 'scene_name'
off_nadir_tbl_id = db_config['tables'][off_nadir_tbl]['unique_id'][0]
scenes_tbl = 'scenes2index'
# scenes_tbl_id = 'id'
scenes_tbl_id = db_config['tables'][scenes_tbl]['unique_id']

# Existing field names in csvs
off_nadir = 'sat.off_nadir'
azimuth = 'sat.satellite_azimuth_mean'

# Created / renamed field names
signed_off_nadir = 'off_nadir_signed'
tbl_off_nadir = 'off_nadir'
tbl_azimuth = 'azimuth'


def ingest_off_nadir(export_dir, onhand_scenes_only=True, new_only=True,
                     ext='.csv',
                     dryrun=False):
    """Ingest off-nadir files to a database table"""
    logger.info("Reading off-nadir csvs...")
    with Postgres() as db:
        if off_nadir_tbl in db.list_db_tables():
            tbl_exists = True
        else:
            tbl_exists = False
            logger.info('Table "{}" not found. Will be '
                        'created.'.format(off_nadir_tbl))
        if tbl_exists:
            logger.info('Starting count for {}.{}: '
                        '{:,}'.format(planet_db, off_nadir_tbl,
                                      db.get_table_count(off_nadir_tbl)))

        if onhand_scenes_only:
            logger.info('Locating records where scene is on hand...')
            # Get all onhand footprint ids
            onhand_scenes = set(db.get_values(scenes_tbl, scenes_tbl_id[0]))
            # # Keep only records that are onhand
            # records_to_add = records_to_add[
            #     records_to_add[off_nadir_tbl_id].isin(onhand_scenes)]
            # logger.info('Remaining records to add: '
            #             '{:,}'.format(len(records_to_add)))

        all_dfs = []
        # Locate all off-nadir files
        csvs = [os.path.join(export_dir, f) for f in os.listdir(export_dir)
                if f.endswith(ext)]

        logger.info('Reading {} files in {}...'.format(ext, export_dir))
        pbar = tqdm(csvs, desc='Reading files')
        # Create and clean up a dataframe for each file
        for csv in pbar:
            csv_df = pd.read_csv(csv)
            # Rename columns to match table names
            csv_df.rename(columns={off_nadir: tbl_off_nadir,
                                   azimuth: tbl_azimuth},
                          inplace=True)
            # Column names are quoted... remove
            csv_df.rename(columns={c: c.replace('"', '')
                                   for c in csv_df.columns},
                          inplace=True)
            # Replace periods in column names with underscore
            csv_df.rename(columns={c: c.replace(".", '_')
                                   for c in csv_df.columns},
                          inplace=True)
            # Add signed_off_nadir column
            csv_df[signed_off_nadir] = np.sign(csv_df[tbl_azimuth]) * \
                                       csv_df[tbl_off_nadir]
            # Keep only relevant columns
            csv_df = csv_df[[off_nadir_tbl_id,
                             tbl_off_nadir,
                             tbl_azimuth,
                             signed_off_nadir]]
            if onhand_scenes_only:
                # Keep only records that are onhand
                csv_df = csv_df[csv_df[off_nadir_tbl_id].isin(onhand_scenes)]

            all_dfs.append(csv_df)

        logger.info('Merging records from each file, removing duplicates...')
        logger.info('Total files: {:,}'.format(len(all_dfs)))
        logger.info('Total records: {:,}'.format(sum([len(df)
                                                      for df in all_dfs])))
        records_to_add = pd.concat(all_dfs)
        records_to_add.drop_duplicates(subset=off_nadir_tbl_id, inplace=True)
        logger.info('Unique records found: {:,}'.format(len(records_to_add)))

        if tbl_exists and new_only:
            # Only add new values, do not overwrite existing
            logger.info('Locating new records...')
            sql = generate_sql(layer=off_nadir_tbl,
                               columns=off_nadir_tbl_id)
            # Get IDs all records currently in off-nadir table
            existing_off_nadirs = set(
                db.sql2df(sql_str=sql, columns=off_nadir_tbl_id)[off_nadir_tbl_id])
            # Keep only records not currently in off-nadir table
            records_to_add = records_to_add[
                ~records_to_add[off_nadir_tbl_id].isin(existing_off_nadirs)]
            logger.info('Remaining records to add: '
                        '{:,}'.format(len(records_to_add)))

        if len(records_to_add) == 0:
            logger.debug('No records to add.')
            sys.exit()

        # Add records to table
        logger.info('Adding new records...')
        db.insert_new_records(records_to_add, off_nadir_tbl, dryrun=dryrun)
        logger.info('New records added. New table count:'
                    ' {:,}'.format(db.get_table_count(off_nadir_tbl)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=""""Ingest Planet off-nadir exports from a given 
    directory. By default, this routine will only add those records that have a corresponding id 
    in the "scenes2index" table.""")

    parser.add_argument('-i', '--input_directory', type=os.path.abspath,
                        help='Path to directory holding csvs exported by '
                             'Planet.')
    parser.add_argument('-noh', '--not_on_hand', action='store_true',
                        help='Use to load all new records found, not just those'
                             ' with a corresponding scene in "scenes2index" table.')
    parser.add_argument('-all', '--all_records', action='store_true',
                        help='Load all off-nadir files found in directory, '
                             'not just those not in off-nadir table. This '
                             'will overwrite any existing records in off-nadir'
                             'table.')
    parser.add_argument('-ext', '--extension', type=str, nargs='+',
                        default=['.csv'],
                        help='Extension(s) of off-nadir files to load.')
    parser.add_argument('--logfile', type=os.path.abspath)

    # For debugging
    # sys.argv = ['ingest_off_nadir.py',
    #             # '-i', r'V:\pgc\data\scratch\jeff\projects\planet'
    #             #       r'\off_nadir_exports\paris_export'
    #             #       r'\pl_fp_2019oct01_2020jun30\nasa_csv_PE-33905',
    #             '-i', r'V:\pgc\data\scratch\jeff\projects\planet\scratch'
    #                   r'\test_off_nadirs'
    #             ]

    args = parser.parse_args()

    export_dir = args.input_directory
    onhand_scenes_only = not args.not_on_hand
    new_only = not args.all_records
    ext = tuple(args.extension)
    logfile = args.logfile

    if not logfile:
        logfile = create_logfile_path('ingest_off_nadir')
    logger = create_logger(__name__, 'fh', 'DEBUG', filename=logfile)

    ingest_off_nadir(export_dir=export_dir,
                     onhand_scenes_only=onhand_scenes_only,
                     new_only=new_only, ext=ext)
