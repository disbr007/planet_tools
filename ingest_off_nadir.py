import argparse
import numpy as np
import os
from pathlib import Path
import sys

import pandas as pd
from tqdm import tqdm

# from lib.db import Postgres, generate_sql
from lib.lib import get_config
from lib.logging_utils import create_logger, create_logfile_path
import lib.constants as constants

logger = create_logger(__name__, 'sh', 'INFO')

# External modules
sys.path.append(str(Path(__file__).parent / '..'))
try:
    from db_utils.db import Postgres, generate_sql
except ImportError as e:
    logger.error('db_utils module not found. It should be adjacent to '
                 'the planet_tools directory. Path: \n{}'.format(sys.path))
    sys.exit()


def ingest_off_nadir(export_dir, onhand_scenes_only=True, new_only=True,
                     ext='.csv',
                     dryrun=False):
    """Ingest off-nadir files to a database table"""
    logger.info("Reading off-nadir csvs...")
    # TODO: update this to use db_utils syntax
    with Postgres(host=constants.SANDWICH, database=constants.PLANET) as db:
        if constants.OFF_NADIR in db.list_db_tables():
            tbl_exists = True
        else:
            tbl_exists = False
            logger.info('Table "{}" not found. Will be '
                        'created.'.format(constants.OFF_NADIR))
        if tbl_exists:
            logger.info('Starting count for {}.{}: '
                        '{:,}'.format(constants.SANDWICH_POOL_PLANET, constants.OFF_NADIR,
                                      db.get_table_count(constants.OFF_NADIR)))

        if onhand_scenes_only:
            logger.info('Locating records where scene is on hand...')
            # Get all onhand footprint ids
            onhand_scenes = set(db.get_values(constants.SCENES, constants.ID))
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
            csv_df.rename(columns={constants.SAT_OFF_NADIR: constants.OFF_NADIR_FLD,
                                   constants.SAT_AZIMUTH: constants.AZIMUTH},
                          inplace=True)
            # Column names are quoted... remove
            csv_df.rename(columns={c: c.replace('"', '')
                                   for c in csv_df.columns},
                          inplace=True)
            # Replace periods in column names with underscore
            csv_df.rename(columns={c: c.replace(".", '_')
                                   for c in csv_df.columns},
                          inplace=True)
            # Add off_nadir_signed column
            csv_df[constants.OFF_NADIR_SIGNED] = np.sign(csv_df[constants.AZIMUTH]) * \
                                       csv_df[constants.OFF_NADIR_FLD]
            # Keep only relevant columns
            csv_df = csv_df[[constants.SCENE_NAME,
                             constants.OFF_NADIR_FLD,
                             constants.AZIMUTH,
                             constants.OFF_NADIR_SIGNED]]
            if onhand_scenes_only:
                # Keep only records that are onhand
                csv_df = csv_df[csv_df[constants.SCENE_NAME].isin(onhand_scenes)]

            all_dfs.append(csv_df)

        logger.info('Merging records from each file, removing duplicates...')
        logger.info('Total files: {:,}'.format(len(all_dfs)))
        logger.info('Total records: {:,}'.format(sum([len(df)
                                                      for df in all_dfs])))
        records_to_add = pd.concat(all_dfs)
        records_to_add.drop_duplicates(subset=constants.SCENE_NAME, inplace=True)
        logger.info('Unique records found: {:,}'.format(len(records_to_add)))

        if tbl_exists and new_only:
            # Only add new values, do not overwrite existing
            logger.info('Locating new records...')
            sql = generate_sql(layer=constants.OFF_NADIR,
                               columns=constants.SCENE_NAME)
            # Get IDs all records currently in off-nadir table
            existing_off_nadirs = set(
                db.sql2df(sql_str=sql, columns=constants.SCENE_NAME)[constants.SCENE_NAME])
            # Keep only records not currently in off-nadir table
            records_to_add = records_to_add[
                ~records_to_add[constants.SCENE_NAME].isin(existing_off_nadirs)]
            logger.info('Remaining records to add: '
                        '{:,}'.format(len(records_to_add)))

        if len(records_to_add) == 0:
            logger.debug('No records to add.')
            sys.exit()

        # Add records to table
        logger.info('Adding new records...')
        db.insert_new_records(records_to_add, constants.OFF_NADIR, dryrun=dryrun)
        logger.info('New records added. New table count:'
                    ' {:,}'.format(db.get_table_count(constants.OFF_NADIR)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=""""Ingest Planet off-nadir exports from a given 
    directory. By default, this routine will only add those records that have a corresponding id 
    in the "scenes" table.""")

    parser.add_argument('-i', '--input_directory', type=os.path.abspath,
                        help='Path to directory holding csvs exported by '
                             'Planet.')
    parser.add_argument('-noh', '--not_on_hand', action='store_true',
                        help='Use to load all new records found, not just those'
                             ' with a corresponding scene in "scenes" table.')
    parser.add_argument('-all', '--all_records', action='store_true',
                        help='Load all off-nadir files found in directory, '
                             'not just those not in off-nadir table. This '
                             'will overwrite any existing records in off-nadir'
                             'table.')
    parser.add_argument('-ext', '--extension', type=str, nargs='+',
                        default=['.csv'],
                        help='Extension(s) of off-nadir files to load.')
    parser.add_argument('--logfile', type=os.path.abspath)

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
