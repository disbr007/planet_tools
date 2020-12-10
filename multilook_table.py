import argparse
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from lib.db import Postgres
from lib.logging_utils import create_logger


logger = create_logger(__name__, 'sh', 'INFO')
#
# # Inputs
# multilook_src_tbl_p = r'V:\pgc\data\scratch\jeff\projects\planet\deliveries' \
#                       r'\2020oct14_multilook\2020oct14_multilook_pairs.geojson'
# pan_fn = True

# Constants
PER_SCENE_FLDS = ['fn_id', 'off_nadir_signed', 'azimuth', 'gsd_avg']
SOMD_VIEW = 'scenes_onhand_metadata'
FILENAME_FLD = 'filename'  # in database table
SON_FLD = 'off_nadir_signed'  # in database
AZI_FLD = 'azimuth'  # in database
GSD_FLD = 'gsd_avg'  # in database
PAIR_COUNT_FLD = 'pair_count'  # in both input and output
PAIRNAME_FLD = 'fn_pairname'  # in input table
SRC_ID_FLD = 'src_id'  # in output table
GEOMETRY_FLD = 'geometry'  # in both input and output
GSD_OUT_FLD = 'gsd'  # in output
UL_FLD = 'upper_left'  # in output
LR_FLD = 'lower_right'  # in output
PAN_FN_FLD = 'pan_filename'  # in output
PAN_SFX = '_pan'  # in output

add_fields = [SON_FLD, AZI_FLD, GSD_OUT_FLD]


def multilook_table(input_table_path, add_pan_fn):
    logger.info('Loading input table...')
    input_table = gpd.read_file(input_table_path)
    logger.info('Pairs located: {}'.format(len(input_table)))

    # Get all filenames present in all pairnames
    pair_filenames = set(list(input_table[PAIRNAME_FLD]))
    all_pairs = list(input_table[PAIRNAME_FLD].apply(lambda x: x.split('-')))
    filenames = {fn for pairs in all_pairs for fn in pairs}
    logger.info('Unique scenes: {}'.format(len(filenames)))

    # Load all filenames from stereo_onhand table to get metadata
    logger.info('Loading metadata for scenes...')
    sql_statement = "" \
    "SELECT id, " \
    "LEFT(filename, LENGTH(filename)-4) as filename, " \
    "off_nadir_signed, azimuth, gsd_avg FROM {} " \
    "WHERE LEFT(filename, LENGTH(filename)-4) " \
                    "IN ({}) ".format(SOMD_VIEW, str(filenames)[1:-1])

    with Postgres() as db_src:
        records = db_src.sql2df(sql_str=sql_statement)
        records.set_index(FILENAME_FLD, inplace=True)
        records.rename(columns={GSD_FLD: GSD_OUT_FLD}, inplace=True)
    logger.info('Metadata records found: {:,}'.format(len(records)))

    # Build new table
    logger.info('Creating output table...')
    out_table = pd.DataFrame()
    for i, row in tqdm(input_table.iterrows(), total=len(input_table)):
        row_pairs = row[PAIRNAME_FLD].split('-')
        minx, miny, maxx, maxy = row['geometry'].bounds
        new_row = {SRC_ID_FLD: row_pairs[0],
                   PAIR_COUNT_FLD: row[PAIR_COUNT_FLD],
                   UL_FLD: (minx, maxy),
                   LR_FLD: (maxx, miny)
                   }
        for j, pair in enumerate(row_pairs):
            new_row['{}{}'.format(FILENAME_FLD, j+1)] = pair
            for field in add_fields:
                new_row['{}{}'.format(field, j+1)] = records.at[pair, field]
            if add_pan_fn:
                new_row[PAN_FN_FLD] = '{}{}{}'.format(row_pairs[0], PAN_SFX, j)
        out_table = pd.concat([out_table, pd.DataFrame(new_row)])

    logger.info('Done.')

    return out_table


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Create a table with columns for each '
                                     'scene of each pair in a multilook pairs '
                                     'table.')
    parser.add_argument('-i', '--input_table', type=os.path.abspath,
                        help='Path to multilook input table. Must contain the '
                             'following fields: \n'
                             'fn_pairname: '
                             'hypen seperated filenames without extensions '
                             'pair_count: number of pairs in each record.')
    parser.add_argument('-o', '--output_table', type=os.path.abspath,
                        help='Path to write new table to.')
    parser.add_argument('--add_pan_name', action='store_true',
                        help='Use to also included "filename_pan" fields.')

    args = parser.parse_args()

    input_table = args.input_table
    output_table = args.output_table
    add_pan_fn = args.add_pan_name

    if not output_table:
        itp = Path(input_table)
        output_table = itp.parent / '{}_metadata.csv'.format(itp.stem)

    result = multilook_table(input_table, add_pan_fn)

    logger.info('Writing to: {}'.format(output_table))
    result.to_csv(output_table)
