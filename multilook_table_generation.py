import numpy as np

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

from lib.db import Postgres
from lib.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')


# Args
pairs_p = r'V:\pgc\data\scratch\jeff\projects\planet\deliveries' \
          r'\2020oct14_multilook\2020oct14_multilook_pairs.geojson'

# Constants
pairname_col = 'pairname' # in src
scenes_tbl = 'scenes_metadata'
on_col = 'off_nadir_signed'


logger.info('Loading source pairs...')
src_pairs = gpd.read_file(pairs_p)
logger.info('Pairs loaded: {}'.format(len(src_pairs)))

logger.info('Looking up metadata for pairs...')
pairs_off_nadirs = pd.DataFrame()
with Postgres('sandwich-pool.planet') as db_src:
    for i, row in tqdm(src_pairs.iterrows(), total=len(src_pairs)):
        pairnames = row[pairname_col].split('-')
        sql_query = 'SELECT * FROM {} ' \
                    'WHERE id IN ({})'.format(scenes_tbl, str(pairnames)[1:-1])
        pairs = db_src.sql2df(sql_query)
        if len(pairs) != len(pairnames):
            logger.warning('Metadata for some pairs not found.')
        pairs = pairs.set_index('id').drop(row['id'])[[on_col]].transpose()
        # TODO: remove this after removing DUPs from multilook pairnames
        # Drop duplicate columns
        pairs = pairs.loc[:, ~pairs.columns.duplicated()]
        pairs.index = [row['id']]
        pairs = pairs.rename(columns={col: 'pair{}_off_nadir_signed'.format(i+1)
                                      for i, col in enumerate(pairs.columns)},)
        if len(pairs.columns) != len(pairs_off_nadirs.columns):
            if len(pairs.columns) > len(pairs_off_nadirs.columns):
                # def add_cols(df_w_more, df_w_less):
                #     for col in df_w_more.columns:
                #         if col not in df_w_less.columns:
                #             df_w_less[col] = np.NaN

                for col in pairs.columns:
                    if col not in pairs_off_nadirs.columns:
                        pairs_off_nadirs[col] = np.NaN
            if len(pairs_off_nadirs.columns) > len(pairs.columns):
                for col in pairs_off_nadirs.columns:
                    if col not in pairs.columns:
                        pairs[col] = np.NaN
        pairs_off_nadirs = pd.concat([pairs, pairs_off_nadirs])
