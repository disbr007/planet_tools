import argparse
import os

from tqdm import tqdm
import pandas as pd
import numpy as np

from lib import write_gdf
from db_utils import Postgres
from logging_utils.logging_utils import create_logger
import matplotlib.pyplot as plt
from index_utils import parse_xml

logger = create_logger(__name__, 'sh', 'DEBUG')


def select_xtrack(where, out_ids, out_footprint):
    sql = """"SELECT * FROM {} WHERE {}""".format(where)

    with Postgres('sandwich-pool.planet') as db:
        gdf = db.sql2gdf(sql=sql, geom_col='ovlp_geom')

    # Write footprint
    if out_footprint:
        # Convert datetime columns to str
        date_cols = gdf.select_dtypes(include=['datetime64']).columns
        for dc in date_cols:
            gdf[dc] = gdf[dc].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

        write_gdf(gdf, out_footprint)

    # Write IDs
    if out_ids:
        all_sids = list(gdf['id1']) + list(gdf['id2'])
        logger.info('Total IDs before removing duplicates: {}'.format(len(all_sids)))

        all_sids = set(all_sids)
        logger.info('Unique scene ids: {}'.format(len(all_sids)))

        with open(out_ids, 'w') as dst:
            for sid in all_sids:
                dst.write(sid)
                dst.write('\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--where', type=str, help='The SQL WHERE clause to apply to the stereo candidates table.')
    parser.add_argument('--out_ids', type=os.path.abspath,
                        help='Path to write text file of IDs to.')
    parser.add_argument('--out_footprint', type=os.path.abspath,
                        help='Path to write footprint to.')

    args = parser.parse_args()

    where = args.where
    out_ids = args.out_ids
    out_footprint = args.out_footprint

    select_xtrack(where=where, out_ids=out_ids, out_footprint=out_footprint)
