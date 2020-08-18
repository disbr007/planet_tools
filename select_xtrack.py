import argparse
import os

from lib import write_gdf
from db_utils import Postgres
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'DEBUG')

stereo_candidates_tbl = 'stereo_candidates'
stereo_candidates_tbl_oh = 'stereo_candidates_oh'


def select_xtrack(where, out_ids=None, out_footprint=None, onhand=True):
    if onhand:
        stereo_tbl = stereo_candidates_tbl_oh
    else:
        stereo_tbl = stereo_candidates_tbl

    sql = """"SELECT * FROM {} WHERE {}""".format(stereo_tbl, where)

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
    parser.add_argument('-all', action='store_true',
                        help='Select all pairs meeting where clause, not just on hand.')

    args = parser.parse_args()

    where = args.where
    out_ids = args.out_ids
    out_footprint = args.out_footprint
    onhand = not args.all

    select_xtrack(where=where, out_ids=out_ids, out_footprint=out_footprint, onhand=onhand)
