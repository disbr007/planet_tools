import argparse
import os

import geopandas as gpd

from lib import write_gdf
from db_utils import Postgres, intersect_aoi_where
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

stereo_candidates_tbl = 'stereo_candidates'
stereo_candidates_tbl_oh = 'stereo_candidates_onhand'
scene_tbl = 'scenes'
scene_tbl_oh = 'scenes_onhand'
sid_col = 'id'
id1_col = 'id1'
id2_col = 'id2'
geom_col = 'ovlp_geom'


def select_xtrack(aoi_path=None, where=None, out_ids=None,
                  out_pairs_footprint=None, out_scene_footprint=None,
                  out_pairs_csv=None,
                  onhand=True):
    if onhand:
        stereo_tbl = stereo_candidates_tbl_oh
        scenes = scene_tbl_oh
    else:
        stereo_tbl = stereo_candidates_tbl
        scenes = scene_tbl

    if not where:
        where = ''

    if aoi_path:
        aoi = gpd.read_file(aoi_path)
        aoi_where = intersect_aoi_where(aoi, geom_col=geom_col)
        if where:
            where += " AND ({})".format(aoi_where)
        else:
            where = aoi_where

    sql = """SELECT * FROM {} WHERE {}""".format(stereo_tbl, where)
    logger.debug('SQL for stereo selection:\n{}'.format(sql))

    with Postgres('sandwich-pool.planet') as db:
        gdf = db.sql2gdf(sql=sql, geom_col='ovlp_geom')

    logger.info('Pairs found: {:,}'.format(len(gdf)))

    # Write footprint of pairs
    if out_pairs_footprint:
        # Convert datetime columns to str
        date_cols = gdf.select_dtypes(include=['datetime64']).columns
        for dc in date_cols:
            gdf[dc] = gdf[dc].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

        write_gdf(gdf, out_pairs_footprint)

    # Write footprint of individual scenes
    if out_scene_footprint:
        all_sids = list(gdf[id1_col]) + list(gdf[id2_col])
        # logger.info('Total IDs before removing duplicates: {:,}'.format(len(all_sids)))
        all_sids = list(set(all_sids))
        logger.info('Unique scene ids: {:,}'.format(len(all_sids)))

        sql = """SELECT * FROM {} WHERE {} IN ({})""".format(scenes, sid_col, str(all_sids)[1:-1])
        logger.debug('SQL for selecting scenes footprint:\n{}...'.format(sql[:500]))
        with Postgres('sandwich-pool.planet') as db:
            scene_footprint = db.sql2gdf(sql=sql)

        write_gdf(scene_footprint, out_scene_footprint)

    # Write IDs
    if out_ids:
        all_sids = list(gdf[id1_col] + list(gdf[id2_col]))
        logger.info('Total IDs before removing duplicates: {:,}'.format(len(all_sids)))

        all_sids = set(all_sids)
        logger.info('Unique scene ids: {:,}'.format(len(all_sids)))

        with open(out_ids, 'w') as dst:
            for sid in all_sids:
                dst.write(sid)
                dst.write('\n')

    if out_pairs_csv:
        logger.info('Writing pairs to CSV: {}'.format(out_pairs_csv))
        gdf.drop(columns=gdf.geometry.name).to_csv(out_pairs_csv)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--aoi', type=os.path.abspath,
                        help='AOI within which to select stereo pairs.')
    parser.add_argument('-w', '--where', type=str, help='The SQL WHERE clause to apply to the stereo candidates table.')
    parser.add_argument('--out_ids', type=os.path.abspath,
                        help='Path to write text file of IDs to.')
    parser.add_argument('--out_pairs_footprint', type=os.path.abspath,
                        help='Path to write footprint of pairs (intersections) to.')
    parser.add_argument('--out_pairs_csv', type=os.path.abspath,
                        help='Path to write csv of pairs to.')
    parser.add_argument('--out_scene_footprint', type=os.path.abspath,
                        help='Path to write footprint of scenes to.')
    parser.add_argument('-all', action='store_true',
                        help='Select all pairs meeting where clause, not just on hand.')

    args = parser.parse_args()

    aoi = args.aoi
    where = args.where
    out_ids = args.out_ids
    out_pairs_footprint = args.out_pairs_footprint
    out_scene_footprint = args.out_scene_footprint
    out_pairs_csv = args.out_pairs_csv
    onhand = not args.all

    select_xtrack(aoi_path=aoi, where=where, out_ids=out_ids,
                  out_pairs_footprint=out_pairs_footprint,  out_scene_footprint=out_scene_footprint,
                  out_pairs_csv=out_pairs_csv,
                  onhand=onhand)
