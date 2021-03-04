import argparse
import os

import geopandas as gpd
from sqlalchemy.exc import ProgrammingError

from lib.db import Postgres, intersect_aoi_where
from lib.lib import read_ids, write_gdf, parse_group_args
# TODO: Fix this - place attrib_arg_lut dict somewhere better
from lib.search import attrib_arg_lut
from lib.logging_utils import create_logger

# logger = create_logger('lib', 'sh', 'INFO')
# Database Tables
scenes_tbl = 'scenes2index'
scenes_onhand_tbl = 'scenes_onhand'

# Databse Fields
GEOMETRY_FLD = 'geometry'


def build_argument_sql(att_args=None, months=None,
                       month_min_days=None, month_max_days=None,
                       aoi_path=None, ids=None, ids_field='id',
                       table=scenes_tbl):
    """Build SQL clause from supplied attribute arguements and AOI path
    att_args: tuple (attribute, value)
    aoi_path: os.path.abspath
    """
    where_statements = []
    if att_args:
        # Build attribute sql
        for arg, value in att_args:
            field_name = attrib_arg_lut[arg]['field_name']
            compare = attrib_arg_lut[arg]['op_symbol']
            if isinstance(value, str):
                arg_where = "{} {} '{}'".format(field_name, compare, value)
            elif isinstance(value, list):
                arg_where = "{} IN ({})".format(field_name, str(value)[1:-1])
            else:
                arg_where = "{} {} {}".format(field_name, compare, value)

            where_statements.append(arg_where)

    if aoi_path:
        # Build AOI sql
        aoi = gpd.read_file(aoi_path)
        aoi_sql = intersect_aoi_where(aoi, GEOMETRY_FLD)
        where_statements.append(aoi_sql)

    where = ""
    for i, clause in enumerate(where_statements):
        where += '({})'.format(clause)
        if i != len(where_statements) - 1:
            where += ' AND '

    if months:
        month_wheres = []
        for month in months:
            month_where = """EXTRACT(MONTH FROM acquired) = {}""".format(month)
            if month_min_days and month in month_min_days.keys():
                min_day_where = """EXTRACT(DAY FROM acquired) >= {}""".format(month_min_days[month])
                month_where += """ AND {}""".format(min_day_where)
            if month_max_days and month in month_max_days.keys():
                max_day_where = """EXTRACT(DAY FROM acquired) <= {}""".format(month_max_days[month])
                month_where += """ AND {}""".format(max_day_where)
            month_where = '({})'.format(month_where)

            month_wheres.append(month_where)

        months_clause = "({})".format(" OR ".join(month_wheres))
        if where:
            where += " AND {}".format(months_clause)
        else:
            where = months_clause

    if ids:
        ids_where = '{} IN ({})'.format(ids_field, str(ids)[1:-1])
        if where:
            where += " AND ({})".format(ids_where)
        else:
            where = ids_where

    sql_str = """SELECT * FROM {} WHERE {}""".format(table, where)

    logger.debug('SQL: {}'.format(sql_str[:500]))
    if len(sql_str) > 500:
        logger.debug('...{}'.format(sql_str[-500:]))

    return sql_str


def make_selection(sql_str):
    with Postgres() as db:
        try:
            selection = db.sql2gdf(sql_str=sql_str)
        except ProgrammingError as sql_error:
            logger.error(sql_error)
            logger.error('SQL: {}'.format(sql_str[:500]))
            if len(sql_str) > 500:
                logger.error('...{}'.format(sql_str[-500:]))
            raise ProgrammingError

    logger.info('Selected features: {:,}'.format(len(selection)))

    return selection


def select_scenes(att_args, aoi_path=None, ids=None, ids_field='id', months=None,
                  month_min_days=None, month_max_days=None,
                  out_selection=None, onhand=False, dryrun=False):
    if onhand:
        tbl = scenes_onhand_tbl
    else:
        tbl = scenes_tbl

    if ids:
        selection_ids = read_ids(ids, field=ids_field)
        # with open(ids, 'r') as src:
        #     selection_ids = src.readlines()
        #     selection_ids = [i.strip() for i in selection_ids]
    else:
        selection_ids = None

    sql_str = build_argument_sql(att_args=att_args, months=months, ids=selection_ids, ids_field=ids_field,
                             month_min_days=month_min_days, month_max_days=month_max_days,
                             aoi_path=aoi_path, table=tbl)
    selection = make_selection(sql_str=sql_str)

    if out_selection and not dryrun:
        logger.info('Writing selected features to: {}'.format(out_selection))
        write_gdf(selection, out_selection)

    logger.info('Done.')


def select_xtrack(aoi_path=None, where=None, out_ids=None,
                  out_pairs_footprint=None, out_scene_footprint=None,
                  out_pairs_csv=None,
                  onhand=True):
    # Constants
    stereo_candidates_tbl = 'stereo_candidates'
    stereo_candidates_tbl_oh = 'stereo_candidates_onhand'
    scene_tbl = 'scenes2index'
    scene_tbl_oh = 'scenes_onhand'
    sid_col = 'id'
    id1_col = 'id1'
    id2_col = 'id2'
    geom_col = 'ovlp_geom'

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

    sql_str = """SELECT * FROM {} WHERE {}""".format(stereo_tbl, where)
    logger.debug('SQL for stereo selection:\n{}'.format(sql_str))

    with Postgres('sandwich-pool.planet') as db:
        gdf = db.sql2gdf(sql_str=sql_str, geom_col='ovlp_geom')

    logger.info('Pairs found: {:,}'.format(len(gdf)))

    # Write footprint of pairs
    if out_pairs_footprint:
        # Convert datetime columns to str
        date_cols = gdf.select_dtypes(include=['datetime64']).columns
        for dc in date_cols:
            gdf[dc] = gdf[dc].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

        write_gdf(gdf, out_pairs_footprint)

    # Write footprint of individual scenes2index
    if out_scene_footprint:
        all_sids = list(gdf[id1_col]) + list(gdf[id2_col])
        # logger.info('Total IDs before removing duplicates: {:,}'.format(len(all_sids)))
        all_sids = list(set(all_sids))
        logger.info('Unique scene ids: {:,}'.format(len(all_sids)))

        sql_str = """SELECT * FROM {} WHERE {} IN ({})""".format(scenes, sid_col, str(all_sids)[1:-1])
        logger.debug('SQL for selecting scenes2index footprint:\n{}...'.format(sql_str[:500]))
        with Postgres() as db:
            scene_footprint = db.sql2gdf(sql_str=sql_str)

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
    # TODO: Add support for selecting xtrack using select_xtrack fxn
    # Groups
    att_group = 'Attributes'

    # Choices
    choices_instruments = ['PS2', 'PSB.SD', 'PS2.SD']
    choices_quality_category = ['standard', 'test']

    parser = argparse.ArgumentParser("Select footprints by attribute or AOI from scenes2index table "
                                     "or from scenes_onhand table.")

    attribute_args = parser.add_argument_group(att_group)

    parser.add_argument('--onhand', action='store_true',
                        help='Select from scenes_onhand table.')
    parser.add_argument('--months', type=str, nargs='+',
                        help='Month as zero-padded number, e.g. 04')
    parser.add_argument('--month_min_day', nargs=2, action='append',
                        help='Mimumum day to include in a given month: eg. 12 20'
                             'Can be repeated multiple times.')
    parser.add_argument('--month_max_day', nargs=2, action='append',
                        help='Maximum day to include in a given month: eg. 12 20'
                             'Can be repeated multiple times.')
    attribute_args.add_argument('--min_date', type=str,)
    attribute_args.add_argument('--max_date', type=str,)
    attribute_args.add_argument('--max_cc', type=float, )
    attribute_args.add_argument('--min_ulx', type=float, )
    attribute_args.add_argument('--max_ulx', type=float, )
    attribute_args.add_argument('--min_uly', type=float, )
    attribute_args.add_argument('--max_uly', type=float, )
    attribute_args.add_argument('--min_sun_azimuth', type=float)
    attribute_args.add_argument('--max_sun_azimuth', type=float)
    attribute_args.add_argument('--max_sun_elevation', type=float,)
    attribute_args.add_argument('--min_sun_elevation', type=float,)
    attribute_args.add_argument('--max_usable_data', type=float,)
    attribute_args.add_argument('--provider', type=str, nargs='+',
                                choices=['planetscope', 'rapideye'])
    attribute_args.add_argument('--satellite_id', type=str, nargs='+', )
    attribute_args.add_argument('--instrument', type=str, nargs='+',
                                choices=choices_instruments, )
    attribute_args.add_argument('--strip_id', type=str, nargs='+', )
    attribute_args.add_argument('--quality_category', type=str, nargs='+',
                                choices=choices_quality_category)
    attribute_args.add_argument('--ground_control', type=bool, nargs='+')

    parser.add_argument('--aoi', type=os.path.abspath,
                        help='Path to AOI vector file to use for selection.')
    parser.add_argument('--ids', type=os.path.abspath,
                        help='Path to text file of IDs to include.')
    parser.add_argument('--ids_field', type=str, default='id',
                        help='The name of the field in the table being searched to '
                             'locate ids.')
    parser.add_argument('-o', '--out_selection', type=os.path.abspath,
                        help='Path to write selection to')
    parser.add_argument('--dryrun', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')

    # import sys
    # sys.argv = [__file__,
    #             '--onhand']
    args = parser.parse_args()

    if args.verbose:
        log_lvl = 'DEBUG'
    else:
        log_lvl = 'INFO'

    logger = create_logger(__name__, 'sh', log_lvl)

    out_selection = args.out_selection
    onhand = args.onhand
    aoi_path = args.aoi
    ids = args.ids
    ids_field = args.ids_field
    months = args.months
    month_min_day_args = args.month_min_day
    month_max_day_args = args.month_max_day
    attribute_args = parse_group_args(parser, att_group)

    supplied_att_args = [(kwa[0], kwa[1]) for kwa in attribute_args._get_kwargs()
                         if kwa[1]]

    month_min_days = None
    month_max_days = None
    if months:
        if month_min_day_args:
            month_min_days = {month: day for month, day in month_min_day_args}
        if month_max_day_args:
            month_max_days = {month: day for month, day in month_max_day_args}

    select_scenes(att_args=supplied_att_args, months=months, aoi_path=aoi_path,
                  ids=ids, ids_field=ids_field, onhand=onhand,
                  month_min_days=month_min_days, month_max_days=month_max_days,
                  out_selection=out_selection, dryrun=args.dryrun)
