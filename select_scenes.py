import argparse
import os

import geopandas as gpd
from sqlalchemy.exc import ProgrammingError

from db_utils import Postgres, intersect_aoi_where
from lib import write_gdf, parse_group_args
# TODO: Fix this - place attrib_arg_lut dict somewhere better
from search_utils import attrib_arg_lut
from logging_utils.logging_utils import create_logger

# logger = create_logger('lib', 'sh', 'INFO')
scenes_tbl = 'scenes'
scenes_onhand_tbl = 'scenes_onhand'

def build_argument_sql(att_args=None, months=None,
                       month_min_days=None, month_max_days=None,
                       aoi_path=None,
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
        aoi_sql = intersect_aoi_where(aoi, 'geom')
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


    sql = """SELECT * FROM {} WHERE {}""".format(table, where)

    logger.debug('SQL: {}'.format(sql[:500]))
    if len(sql) > 500:
        logger.debug('...{}'.format(sql[-500:]))

    return sql


def make_selection(sql):
    with Postgres('sandwich-pool.planet') as db:
        try:
            selection = db.sql2gdf(sql=sql)
        except ProgrammingError as sql_error:
            logger.error('SQL: {}'.format(sql[:500]))
            if len(sql) > 500:
                logger.error('...{}'.format(sql[-500:]))
            raise ProgrammingError

    logger.info('Selected features: {:,}'.format(len(selection)))

    return selection


def select_scenes(att_args, aoi_path, months=None,
                  month_min_days=None, month_max_days=None,
                  out_selection=None, dryrun=False):
    sql = build_argument_sql(att_args=att_args, months=months,
                             month_min_days=month_min_days, month_max_days=month_max_days,
                             aoi_path=aoi_path)
    selection = make_selection(sql)

    if out_selection and not dryrun:
        write_gdf(selection, out_selection)

    logger.info('Done.')


if __name__ == '__main__':
    # Groups
    att_group = 'Attributes'

    # Choices
    choices_instruments = ['PS2', 'PSB.SD', 'PS2.SD']
    choices_quality_category = ['standard', 'test']

    parser = argparse.ArgumentParser("Select footprints by attribute or AOI from scenes table "
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

    # TODO: Add multi-aoi support - join aoi_wheres with ' OR '
    parser.add_argument('--aoi', type=os.path.abspath,
                        help='Path to AOI vector file to use for selection.')

    parser.add_argument('-o', '--out_selection', type=os.path.abspath,
                        help='Path to write selection to')
    parser.add_argument('--dryrun', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')

    args = parser.parse_args()

    if args.verbose:
        log_lvl = 'DEBUG'
    else:
        log_lvl = 'INFO'

    logger = create_logger(__name__, 'sh', log_lvl)

    out_selection = args.out_selection
    aoi_path = args.aoi
    months = args.months
    month_min_day_args = args.month_min_day
    month_max_day_args = args.month_max_day
    attribute_args = parse_group_args(parser, att_group)

    supplied_att_args = [(kwa[0], kwa[1]) for kwa in attribute_args._get_kwargs()
                         if kwa[1]]

    if months:
        month_min_days = None
        month_max_days = None
        if month_min_day_args:
            month_min_days = {month: day for month, day in month_min_day_args}
        if month_max_day_args:
            month_max_days = {month: day for month, day in month_max_day_args}

    select_scenes(att_args=supplied_att_args, months=months, aoi_path=aoi_path,
                  month_min_days=month_min_days, month_max_days=month_max_days,
                  out_selection=out_selection, dryrun=args.dryrun)
