import argparse
import os

import geopandas as gpd
from sqlalchemy.exc import ProgrammingError

from db_utils import Postgres, intersect_aoi_where
from lib import write_gdf, parse_group_args
# TODO: Fix this - place attrib_arg_lut dict somewhere better
from search_utils import attrib_arg_lut
from logging_utils.logging_utils import create_logger


scenes_tbl = 'scenes'

def build_argument_sql(att_args=None, months=None, aoi_path=None,):
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

    if months:
        for month in months:
            month_where = """EXTRACT(MONTH FROM acquired) = {}""".format(month)
            where_statements.append(month_where)

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

    sql = """SELECT * FROM {} WHERE {}""".format(scenes_tbl, where)

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


def select_scenes(att_args, aoi_path, months=None, out_selection=None, dryrun=False):
    sql = build_argument_sql(att_args=att_args, months=months, aoi_path=aoi_path)
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

    parser = argparse.ArgumentParser("Select footprints by attribute or AOI from scenes table.")

    attribute_args = parser.add_argument_group(att_group)

    # parser.add_argument('-n', '--name', type=str, help='Name of search to create')
    parser.add_argument('--months', type=str, nargs='+',
                                help='Month as zero-padded number, e.g. 04')
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
    attribute_args = parse_group_args(parser, att_group)

    supplied_att_args = [(kwa[0], kwa[1]) for kwa in attribute_args._get_kwargs()
                         if kwa[1]]

    select_scenes(att_args=supplied_att_args, months=months, aoi_path=aoi_path,
                  out_selection=out_selection, dryrun=args.dryrun)
