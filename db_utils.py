import json
import re
import os

from sqlalchemy import create_engine
from sqlalchemy.types import DateTime
from geoalchemy2 import Geometry, WKTElement
import psycopg2
import pandas as pd
import geopandas as gpd

from logging_utils.logging_utils import create_logger

# Supress pandas SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

logger = create_logger(__name__, 'sh', 'INFO')

# Paths to config files for connecting to various DB's
db_confs = {
    'sandwich-pool.dem': os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                      'config', 'sandwich-pool.dem.json'),
    'danco.footprint': os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                    'config', 'danco.footprint.json'),
    'sandwich-pool.planet': os.path.join(os.path.dirname(__file__),
                                         'config', 'sandwich-pool.planet.json'),
}

# Params
stereo_pair_cand = 'stereo_candidates'
fld_acq = 'acquired'
fld_acq1 = '{}1'.format(fld_acq)
fld_ins = 'instrument'
fld_ins1 = '{}1'.format(fld_ins)
fld_ins2 = '{}2'.format(fld_ins)
fld_date_diff = 'date_diff'
fld_view_angle_diff = 'view_angle_diff'
fld_off_nadir_diff = 'off_nadir_diff'
fld_ovlp_perc = 'ovlp_perc'
fld_geom = 'ovlp_geom'


def load_db_config(db_conf):
    """Load config params for connecting to PostGRES DB"""
    params = json.load(open(db_conf))

    return params


def check_where(where, join='AND'):
    if where:
        where += """ {} """.format(join)
    else:
        where = ""

    return where


def ids2sql(ids):
    return str(ids)[1:-1]


def encode_geom_sql(geom_col, encode_geom_col):
    """
    SQL statement to encode geometry column in non-PostGIS PostGRES database for reading
    by geopandas.read_postgis.
    """
    geom_sql = """encode(ST_AsBinary({}), 'hex') AS {}""".format(geom_col, encode_geom_col)

    return geom_sql


def generate_sql(layer, columns=None, where=None, orderby=False, orderby_asc=False,
                 distinct=False, limit=False, offset=None,
                 geom_col=None, encode_geom_col=None,
                 remove_id_tbl=None, remove_id_tbl_col=None, remove_id_src_cols=None):
    """
    geom_col not needed for PostGIS if loading SQL with geopandas -
        gpd can interpet the geometry column without encoding
    """
    # COLUMNS
    if columns:
        cols_str = ', '.join(columns)
    else:
        cols_str = '*'  # select all columns

    if not geom_col:
        sql = "SELECT {} FROM {}".format(cols_str, layer)
    else:
        sql = "SELECT encode(ST_AsBinary({}), 'hex') AS {}, {} FROM {}".format(geom_col, encode_geom_col,
                                                                               cols_str, layer)

    # CUSTOM WHERE CLAUSE
    if where:
        sql_where = " WHERE {}".format(where)
        sql = sql + sql_where
    # REMOVE IDs found in another table
    if remove_id_tbl and remove_id_tbl_col and remove_id_src_cols:
        remove_wheres = ["""{} IN (SELECT {} FROM {}""".format(id_src_col,
                                                               remove_id_tbl_col,
                                                               remove_id_tbl)
                         for id_src_col in remove_id_src_cols]
        remove_where = """({})""".format(' AND '.join(remove_wheres))
        if where:
            sql += """ AND {}""".format(remove_where)
        else:
            sql += """ WHERE {}""".format(remove_where)

    # ORDERBY
    if orderby:
        if orderby_asc:
            asc = 'ASC'
        else:
            asc = 'DESC'
        sql_orderby = " ORDER BY {} {}".format(orderby, asc)
        sql += sql_orderby

    # LIMIT number of rows
    if limit:
        sql_limit = " LIMIT {}".format(limit)
        sql += sql_limit
    if offset:
        sql_offset = " OFFSET {}".format(offset)
        sql += sql_offset

    if distinct:
        sql = sql.replace('SELECT', 'SELECT DISTINCT')

    logger.debug('Generated SQL: {}'.format(sql))

    return sql


def stereo_pair_sql(aoi=None, date_min=None, date_max=None, ins=None,
                    date_diff_min=None, date_diff_max=None,
                    view_angle_diff=None,
                    ovlp_perc_min=None, ovlp_perc_max=None,
                    off_nadir_diff_min=None, off_nadir_diff_max=None,
                    limit=None, orderby=False, orderby_asc=False,
                    remove_id_tbl=None, remove_id_tbl_col=None, remove_id_src_cols=None,
                    geom_col=fld_geom, columns='*'):
    """Create SQL statment to select stereo pairs based on passed arguments."""
    where = ""
    if date_min:
        where = check_where(where)
        where += "{} >= '{}'".format(fld_acq1, date_min)
    if date_max:
        where = check_where(where)
        where += "{} <= '{}'".format(fld_acq1, date_max)
    if ins:
        where = check_where(where)
        where += "{0} = '{1}' AND {2} = '{1}'".format(fld_ins1, ins, fld_ins2)
    if date_diff_max:
        where = check_where(where)
        where += "{} <= {}".format(fld_date_diff, date_diff_max)
    if date_diff_min:
        where = check_where(where)
        where += "{} <= {}".format(fld_date_diff, date_diff_min)
    if off_nadir_diff_max:
        where = check_where(where)
        where += "{} <= {}".format(fld_off_nadir_diff, off_nadir_diff_max)
    if off_nadir_diff_min:
        where = check_where(where)
        where += "{} >= {}".format(fld_off_nadir_diff, off_nadir_diff_min)
    if view_angle_diff:
        where = check_where(where)
        where += "{} >= {}".format(fld_view_angle_diff, view_angle_diff)
    if ovlp_perc_min:
        where = check_where(where)
        where += "{} >= {}".format(fld_ovlp_perc, ovlp_perc_min)
    if ovlp_perc_max:
        where = check_where(where)
        where += "{} >= {}".format(fld_ovlp_perc, ovlp_perc_max)
    if isinstance(aoi, gpd.GeoDataFrame):
        where = check_where(where)
        where += intersect_aoi_where(aoi, geom_col=geom_col)

    if columns != '*' and geom_col not in columns:
        columns.append(geom_col)

    sql = generate_sql(layer=stereo_pair_cand, columns=columns, where=where,
                       limit=limit, orderby=orderby, orderby_asc=orderby_asc,
                       remove_id_tbl=remove_id_tbl, remove_id_tbl_col=remove_id_tbl_col,
                       remove_id_src_cols=remove_id_src_cols)

    return sql


def intersect_aoi_where(aoi, geom_col):
    """Create a where statement for a PostGIS intersection between the geometry(s) in
    the aoi geodataframe and a PostGIS table with geometry in geom_col"""
    aoi_epsg = aoi.crs.to_epsg()
    aoi_wkts = [geom.wkt for geom in aoi.geometry]
    intersect_wheres = ["""ST_Intersects({}, ST_SetSRID('{}'::geometry, {}))""".format(geom_col,
                                                                                       wkt,
                                                                                       aoi_epsg,)
                        for wkt in aoi_wkts]
    aoi_where = " OR ".join(intersect_wheres)

    return aoi_where


class Postgres(object):
    """
    Class for interacting with Postgres database using psycopg2. This
    allows keeping a connection and cursor open while performing multiple
    operations. Best used with a context manager, i.e.:
    with Postgres(db_name) as db:
        ...
    """
    _instance = None

    def __init__(self, db_name):
        config = load_db_config(db_confs[db_name])
        self.host = config['host']
        self.database = config['database']
        self.user = config['user']
        self.password = config['password']
        try:
            self.connection = psycopg2.connect(user=self.user, password=self.password,
                                               host=self.host, database=self.database)
            self.cursor = self.connection.cursor()
            self.cursor.execute('SELECT VERSION()')
            db_version = self.cursor.fetchone()
        except psycopg2.Error as error:
            Postgres._instance = None
            logger.error('Error connecting to {} at {}'.format(self.database, self.host))
            logger.error(error)
            raise error
        else:
            logger.debug('Connection to {} at {} established. Version: {}'.format(self.database, self.host,
                                                                                  db_version))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def list_db_tables(self):
        logger.debug('Listing tables...')
        self.cursor.execute("""SELECT table_schema as schema_name,
                                      table_name as view_name
                               FROM information_schema.tables
                               WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                               ORDER BY schema_name, view_name""")
        tables = self.cursor.fetchall()
        logger.debug('Tables: {}'.format(tables))

        self.cursor.execute("""SELECT table_schema as schema_name,
                                      table_name as view_name
                               FROM information_schema.views
                               WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                               ORDER BY schema_name, view_name""")

        # tables = self.cursor.fetchall()
        views = self.cursor.fetchall()
        tables.extend(views)
        logger.debug('Views: {}'.format(views))

        self.cursor.execute("""SELECT schemaname as schema_name, 
                                      matviewname as view_name
                               FROM pg_matviews""")
        matviews = self.cursor.fetchall()
        tables.extend(matviews)
        logger.debug("Materialized views: {}".format(matviews))

        tables = [x[1] for x in tables]
        tables = sorted(tables)

        return tables

    def execute_sql(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()

        return results

    def get_sql_count(self, sql):
        count_sql = re.sub('SELECT (.*) FROM', 'SELECT COUNT(\\1) FROM', sql)
        logger.debug('Count sql: {}'.format(count_sql))
        self.cursor.execute(count_sql)
        count = self.cursor.fetchall()[0][0]

        return count

    def get_table_count(self, layer):
        self.cursor.execute("SELECT COUNT(*) FROM {}".format(layer))
        count = self.cursor.fetchall()[0][0]

        return count

    def get_layer_columns(self, layer):
        self.cursor.execute("SELECT * FROM {} LIMIT 0".format(layer))
        columns = [d[0] for d in self.cursor.description]

        return columns

    def get_values(self, layer, columns, distinct=False, table=False):
        if isinstance(columns, str):
            columns = [columns]
        sql = generate_sql(layer=layer, columns=columns, distinct=distinct,)# table=True)
        values = self.execute_sql(sql)
        values = [a[0] for a in values]

        return values

    def get_engine(self):
        engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(self.user, self.password,
                                                                          self.host, self.database))

        return engine

    def sql2gdf(self, sql, geom_col='geom', crs=4326,):
        gdf = gpd.GeoDataFrame.from_postgis(sql=sql, con=self.get_engine().connect(),
                                                geom_col=geom_col, crs=crs)
        return gdf

    def sql2df(self, sql, columns=None):
        df = pd.read_sql(sql=sql, con=self.get_engine().connect(), columns=columns)

        return df

    def insert_new_records(self, records, table, unique_id=None,
                           date_cols=None, date_format="%Y-%m-%dT%H:%M:%S.%fZ",
                           dryrun=False):
        """Add records to table, optionally using a unique_id to skip duplicates."""
        logger.info('Inserting records into {}...'.format(table))
        logger.debug('Loading existing IDs..')
        if isinstance(records, gpd.GeoDataFrame):
            has_geometry = True
        else:
            has_geometry = False

        if table in self.list_db_tables() and unique_id is not None:
            existing_ids = self.get_values(layer=table, columns=[unique_id], distinct=True)
            logger.debug('Removing any existing IDs from search results...')
            logger.debug('Existing unique IDs in table "{}": {:,}'.format(table, len(existing_ids)))
            if len(existing_ids) != 0:
                logger.debug('Example ID: {}'.format(existing_ids[0]))
            new = records[~records[unique_id].isin(existing_ids) == True]
            del records
        elif table not in self.list_db_tables():
            logger.warning('Table "{}" not found in database "{}", creating new table'.format(table,
                                                                                              self.database))
            new = records
        elif unique_id is None:
            logger.warning('No unique ID provided. Exiting to avoid adding duplicates.')
            # new = records
            new = None

        logger.debug('Remaining IDs to add: {:,}'.format(len(new)))

        if has_geometry:
            # Get epsg code
            srid = new.crs.to_epsg()
            # Drop old format geometry column
            geometry_name = new.geometry.name
            if not geometry_name:
                geometry_name = 'geometry'
            # logger.info('New geometry:\n'.format('\n'.join(list(new.geometry))))
            logger.info('Features to add: {}'.format(len(new)))
            # logger.info('Features with valid geom: {}'.format(len(new[new.geometry].isnull())))
            new['geom'] = new.geometry.apply(lambda x: WKTElement(x.wkt, srid=srid))
            new.drop(columns=geometry_name, inplace=True)

        # Convert date column to datetime
        dtype = None
        if date_cols:
            for col in date_cols:
                new[col] = pd.to_datetime(new[col], format=date_format)

        # logger.debug('Dataframe column types:\n{}'.format(new.dtypes))
        if len(new) != 0 and not dryrun:
            logger.info('Writing new IDs to {}.{}: {:,}'.format(self.database, table, len(new)))
            if date_cols:
                dtype = {dc: DateTime() for dc in date_cols}
            if has_geometry:
                geom_dtype = {'geom': Geometry('POLYGON', srid=srid)}
                dtype.update(geom_dtype)
                new.to_sql(table, con=self.get_engine(), if_exists='append', index=False,
                           dtype=dtype)
            else:
                new.to_sql(table, con=self.get_engine(), if_exists='append', index=False,
                           dtype=dtype)
        else:
            logger.info('No new records to be written.')
            
        logger.info('New count for {}.{}: {}'.format(self.database, table, self.get_table_count(table)))

# TODO: Create overwrite scenes function that removes any scenes in the input before writing them to DB
