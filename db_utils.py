import json
import re
import os

from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import psycopg2
import pandas as pd
import geopandas as gpd

from misc_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

db_confs = {
    'sandwich-pool.dem': os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                      'config', 'sandwich-pool.dem.json'),
    'danco.footprint': os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                    'config', 'danco.footprint.json'),
}


def load_db_config(db_conf):
    params = json.load(open(db_conf))

    return params


def encode_geom_sql(geom_col, encode_geom_col):
    geom_sql = """encode(ST_AsBinary({}), 'hex') AS {}""".format(geom_col, encode_geom_col)

    return geom_sql


def generate_sql(layer, columns=None, where=None, orderby=False, orderby_asc=False,
                 distinct=False, limit=False, offset=None,
                 geom_col=None, encode_geom_col=None):
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


class Postgres(object):
    _instance = None

    def __init__(self, db_name):
        # self.connection = self._instance.connection
        # self.cursor = self._instance.cursor
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
        # self.cursor.execute("""SELECT table_name FROM information_schema.tables""")
        # self.cursor.execute("""SELECT table_schema as schema_name,
        #                               table_name as view_name
        #                        FROM information_schema.views
        #                        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        #                        ORDER BY schema_name, view_name""")
        # # tables = self.cursor.fetchall()
        # views = self.cursor.fetchall()
        self.cursor.execute("""SELECT table_schema as schema_name,
                                      table_name as view_name
                               FROM information_schema.tables
                               WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                               ORDER BY schema_name, view_name""")
        tables = self.cursor.fetchall()
        # tables.extend(views)
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

    def get_layer_columns(self, layer):
        self.cursor.execute("SELECT * FROM {} LIMIT 0".format(layer))
        columns = [d[0] for d in self.cursor.description]

        return columns

    def get_values(self, layer, columns, distinct=False):
        if isinstance(columns, str):
            columns = [columns]
        sql = generate_sql(layer=layer, columns=columns, distinct=distinct, table=True)
        values = self.execute_sql(sql)
        # values = [a[0] for a in r]

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


def insert_new_records(records, table, unique_id=None, dryrun=False):
    logger.debug('Loading existing IDs..')
    with Postgres() as pg:

        if table in pg.list_db_tables():
            existing_ids = pg.get_values(table, columns=[unique_id], distinct=True)
            logger.debug('Removing any existing IDs from search results...')
        else:
            logger.error('Table "{}" not found in database "{}"'.format(table, pg.db_name))

        logger.debug('Existing unique IDs in table "{}": {:,}'.format(table, len(existing_ids)))
        new = records[~records[unique_id].isin(existing_ids)].copy()
        del records

        logger.debug('Remaining IDs to add: {:,}'.format(len(new)))

        # Get epsg code
        srid = new.crs.to_epsg()
        # Drop old format geometry column
        geometry_name = new.geometry.name
        new['geom'] = new.geometry.apply(lambda x: WKTElement(x.wkt, srid=srid))
        new.drop(columns=geometry_name, inplace=True)
        # Convert date column to datetime
        new['acquired'] = pd.to_datetime(new['acquired'], format="%Y-%m-%dT%H:%M:%S.%fZ")

        # logger.debug('Dataframe column types:\n{}'.format(new.dtypes))
        if len(new) != 0 and not dryrun:
            logger.info('Writing new IDs to {}: {}'.format(table, len(new)))
            new.to_sql(table, con=pg.get_engine(), if_exists='append', index=False,
                       dtype={'geom': Geometry('POLYGON', srid=srid)})
        else:
            logger.info('No new records to be written.')


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
# TODO: Create overwrite scenes function that removes any scenes in the input before writing them to DB