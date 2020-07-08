import json
import os

from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import psycopg2
import pandas as pd

from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'DEBUG')

db_conf = os.path.join(os.path.dirname(__file__),'config', 'db_creds.json')

params = json.load(open(db_conf))
user = params['user']
password = params['password']
host = 'localhost'
database = 'plt_fps'
db_config = {'user': user,
             'password': password,
             'database': database,
             'host': host}


def generate_sql(layer, columns=None, where=None, orderby=False, orderby_asc=False, distinct=False,
                 limit=False, offset=None,):
    # COLUMNS
    if columns:
        cols_str = ', '.join(columns)
    else:
        cols_str = '*'  # select all columns

    sql = "SELECT {} FROM {}".format(cols_str, layer)

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
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor
        self.db_name = db_name

    def __new__(cls, db_name):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            try:
                connection = Postgres._instance.connection = psycopg2.connect(user=user, password=password,
                                                                              host=host, database=db_name)
                cursor = Postgres._instance.cursor = connection.cursor()
                cursor.execute('SELECT VERSION()')
                db_version = cursor.fetchone()
            except psycopg2.Error as error:
                Postgres._instance = None
                logger.error('Error connecting to {} at {}'.format(database, host))
                logger.error(error)
                raise error
            else:
                logger.debug('Connection to {} at {} established. Version: {}'.format(database, host, db_version))

        return cls._instance

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def list_db_tables(self):
        self.cursor.execute("""SELECT table_name FROM information_schema.tables""")
        tables = self.cursor.fetchall()
        tables = [x[0] for x in tables]
        tables = sorted(tables)

        return tables

    def execute_sql(self, sql):
        self.cursor.execute(sql)
        results = self.cursor.fetchall()

        return results

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
        engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(user, password, host, database))

        return engine


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


# TODO: Create overwrite scenes function that removes any scenes in the input before writing them to DB