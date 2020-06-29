import json
import os

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import create_engine
import psycopg2

from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'DEBUG')

# TODO: put this all in a config file
# user = 'postgres'
# pw = '1nil2theARSENALfc'
# host = 'localhost'
# database = 'plt_fps'
# layer = "scenes"
db_conf = os.path.join('config', 'db_creds.json')

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
                 limit=False, offset=None, table=False):
    # COLUMNS
    if columns:
        cols_str = ', '.join(columns)
    else:
        cols_str = '*'  # select all columns

    # If table, do not select geometry
    if table == True:
        sql = "SELECT {} FROM {}".format(cols_str, layer)
    else:
        sql = "SELECT {}, encode(ST_AsBinary(geom), 'hex') AS geom FROM {}".format(cols_str, layer)

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

    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            try:
                connection = Postgres._instance.connection = psycopg2.connect(user=user, password=password, host=host, database=database)
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

    def __init__(self):
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor

    def __del__(self):
        self.cursor.close()
        self.connection.close()

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
        r = self.execute_sql(sql)
        values = [a[0] for a in r]

        return values

    def get_engine(self):
        engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(user, password, host, database))

        return engine
#
# def db_connect(database=database, user=user, password=password, host=host):
#     try:
#         connection = psycopg2.connect(user=user, password=password, host=host, database=database)
#
#     except(psycopg2.Error) as error:
#         connection = None
#         logger.error('Error connecting to {} at {}'.format(database, host))
#         logger.error(error)
#         raise error
#
#     return connection
#
#
# def execute_sql(connection, sql):
#     cursor = connection.cursor()
#     cursor.execute(sql)
#     results = cursor.fetchall()
#
#     return results
#
#
# def get_layer_columns(connection, layer):
#     cursor = connection.cursor()
#     cursor.execute("SELECT * FROM {} LIMIT 0".format(layer))
#     columns = [d[0] for d in cursor.description]
#
#     return columns
#
#
# def get_values(connection, layer, columns, distinct=False):
#     if isinstance(columns, str):
#         columns = [columns]
#     sql = generate_sql(layer=layer, columns=columns, distinct=distinct, table=True)
#     r = execute_sql(connection, sql)
#     values = [a[0] for a in r]
#
#     return values
#
#
# def layer_column_values(layer, columns, distinct, database=database, user=user, password=password, host=host):
#     connection = db_connect(user=user, password=password, host=host, database=database)
#     values = get_values(connection=connection, layer=layer, columns=columns, distinct=distinct)
#
#     return values
