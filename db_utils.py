import json
import os

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
db_conf = os.path.join('config', 'db_cxn.json')

params = json.load(open(db_conf))

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

    logger.debug('Generated SQL:\n{}'.format(sql))

    return sql


def db_connect(user, password, host, database):
    try:
        connection = psycopg2.connect(user=user, password=password, host=host, database=database)
    except(psycopg2.Error) as error:
        connection = None
        logger.error('Error connecting to {} at {}'.format(database, host))

    return connection


def execute_sql(connection, sql):
    cursor = connection.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    return results


def get_layer_columns(connection, layer):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM {} LIMIT 0".format(layer))
    columns = [d[0] for d in cursor.description]

    return columns


def get_values(connection, layer, columns, distinct=False):
    if isinstance(columns, str):
        columns = [columns]
    sql = generate_sql(layer=layer, columns=columns, distinct=distinct, table=True)
    r = execute_sql(connection, sql)
    values = [a[0] for a in r]

    return values


def layer_column_values(user, password, host, database, layer, columns, distinct):
    connection = db_connect(user=user, password=password, host=host, database=database)
    values = get_values(connection=connection, layer=layer, columns=columns, distinct=distinct)

    return values
