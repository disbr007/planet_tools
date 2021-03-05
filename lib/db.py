import json
import re
import os
from pathlib import Path
import sys
import time

from sqlalchemy import create_engine
from tqdm import tqdm
import psycopg2
from psycopg2 import sql
import pandas as pd
import geopandas as gpd

from .lib import get_config, get_geometry_cols
from .logging_utils import create_logger

# Supress pandas SettingWithCopyWarning
pd.set_option('mode.chained_assignment', None)

logger = create_logger(__name__, 'sh', 'INFO')

# Paths to config files for connecting to various DB's
# Params
db_params = get_config("db")
db_config = db_params["db_config"]
tables_config = db_params["tables"]

k_unique_id = "unique_id"  # key in config

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


# def load_db_config(db_conf):
#     """Load config params for connecting to PostGRES DB"""
#     params = json.load(open(db_conf))
#
#     return params


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
    SQL statement to encode geometry column in non-PostGIS Postgres
    database for reading by geopandas.read_postgis.
    """
    geom_sql = "encode(ST_AsBinary({}), 'hex') AS " \
               "{}".format(geom_col, encode_geom_col)

    return geom_sql


def make_identifier(sql_str):
    if (sql_str is not None and
            not isinstance(sql_str, sql.Identifier)):
        return sql.Identifier(sql_str)


def generate_sql(layer, columns=None, where=None, orderby=False,
                 orderby_asc=False, distinct=False, limit=False, offset=None,
                 geom_col=None, encode_geom_col=None, remove_id_tbl=None,
                 remove_id_tbl_col=None, remove_id_src_cols=None):
    """
    geom_col not needed for PostGIS if loading SQL with geopandas -
        gpd can interpet the geometry column without encoding
    """
    if isinstance(columns, str):
        columns = [columns]
    if distinct:
        sql_select = 'SELECT DISTINCT'
    else:
        sql_select = "SELECT"

    # Only necessary for geometries in non-PostGIS DBs
    if geom_col:
        columns.append("encode(ST_AsBinary({}), 'hex') AS "
                       "{}".format(geom_col, encode_geom_col))

    # Create base query object
    query = sql.SQL("{select} {fields} FROM {table}").format(
        select=sql.SQL(sql_select),
        fields=sql.SQL(',').join([sql.Identifier(f) for f in columns]),
        table=sql.Identifier(layer))
    # Add any provided additional parameters
    if where:
        sql_where = " WHERE {}".format(where)
        # Remove IDs found in another table
        if all(remove_id_tbl, remove_id_tbl_col, remove_id_src_cols):
            remove_wheres = ["{} IN (SELECT {} FROM {}".format(
                             id_src_col, remove_id_tbl_col, remove_id_tbl)
                             for id_src_col in remove_id_src_cols]
            remove_where = """({})""".format(' AND '.join(remove_wheres))
            if where:
                sql_where += """ AND {}""".format(remove_where)
            else:
                sql_where = """ WHERE {}""".format(remove_where)
        query += sql.SQL(sql_where)
    if orderby:
        if orderby_asc:
            asc = 'ASC'
        else:
            asc = 'DESC'
        sql_orderby = sql.SQL("ORDER BY {field} {asc}").format(
            field=sql.Identifier(orderby),
            asc=sql.Literal(asc))
        query += sql_orderby
    if limit:
        sql_limit = sql.SQL("LIMIT {}".format(limit))
        query += sql_limit
    if offset:
        sql_offset = sql.SQL("OFFSET {}".format(offset))
        query += sql_offset

    logger.debug('Generated SQL: {}'.format(query))

    return query


def stereo_pair_sql(aoi=None, date_min=None, date_max=None, ins=None,
                    date_diff_min=None, date_diff_max=None,
                    view_angle_diff=None,
                    ovlp_perc_min=None, ovlp_perc_max=None,
                    off_nadir_diff_min=None, off_nadir_diff_max=None,
                    limit=None, orderby=False, orderby_asc=False,
                    remove_id_tbl=None, remove_id_tbl_col=None,
                    remove_id_src_cols=None, geom_col=fld_geom, columns='*'):
    """
    Create SQL statment to select stereo pairs based on passed
    arguments.
    """
    # Ensure properly quoted identifiers
    remove_id_tbl = make_identifier(remove_id_tbl)

    # TODO: make this a loop over a dict of fields and argss
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

    sql_statement = generate_sql(layer=stereo_pair_cand, columns=columns,
                                 where=where, limit=limit, orderby=orderby,
                                 orderby_asc=orderby_asc,
                                 remove_id_tbl=remove_id_tbl,
                                 remove_id_tbl_col=remove_id_tbl_col,
                                 remove_id_src_cols=remove_id_src_cols)

    return sql_statement


def intersect_aoi_where(aoi, geom_col='geometry'):
    """Create a where statement for a PostGIS intersection between the
    geometry(s) in the aoi geodataframe and a PostGIS table with
    geometry in geom_col"""
    aoi_epsg = aoi.crs.to_epsg()
    aoi_wkts = [geom.wkt for geom in aoi.geometry]
    intersect_wheres = ["ST_Intersects({}, ST_SetSRID('{}'::geometry, " \
                        "{}))".format(geom_col, wkt, aoi_epsg,)
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

    def __init__(self):
        self.host = db_config['host']
        self.database = db_config['database']
        self.user = db_config['user']
        self.password = db_config['password']
        self._connection = None
        self._cursor = None

    @property
    def connection(self):
        """Establish connection to database."""
        if self._connection is None:
            try:
                self._connection = psycopg2.connect(user=self.user,
                                                    password=self.password,
                                                    host=self.host,
                                                    database=self.database)

            except psycopg2.Error as error:
                Postgres._instance = None
                logger.error('Error connecting to {} at '
                             '{}'.format(self.database, self.host))
                logger.error(error)
                raise error
            else:
                logger.debug('Connection to {} at {} '
                             'established.'.format(self.database, self.host))

        return self._connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.connection.closed:
            if not self.cursor.closed:
                self.cursor.close()
            self.connection.close()

    def __del__(self):
        if not self.connection.closed:
            if not self.cursor.closed:
                self.cursor.close()
            self.connection.close()

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.connection.cursor()
        if self._cursor.closed:
            self._cursor = self.connection.cursor()

        return self._cursor

    def list_db_tables(self):
        """List all tables in the database."""
        logger.debug('Listing tables...')
        tables_sql = sql.SQL("""SELECT table_schema as schema_name,
                                       table_name as view_name
                                FROM information_schema.tables
                                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                                ORDER BY schema_name, view_name""")
        self.cursor.execute(tables_sql)
        tables = self.cursor.fetchall()
        logger.debug('Tables: {}'.format(tables))

        views_sql = sql.SQL("""SELECT table_schema as schema_name,
                                      table_name as view_name
                               FROM information_schema.views
                               WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                               ORDER BY schema_name, view_name""")
        self.cursor.execute(views_sql)

        # tables = self.cursor.fetchall()
        views = self.cursor.fetchall()
        tables.extend(views)
        logger.debug('Views: {}'.format(views))

        matviews_sql = sql.SQL("""SELECT schemaname as schema_name, 
                                      matviewname as view_name
                                  FROM pg_matviews""")
        self.cursor.execute(matviews_sql)
        matviews = self.cursor.fetchall()
        tables.extend(matviews)
        logger.debug("Materialized views: {}".format(matviews))

        tables = [x[1] for x in tables]
        tables = sorted(tables)

        return tables

    def execute_sql(self, sql_query):
        """Execute the passed query on the database."""
        if not isinstance(sql_query, (sql.SQL, sql.Composable, sql.Composed)):
            sql_query = sql.SQL(sql_query)
        # logger.debug('SQL query: {}'.format(sql_query))
        self.cursor.execute(sql_query)
        results = self.cursor.fetchall()

        return results

    def get_sql_count(self, sql_str):
        """Get count of records returned by passed query. Query should
        not have COUNT() in it already."""
        if not isinstance(sql_str, sql.SQL):
            sql_str = sql.SQL(sql_str)
        count_sql = sql.SQL(re.sub('SELECT (.*) FROM',
                                   'SELECT COUNT(\\1) FROM',
                                   sql_str.string))
        logger.debug('Count sql: {}'.format(count_sql))
        self.cursor.execute(count_sql)
        count = self.cursor.fetchall()[0][0]

        return count

    def get_table_count(self, table):
        """Get total count for the passed table."""
        if not isinstance(table, sql.Identifier):
            table = sql.Identifier(table)
        self.cursor.execute(sql.SQL(
            """SELECT COUNT(*) FROM {}""").format(table))
        count = self.cursor.fetchall()[0][0]
        logger.debug('{} count: {:,}'.format(table, count))

        return count

    def get_table_columns(self, table):
        """Get columns in passed table."""
        self.cursor.execute(sql.SQL(
            "SELECT * FROM {} LIMIT 0").format(sql.Identifier(table)))
        columns = [d[0] for d in self.cursor.description]

        return columns

    def get_values(self, table, columns, distinct=False, is_table=False):
        """Get values in the passed columns(s) in the passed table. If
        distinct, unique values returned (across all columns passed)"""
        # if not isinstance(layer, sql.Identifier):
        #     layer = sql.Identifier(layer)

        if isinstance(columns, str):
            columns = [columns]

        sql_statement = generate_sql(layer=table, columns=columns,
                                     distinct=distinct,)# is_table=True)
        values = self.execute_sql(sql_statement)

        # Convert from list of tuples to flat list if only one column
        if len(columns) == 1:
            values = [a[0] for a in values]

        return values

    def get_engine(self):
        """Create sqlalchemy.engine object."""
        engine = create_engine('postgresql+psycopg2://'
                               '{}:{}@{}/{}'.format(self.user, self.password,
                                                    self.host, self.database))

        return engine

    def sql2gdf(self, sql_str, geom_col='geometry', crs=4326,):
        """Get a GeoDataFrame from a passed SQL query"""
        gdf = gpd.GeoDataFrame.from_postgis(sql=sql_str,
                                            con=self.get_engine().connect(),
                                            geom_col=geom_col, crs=crs)
        return gdf

    def sql2df(self, sql_str, columns=None):
        """Get a DataFrame from a passed SQL query"""
        if isinstance(sql_str, sql.Composed):
            sql_str = sql_str.as_string(self.cursor)
        if isinstance(columns, str):
            columns = [columns]

        df = pd.read_sql(sql=sql_str, con=self.get_engine().connect(),
                         columns=columns)

        return df

    def insert_new_records(self, records, table, dryrun=False):
        """
        Add records to table, converting data types as necessary for INSERT.
        Optionally using a unique_id (or combination of columns) to skip
        duplicates.
        records : pd.DataFrame / gpd.GeoDataFrame
            DataFrame containing rows to be inserted to table
        table : str
            Name of table to be inserted into
        """
        # TODO: Create overwrite scenes option that removes any scenes in the
        #  input from the DB before writing them

        def _row_columns_unique(row, unique_on, values):
            """Determines if row has combination of columns in unique_on that
            are in values.
            Parameters
            ----------
            row : pd.Series
                Table row to be inserted
            unique_on : list / tuple
                Column names that when combined indicate a unique row
            values : list / tuple
                Values to check row against
            Returns
            -------
            bool
            """
            if isinstance(unique_on, str):
                unique_on = [unique_on]
            row_values = [row[c] for c in unique_on]
            if len(row_values) > 1:
                row_values = tuple(row_values)
            else:
                row_values = row_values[0]

            return row_values in values

        # Check that records is not empty
        if len(records) == 0:
            logger.warning('No records to be added.')
            return

        # Check if table exists, get table starting count, unique constraint
        logger.info('Inserting records into {}...'.format(table))
        if table in self.list_db_tables():
            logger.info('Starting count for {}: '
                        '{:,}'.format(table, self.get_table_count(table)))
            unique_on = tables_config[table][k_unique_id]
        else:
            logger.warning('Table "{}" not found in database "{}", '
                           'exiting.'.format(table, self.database))
            sys.exit()

        # Get unique IDs to remove duplicates if provided
        if table in self.list_db_tables() and unique_on is not None:
            # Remove duplicate values from rows to insert based on unique_on
            # columns
            existing_ids = self.get_values(table=table, columns=unique_on,
                                           distinct=True)
            logger.debug('Removing any existing IDs from search results...')
            logger.debug('Existing unique IDs in table "{}": '
                         '{:,}'.format(table, len(existing_ids)))

            # Remove dups
            starting_count = len(records)
            records = records[~records.apply(lambda x: _row_columns_unique(
                x, unique_on, existing_ids), axis=1)]
            if len(records) != starting_count:
                logger.info('Duplicates removed: {}'.format(starting_count -
                                                            len(records)))
        logger.info('IDs to add: {:,}'.format(len(records)))
        if len(records) == 0:
            logger.info('No new records, skipping indexing.')
            return

        geom_cols = get_geometry_cols(records)
        if geom_cols:
            # Get epsg code
            srid = records.crs.to_epsg()
        else:
            geom_cols = []

        # Insert new records
        if len(records) != 0:
            logger.info('Writing new records to {}.{}: '
                        '{:,}'.format(self.database, table, len(records)))

            for i, row in tqdm(records.iterrows(),
                               desc='Adding new records to: {}'.format(table),
                               total=len(records)):
                if dryrun:
                    if i == 0:
                        logger.info('-dryrun-')
                    continue

                # Format the INSERT query
                columns = [sql.Identifier(c) for c in row.index if c not in
                           geom_cols]
                if geom_cols:
                    for gc in geom_cols:
                        columns.append(sql.Identifier(gc))
                # Create INSERT statement, parenthesis left open intentionally
                # to accommodate adding geometry statements, e.g.:
                # "ST_GeomFromText(..)"
                # paranthesis, closed in else block if no geometry columns
                insert_statement = sql.SQL(
                    "INSERT INTO {table} ({columns}) VALUES ({values}").format(
                    table=sql.Identifier(table),
                    columns=sql.SQL(', ').join(columns),
                    values=sql.SQL(', ').join([sql.Placeholder(f)
                                               for f in row.index
                                               if f not in geom_cols]),
                )
                if geom_cols:
                    geom_statements = [sql.SQL(', ')]
                    for i, gc in enumerate(geom_cols):
                        if i != len(geom_cols) - 1:
                            geom_statements.append(
                                sql.SQL(
                                    " ST_GeomFromText({gc}, {srid}),").format(
                                    gc=sql.Placeholder(gc),
                                    srid=sql.Literal(srid)))
                        else:
                            geom_statements.append(
                                sql.SQL(
                                    " ST_GeomFromText({gc}, {srid}))").format(
                                    gc=sql.Placeholder(gc),
                                    srid=sql.Literal(srid)))
                    geom_statement = sql.Composed(geom_statements)
                    insert_statement = insert_statement + geom_statement
                else:
                    # Close paranthesis that was left open for geometries
                    insert_statement = sql.SQL("{statement})").format(
                        statement=insert_statement)

                values = {f: row[f] if f not in geom_cols
                          else row[f].wkt for f in row.index}

                # Make the INSERT
                with self.cursor as cursor:
                    try:
                        cursor.execute(self.cursor.mogrify(insert_statement,
                                                           values))
                        self.connection.commit()
                    except Exception as e:
                        if e == psycopg2.errors.UniqueViolation:
                            logger.warning('Skipping due to unique violation '
                                           'for scene: '
                                           '{}'.format(row[unique_on]))
                            logger.warning(e)
                            self.connection.rollback()
                        elif e == psycopg2.errors.IntegrityError:
                            logger.warning('Skipping due to integrity error '
                                           'for scene: '
                                           '{}'.format(row[unique_on]))
                            logger.warning(e)
                            self.connection.rollback()
                        else:
                            logger.debug('Error on statement: {}'.format(
                                f"{str(self.cursor.mogrify(insert_statement, values))}"))
                            logger.error(e)
                            self.connection.rollback()
        else:
            logger.info('No new records to be written.')
            
        logger.info('New count for {}.{}: '
                    '{:,}'.format(self.database, table,
                                  self.get_table_count(table)))
