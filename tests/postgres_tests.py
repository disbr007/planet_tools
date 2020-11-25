from lib.lib import Postgres

db_config = 'sandwich-pool.planet'
tbl = 'scenes_onhand'
columns = ['id']
geom_col = 'geometry'

with Postgres(db_config) as db_src:
    tbls = db_src.list_db_tables()
    q = db_src.execute_sql("SELECT * FROM {}".format(tbl))
    ct = db_src.get_sql_count("SELECT * FROM {}".format(tbl))
    tct = db_src.get_table_count(tbl)
    lcols = db_src.get_table_columns(tbl)
    vals = db_src.get_values(tbl, columns=columns)
    df = db_src.sql2df("SELECT * FROM {}".format(tbl))
    gdf = db_src.sql2gdf("SELECT * FROM {}".format(tbl),
                         geom_col=geom_col)
