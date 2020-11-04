import geopandas as gpd
from lib.lib import Postgres
from psycopg2 import sql


gdf = gpd.read_file(r'E:\disbr007\projects\planet\footprints'
                    r'\local_fps_2019dec.geojson')
"""
insert_statement = sql.SQL(
    "INSERT INTO {table} ({columns}) VALUES ("
    "{values}, "
    "ST_GeomFromText({geometry}, {srid}), "
    "ST_GeomFromText({centroid}, {srid})"
    ")").format(
    table=sql.Identifier(table),
    columns=sql.SQL(', ').join(columns),
    values=sql.SQL(', ').join([sql.Placeholder(f)
                               for f in row.index
                               if f not in geom_cols]),
    geometry=sql.Placeholder('geometry'),
    centroid=sql.Placeholder('centroid'),
    srid=sql.Literal(srid))
"""
table = 'scenes'
columns = [c for c in list(gdf) if c != gdf.geometry.name]
geom_cols = ['geometry']
srid = gdf.crs.to_epsg()

with Postgres('sandwich-pool.planet') as db_src:
    for i, row in gdf.iterrows():
        insert_statement = sql.SQL(
            "INSERT INTO {table} ({columns}) VALUES ("
            "{values})").format(
            table=sql.Identifier(table),
            columns=sql.SQL(', ').join([sql.Identifier(c) for c in
                                        columns]),
            values=sql.SQL(', ').join([sql.Placeholder(f)
                                       for f in row.index
                                       if f not in geom_cols]),
            )
        if geom_cols:
            geom_statements = [sql.Literal(',')]
            for i, gc in enumerate(geom_cols):
                if i != len(geom_cols) - 1:
                    geom_statements.append(
                        sql.SQL(" ST_GeomFromText({gc}, {srid}),").format(
                            gc=sql.Placeholder(gc),
                            srid=sql.Literal(srid)))
                else:
                    geom_statements.append(
                        sql.SQL(" ST_GeomFromText({gc}, {srid})").format(
                            gc=sql.Placeholder(gc),
                            srid=sql.Literal(srid)))
            geom_statement = sql.Composed(geom_statements)
            insert_statement = insert_statement + geom_statement

        values = {f: row[f] if f not in geom_cols
                  else row[f].wkt for f in row.index}

        print(db_src.cursor.mogrify(insert_statement, values))
        break