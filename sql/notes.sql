# ALL PAIRS VIEW
# Create view with overlap percent and overlap geometry and day diff col
CREATE OR REPLACE VIEW scene_pair AS SELECT a.id || b.id AS pairname, a.id AS id1, b.id AS id2, a.acquired AS acquired1, b.acquired AS acquired2, a.geom AS geom1, b.geom AS geom2, ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) AS overlap, ST_INTERSECTION(a.geom, b.geom) AS geom FROM scenes AS a, scenes AS b WHERE a.id > b.id AND a.ground_control=1 AND b.ground_control=1 AND ST_Intersects(a.geom, b.geom);

# Add day diff col
CREATE OR REPLACE VIEW scene_pair AS SELECT a.id || b.id AS pairname, a.id AS id1, b.id AS id2, a.acquired AS acquired1, b.acquired AS acquired2, ABS(DATE_PART('day', a.acquired - b.acquired)) AS date_diff, ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) AS overlap, ST_INTERSECTION(a.geom, b.geom) AS ovlp_geom FROM scenes AS a, scenes AS b WHERE a.id > b.id AND a.ground_control=1 AND b.ground_control=1 AND ST_Intersects(a.geom, b.geom);

# Add view angle diff col
CREATE OR REPLACE VIEW scene_pair AS SELECT  a.id || '-' || b.id AS pairname, a.id AS id1, b.id AS id2, a.acquired AS acquired1, b.acquired AS acquired2, a.instrument AS instrument1, b.instrument AS instrument2, a.view_angle AS view_angle1, b.view_angle as view_angle2, ABS(DATE_PART('day', a.acquired - b.acquired)) AS date_diff, ABS(a.view_angle - b.view_angle) AS view_angle_diff, ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) AS ovlp_perc, ST_INTERSECTION(a.geom, b.geom) AS ovlp_geom FROM scenes AS a, scenes AS b WHERE a.id > b.id AND a.ground_control=1 AND b.ground_control=1 AND ST_Intersects(a.geom, b.geom);


CREATE MATERIALIZED VIEW scene_pair AS SELECT  a.id || '-' || b.id AS pairname, a.id AS id1, b.id AS id2, a.acquired AS acquired1, b.acquired AS acquired2, a.instrument AS instrument1, b.instrument AS instrument2, a.view_angle AS view_angle1, b.view_angle as view_angle2, ABS(DATE_PART('day', a.acquired - b.acquired)) AS date_diff, ABS(a.view_angle - b.view_angle) AS view_angle_diff, ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) AS ovlp_perc, ST_INTERSECTION(a.geom, b.geom) AS ovlp_geom FROM scenes AS a, scenes AS b WHERE a.id > b.id AND a.ground_control=1 AND b.ground_control=1 AND ST_Intersects(a.geom, b.geom);


# Create spatial index on scene_pair
CREATE INDEX idx_scene_pair_ovlp_geom ON scene_pair USING GIST(ovlp_geom);

# CANDIDATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW candidate_pairs AS SELECT * FROM scene_pair WHERE view_angle_diff > 1.75 AND date_diff <= 10 AND ovlp_perc >= 0.50 AND ovlp_perc <= 0.70 AND instrument1 = 'PS2' AND instrument2 = 'PS2';


# NOTES
# Create DB
psql -U postgres
CREATE DATABASE example_gis;
\connect example_gis;
CREATE EXTENSION postgis;


# Get table columns and types
\d table


# Delete all rows
DELETE FROM table;

# Create spatial index on table
CREATE INDEX index_name ON table USING GIST(geometry);

# Output to file
COPY (SELECT columns FROM table ORDER BY column DESC LIMIT 1) TO 'path\to\write\to.txt' (format [format], delimiter '[delimiter]')
COPY (SELECT id1, id2 FROM candidate_pairs ORDER BY ovlp_perc DESC LIMIT 1) TO 'C:\temp\test_psql_out.txt' (format text, delimiter ',')
https://www.postgresql.org/docs/current/sql-copy.html

# Select based on intersection with WKT
SELECT pairname FROM scene_pair WHERE ST_Intersects(ST_SetSRID('POLYGON ((-147.0002982248624 64.50004262964862, -147.0002982248624 64.00002147079246, -147.0002982248624 63.99852285425598, -147.5000093251248 63.99852285425598, -148.0000046457648 63.99852285425598, -148.0002888661425 63.99852285425598, -148.0002888661425 64.00002147079246, -148.0002888661425 64.50004262964862, -148.0002888661425 64.99846181910374, -148.0000046457648 64.99846181910374, -147.5000093251248 64.99846181910374, -147.0002982248624 64.99846181910374, -147.0002982248624 64.50004262964862))'::geometry, 4326), ovlp_geom)