/* Counting */
SELECT max(cloud_cover) FROM scenes;
SELECT off_nadir_signed FROM scenes_metadata LIMIT 10;
SELECT * FROM xtrack_cc20 LIMIT 10;
SELECT * FROM xml_metadata LIMIT 10;
SELECT SUBSTR(identifier, 0, 21) FROM xml_metadata LIMIT 10;
SELECT id FROM scenes_onhand LIMIT 10;
SELECT location from scenes_onhand LIMIT 10;
SELECT COUNT(acquired) FROM scenes WHERE cast(acquired as varchar(100)) LIKE '%-01-%';
SELECT EXTRACT(MONTH FROM acquired) FROM scenes;
SELECT * FROM scenes
WHERE EXTRACT(MONTH FROM acquired) = 12 AND
      EXTRACT(DAY FROM acquired) > 15
LIMIT 10;
SELECT COUNT(DISTINCT id) FROM scenes;
SELECT COUNT(*) FROM scenes;
SELECT COUNT(*) FROM scenes_onhand;
SELECT COUNT(*) FROM scenes_metadata;
SELECT COUNT(*) FROM xml_metadata;
SELECT COUNT(*) FROM off_nadir;
SELECT COUNT(*) FROM xtrack_cc20;
SELECT COUNT(*) FROM stereo_candidates;
SELECT COUNT(*) FROM stereo_candidates_onhand;
SELECT COUNT(DISTINCT id2) FROM stereo_candidates_onhand;
SELECT * FROM scenes where acquired >= '2020-01-01'::date;
SELECT * FROM scenes WHERE EXTRACT(MONTH FROM acquired) = 01;
SELECT COUNT(*) FROM scenes WHERE acquired < '2019-01-01'::date;


SELECT location from scenes_onhand WHERE id IN ('20170303_030803_0e0f',
                                                '20170303_030802_0e0f',
                                                '20170303_030801_0e0f',
                                                '20170303_030800_0e0f',
                                                '20170303_125219_0e20',
                                                '20170303_125218_0e20');

SELECT * FROM xml_metadata LIMIT 10;
SELECT * FROM scenes LIMIT 100;
SELECT DISTINCT order_id FROM scenes_onhand;
SELECT * FROM stereo_candidates_onhand LIMIT 10;
SELECT MIN(acquired) FROM scenes;

/* Count distinct occurences of values in column */
SELECT orbitDirection, COUNT(*) AS num
FROM scenes_metadata
GROUP BY orbitDirection;

/* Listing tables */
SELECT schemaname as schema_name,
       matviewname as view_name
       FROM pg_matviews;

/* Removing duplicates by ID */
DELETE FROM scenes_onhand a USING (
    SELECT MIN(CTID) as ctid, id
    FROM scenes_onhand
    GROUP BY id
    HAVING COUNT(*) > 1
    ) b
WHERE a.id = b.id AND a.ctid <> b.ctid;

DELETE FROM off_nadir a USING (
    SELECT MIN(CTID) as ctid, scene_name
    FROM off_nadir
    GROUP BY scene_name
    HAVING COUNT(*) > 1
    ) b
WHERE a.scene_name = b.scene_name AND a.ctid <> b.ctid;

/* Create view with off nadir and xml table joined to scenes - create off_nadir_signed column */
CREATE MATERIALIZED VIEW scenes_metadata AS
    SELECT s.*,
           o.sat_satellite_azimuth_mean as azimuth,
           o.sat_off_nadir as off_nadir_unsigned,
           x."orbitDirection" as orbitDirection,
           x.identifier as identifier,
           SIGN(o.sat_satellite_azimuth_mean)*o.sat_off_nadir as off_nadir_signed
           FROM scenes as s
INNER JOIN off_nadir as o ON s.id = o.scene_name
INNER JOIN xml_metadata as x ON s.id = SUBSTR(x.identifier, 0, 21);

DROP MATERIALIZED VIEW scenes_metadata;
SELECT off_nadir_unsigned, off_nadir_signed, azimuth FROM scenes_metadata LIMIT 100;
SELECT sat_off_nadir, sat_satellite_azimuth_mean,
       SIGN(o.sat_satellite_azimuth_mean)*o.sat_off_nadir as off_nadir_signed
FROM off_nadir as o;

/* Create table with overlaps */
/* TODO: multiple percent overlap * 100 */

CREATE MATERIALIZED VIEW xtrack_cc20
    AS SELECT a.id || '-' || b.id AS pairname_id,
              a.id AS id1, b.id AS id2,
              a.acquired AS acquired1,
              b.acquired AS acquired2,
              a.instrument AS instrument1,
              b.instrument AS instrument2,
              a.cloud_cover as cloud_cover1,
              b.cloud_cover as cloud_cover2,
              a.off_nadir_signed AS off_nadir1,
              b.off_nadir_signed AS off_nadir2,
              a.azimuth AS azimuth1,
              b.azimuth AS azimuth2,
              a.orbitDirection as orbitDirection1,
              b.orbitDirection as orbitDirection2,
              ABS(a.off_nadir_signed - b.off_nadir_signed) as off_nadir_diff,
              ABS(DATE_PART('day', a.acquired - b.acquired)) AS date_diff,
              ABS(a.azimuth - b.azimuth) as azimuth_diff,
              ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom))
                  AS ovlp_perc,
              ST_INTERSECTION(a.geom, b.geom) AS ovlp_geom
    FROM scenes_metadata AS a, scenes_metadata AS b
    WHERE a.id > b.id AND
          a.ground_control=1 AND
          b.ground_control=1 AND
          a.cloud_cover < 0.20 AND
          b.cloud_cover < 0.20 AND
          ABS(DATE_PART('day', a.acquired - b.acquired)) < 30 AND
          ST_Intersects(a.geom, b.geom);

/* Create candidates table with tighter parameters*/
CREATE MATERIALIZED VIEW stereo_candidates AS
SELECT *
FROM xtrack_cc20
WHERE off_nadir_diff > 5 AND
      cloud_cover1 < 10 AND
      cloud_cover2 < 10 AND
      date_diff < 10 AND
      ovlp_perc >= 0.30 AND
      ovlp_perc <= 0.70 AND
      orbitDirection1 = orbitDirection2;


CREATE UNIQUE INDEX idx_id_scenes on scenes(id);

/* Create onhand candidates table */
/* TODO: pairname_fn could come from xml_metadata upon creation of scenes_onhand */
CREATE MATERIALIZED VIEW stereo_candidates_onhand AS
SELECT s.*,
       so1.filename as filename1,
       so2.filename as filename2,
       LEFT(so1.filename, LENGTH(so1.filename)-4) || '-' ||
       LEFT(so2.filename, LENGTH(so2.filename)-4) as pairname_fn
FROM stereo_candidates as s
INNER JOIN scenes_onhand as so1 ON s.id1 = so1.id
INNER JOIN scenes_onhand as so2 ON s.id2 = so2.id;


/* Testing different parameters */
SELECT COUNT(*) FROM xtrack_cc20
WHERE ((ovlp_perc*100) / (off_nadir1 + off_nadir2)) * ((ovlp_perc*100) / 30) < 30 AND
      azimuth_diff < 50 AND
      cloud_cover1 < 0.10 AND
      cloud_cover2 < 0.10 AND
      date_diff < 10;

/* Get all IDs from candidates table */
SELECT DISTINCT sub.id as id FROM (
    SELECT id1 as id FROM stereo_candidates
    UNION
    SELECT id2 as id FROM stereo_candidates)
AS sub;

/* REFRESH materialized views */
REFRESH MATERIALIZED VIEW stereo_candidates;
REFRESH MATERIALIZED VIEW stereo_candidates_onhand;
select COUNT(*) from stereo_candidates_onhand;


/* DELETING THINGS */
/* Delete rows in scenes */
DELETE FROM scenes;

/* Delete columns */
ALTER TABLE;

/* Removing table */
DROP TABLE scenes_onhand CASCADE;
-- DROP TABLE off_nadir CASCADE;
DROP MATERIALIZED VIEW xtrack_cc20 CASCADE;
DROP MATERIALIZED VIEW scenes_off_nadir CASCADE;
DROP MATERIALIZED VIEW scenes_metadata CASCADE;

-- DROP MATERIALIZED VIEW VIEW scenes_off_nadir;
DROP MATERIALIZED VIEW stereo_candidates;

/* Get table sizes */
select t1.datname AS db_name,
       pg_size_pretty(pg_database_size(t1.datname)) as db_size
from pg_database t1
order by pg_database_size(t1.datname) desc


/* Get all intersections */
/* Use a grid of 1km by 1km, search for overlaps that cover at least x grid cells */
SELECT src_id, string_agg(int_id, '-') AS pairname, count(*) AS ct FROM (
    SELECT a.id as src_id,
           b.id AS int_id
    FROM scenes_onhand a, scenes_onhand b
    WHERE a.id < b.id AND
          a.cloud_cover < 0.20 AND
          b.cloud_cover < 0.20 AND
          ABS(DATE_PART('day', a.acquired - b.acquired)) < 10 AND
          ST_Intersects(a.geom, b.geom)
LIMIT 1000) all_int
GROUP BY 1
LIMIT 50;

CREATE MATERIALIZED VIEW multilook_candidates AS
SELECT src_id, string_agg(int_id, '-') AS pairname, count(*) AS ct FROM (
    SELECT a.id AS src_id,
           b.id AS int_id
    FROM scenes_metadata a, scenes_metadata b
    WHERE a.id < b.id AND
          a.cloud_cover < 0.20 AND
          b.cloud_cover < 0.20 AND
          ABS(a.off_nadir_signed - b.off_nadir_signed) > 5 AND
          ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) > 0.3 AND
          ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) < 0.7 AND
          ABS(DATE_PART('day', a.acquired - b.acquired)) < 10 AND
          ST_Intersects(a.geom, b.geom)
) all_int
GROUP BY 1;

CREATE MATERIALIZED VIEW multilook_candidates AS
SELECT src_id, string_agg(int_id, '-') AS pairname, count(*) AS ct FROM (
    SELECT a.id AS src_id,
           b.id AS int_id
    FROM scenes_metadata a, scenes_metadata b
    WHERE a.id < b.id AND
          a.cloud_cover < 0.20 AND
          b.cloud_cover < 0.20 AND
          ABS(a.off_nadir_signed - b.off_nadir_signed) > 5 AND
          ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) > 0.3 AND
          ST_Area(ST_INTERSECTION(a.geom, b.geom)) / ST_Area(ST_Union(a.geom, b.geom)) < 0.7 AND
          ABS(DATE_PART('day', a.acquired - b.acquired)) < 10 AND
          ST_Intersects(a.geom, b.geom)
LIMIT 100000000) all_int
GROUP BY 1
HAVING count(*) > 2;

SELECT * FROM multilook_candidates;
SELECT COUNT(*) FROM multilook_candidates;
DROP MATERIALIZED VIEW multilook_candidates CASCADE;

SELECT * FROM scenes_onhand
WHERE id IN ('20200623_152150_1020',
 '20200622_152252_0f36',
 '20200623_152150_1020',
 '20200623_171654_0e20');