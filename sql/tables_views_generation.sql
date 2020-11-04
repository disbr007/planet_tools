/* Create empty scenes table to hold footprints from public API */
CREATE TABLE scenes (
    ogc_fid             SERIAL PRIMARY KEY,
    id                  varchar(30),
    strip_id            varchar(30),
    acquired            timestamp,
    satellite_id        varchar(25),
    instrument          varchar(10),
    provider            varchar(25),
    item_type           varchar(25),
    origin_x            real,
    origin_y            real,
    epsg_code           integer,
    cloud_cover         numeric(3, 2),      --3 total digits, 2 decimal places
    sun_azimuth         numeric(4, 1),
    sun_elevation       numeric(4, 1),
    view_angle          numeric(4, 2),
    columns             integer,
    rows                integer,
    pixel_resolution    real,
    gsd                 numeric(4, 2),
    anomalous_pixels    integer,
    ground_control      smallint,
    published           timestamp,
    quality_category    varchar(20),
    updated             timestamp,
    azimuth             double precision,
    off_nadir_signed    double precision,
    geometry            geometry(Polygon, 4326),
    UNIQUE (id, item_type)
);
CREATE INDEX scenes_geom_idx ON scenes USING GIST(geom);
SELECT COUNT(*) FROM scenes;
drop table scenes;
/* Add columns from off_nadir table */
UPDATE scenes
SET azimuth = off_nadir.sat_satellite_azimuth_mean,
    off_nadir_signed = off_nadir.sat_off_nadir
FROM off_nadir
WHERE off_nadir.scene_name = scenes_test.id;

select * from scenes_onhand;
/* Create view with off nadir and xml table joined to scenes - create
   off_nadir_signed column */
CREATE MATERIALIZED VIEW scenes_metadata AS
    SELECT s.*,
           o.sat_satellite_azimuth_mean as azimuth,
           o.sat_off_nadir as off_nadir_unsigned,
           x."orbitDirection" as orbitDirection,
           SIGN(o.sat_satellite_azimuth_mean)*o.sat_off_nadir as off_nadir_signed
           FROM scenes as s
INNER JOIN off_nadir as o ON s.id = o.scene_name
INNER JOIN xml_metadata as x ON s.id = SUBSTR(x.identifier, 0, 21);

/* Create xtrack table with all intersections within WHERE parameteres */
CREATE MATERIALIZED VIEW xtrack_cc20
    AS SELECT a.id || '-' || b.id AS pairname,
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
              ABS(a.off_nadir_signed - b.off_nadir_signed) AS off_nadir_diff,
              ABS(DATE_PART('day', a.acquired - b.acquired)) AS date_diff,
              ABS(a.azimuth - b.azimuth) as azimuth_diff,
              ST_Area(ST_INTERSECTION(a.geom, b.geom))
                  / ST_Area(ST_Union(a.geom, b.geom)) AS ovlp_perc,
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


/* Find only pairs that are both onhand, create field:pairname_fn using
   filenames w/o ext */
CREATE MATERIALIZED VIEW stereo_candidates_onhand AS
SELECT s.*,
       so1.filename as filename1,
       so2.filename as filename2,
       LEFT(so1.filename, LENGTH(so1.filename)-4) || '-' ||
       LEFT(so2.filename, LENGTH(so2.filename)-4) as pairname_fn
FROM stereo_candidates as s
INNER JOIN scenes_onhand as so1 ON s.id1 = so1.id
INNER JOIN scenes_onhand as so2 ON s.id2 = so2.id;

/* Create scenes on hand table */
CREATE TABLE scenes_onhand (
    ogc_fid                         SERIAL PRIMARY KEY,
    id                              varchar(30),
    identifier                      varchar(50),
    strip_id                        varchar(30),
    acquisitionDateTime             timestamp,
    bundle_type                     varchar(30),
    center_x                        numeric(8, 5),
    center_y                        numeric(8, 5),
    acquisitionType                 varchar(30),
    productType                     varchar(10),
    status                          varchar(30),
    versionIsd                      real,
    pixelFormat                     varchar(10),
    beginPosition                   timestamp,
    endPosition                     timestamp,
    sensorType                      varchar(30),
    resolution                      numeric(6, 4),
    scanType                        varchar(30),
    orbitDirection                  varchar(30),
    incidenceAngle                  real,
    illuminationAzimuthAngle        real,
    illuminationElevationAngle      real,
    azimuthAngle                    real,
    spaceCraftViewAngle             real,
    fileName                        varchar(75),
    productFormat                   varchar(30),
    resamplingKernel                varchar(30),
    numRows                         integer,
    numColumns                      integer,
    numbands                        integer,
    rowGsd                          numeric(10, 8),
    columnGsd                       numeric(10, 8),
    radiometricCorrectionApplied    varchar(30), --bool?
    geoCorrectionLevel              varchar(30),
    elevationcorrectionapplied      varchar(30),
    atmosphericCorrectionApplied    varchar(30), -- bool?
    platform                        varchar(30),
    serialIdentifier                varchar(30),
    orbitType                       varchar(30),
    instrument                      varchar(30),
    mask_type                       varchar(30),
    mask_format                     varchar(30),
    mask_referenceSystemIdentifier  varchar(30),
    mask_filename                   varchar(75),
    band1_radiometricScaleFactor    numeric(5, 3),
    band1_reflectanceCoefficient    real,
    band2_radiometricScaleFactor    numeric(5, 3),
    band2_reflectanceCoefficient    real,
    band3_radiometricScaleFactor    numeric(5, 3),
    band3_reflectanceCoefficient    real,
    band4_radiometricScaleFactor    numeric(5, 3),
    band4_reflectanceCoefficient    real,
    received_datetime               timestamp,
    shelved_loc                     varchar(300),
    geometry                        geometry(Polygon, 4326),
    centroid                        geometry(Point, 4326),
    UNIQUE (identifier)
    );
CREATE INDEX scenes_onhand_geometry_idx on scenes_onhand USING GIST(geometry);
CREATE INDEX scenes_onhand_centroid_idx on scenes_onhand USING GIST(centroid);

select * from scenes_onhand;
drop table scenes_onhand;
delete from scenes_onhand where 1=1;