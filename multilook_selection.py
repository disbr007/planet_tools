import argparse
import os

import pandas as pd
import geopandas as gpd

import matplotlib.pyplot as plt

from lib import write_gdf
from db_utils import Postgres
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')
logger = create_logger(__name__, 'fh', 'DEBUG', filename=r'E:\disbr007\projects\planet\logs\multilook.log')

db = 'sandwich-pool.planet'
ml_mv = 'multilook_candidates'
scenes_onhand = 'scenes_onhand'
so_id = 'id'
eckertIV = r'+proj=eck4 +lon_0=0 +datum=WGS84 +units=m +no_defs'
min_area = 32_670_000  # meters^2
min_pairs = 3


def get_src_gdf(src_id, db_src):
    """Loads a single record from db_src where id matches src_id"""
    sql = "SELECT * FROM {} WHERE {} = '{}'".format(scenes_onhand, so_id, src_id)
    src = db_src.sql2gdf(sql=sql)
    return src


def get_multilook_pair_gdf(ids, db_src):
    """Loads one records per pair in list of ids from db_src"""
    pairs_sql = "SELECT * FROM {} WHERE {} IN ({})".format(scenes_onhand, so_id, str(ids)[1:-1])
    pairs = db_src.sql2gdf(sql=pairs_sql)
    return pairs


def get_ids_from_multilook(df, src_id_fld, pairname_fld):
    src_ids = list(df[src_id_fld].values)
    pair_ids = [i for sublist in df[pairname_fld].str.split('-').values for i in sublist]
    all_ids = set(src_ids + pair_ids)

    return all_ids


def multilook_intersections(src_id, pair_ids, footprints, min_pairs=3, min_area=min_area):
    """Takes a source geodataframe of one footprint and a pairs geodataframe
    of other footprints that overlap it. Computes the intersections of each
    pair of src-other and sorts by largest intersection. Starting with the
    largest area intersection, each subsequent intersection is tested to see
    if it meets the minimum area threshold. If it does, the intersection of
    the first intersection and next intersection is used for the next iteration.
    As soon as the minimum number of pairs is found, each intersection is saved
    with a unique pairname (id1-id2-id3-...). The iteration continues until
    either all pairs have been tested, or an intersection is attempted that
    results in an area less than the minimum area. The geodataframe of all
    intersections meeting the provided criteria is returned.

    Parameters
    ---------
    src : gpd.GeoDataFrame
        GeoDataFrame with one record, the source footprint
    pairs : gpd.GeoDataFrame
        GeoDataFrame of intersecting footprints with source footprint
    footprints : gpd.GeoDataframe
        GeoDataFrame of all footprints
    min_pairs : int
        The minimum number of pairs required to be kept
    min_area : float
        The minimum area to be kept, in units of crs.
    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame of all intersections meeting criteria."""
    src = footprints[footprints['id'] == src_id]
    pairs = footprints[footprints['id'].isin(pair_ids)]

    # Find initial intersections between src footprint and pairs
    intersections = gpd.overlay(src, pairs)
    intersections['area'] = intersections.geometry.area
    # Drop any intersections that area already smaller than min_area
    intersections = intersections[intersections['area'] > min_area]
    # Sort by area
    intersections.sort_values(by='area', ascending=False, inplace=True)
    intersections.rename(columns={'id_2': 'other_id'}, inplace=True)
    intersections = intersections[['other_id', 'geometry']]

    multilook_pairs = gpd.GeoDataFrame()
    # Start with src footprint
    prev_intersect = src
    # Initialize pairname field to be extended for each added footprint
    prev_intersect['pairname'] = prev_intersect['id']
    for row in intersections.itertuples(index=False):
        # Find intersection of previous intersection and current footprint
        sub_intersect = gpd.overlay(prev_intersect,
                                    gpd.GeoDataFrame([row], geometry='geometry', crs=src.crs))
        # Check for any matches
        if sub_intersect.empty:
            logger.debug('No new intersection found, moving to next source ID.')
            break

        # Calculate area of new intersection
        sub_intersect['area'] = sub_intersect.geometry.area

        # Check if min area has been met
        if sub_intersect['area'].values[0] < min_area:
            logger.debug('Minimum area criteria not met, moving to next source ID.')
            break

        # Add new ID to pairname
        sub_intersect['pairname'] = sub_intersect['pairname'] + '-' + sub_intersect['other_id']
        sub_intersect.drop(columns=['other_id'], inplace=True)
        sub_intersect['pair_count'] = sub_intersect['pairname'].apply(lambda x: len(x.split('-')))
        if sub_intersect['pair_count'].values[0] >= min_pairs:
            multilook_pairs = pd.concat([sub_intersect, multilook_pairs])

        prev_intersect = sub_intersect

    return multilook_pairs


def get_multilook_pairs(min_pairs=min_pairs, min_area=min_area):
    """Loads all records in multilook_candidates table and determines
    if each combination of src_id and overlapping pair meets minimum
    number of pairs and minimum area requirements. See
    multilook_intersections for details."""
    logger.info('Loading multilook candidates from: {}'.format(ml_mv))
    # TODO: Add WHERE pairname NOT IN [multilook_pairs]
    sql = "SELECT * FROM {}".format(ml_mv)
    # sql += " LIMIT 10"
    with Postgres(db) as db_src:
        df = db_src.sql2df(sql=sql)
        db_src = None

    logger.info('Records loaded: {:,}'.format(len(df)))

    df['pair_ids'] = df.pairname.str.split('-')
    with Postgres(db) as db_src:
        logger.info('Loading footprints for all IDs found in multilook source IDs and pairnames.')
        all_ids = get_ids_from_multilook(df, src_id_fld='src_id', pairname_fld='pairname')
        # src_ids = list(df['src_id'].values)
        # pair_ids = [i for sublist in df['pair_ids'].values for i in sublist]
        # all_ids = set(src_ids + pair_ids)
        footprints = get_multilook_pair_gdf(all_ids, db_src)
        logger.info('Footprints loaded: {:,}'.format(len(footprints)))

        db_src = None

    # Convert to equal area crs
    logger.debug('Converting to equal area crs: {}'.format(eckertIV))
    footprints = footprints.to_crs(eckertIV)


    logger.info('Finding multilook pairs meeting thresholds:\nMin. Pairs: {}\nMin. Area: {:,}'.format(min_pairs, min_area))
    df['multilook_pairs'] = df.apply(lambda x: multilook_intersections(x['src_id'],
                                                                       x['pair_ids'],
                                                                       footprints=footprints,
                                                                       min_pairs=min_pairs,
                                                                       min_area=min_area), axis=1)
    logger.info('Merging multilook pair records into single dataframe...')
    multilook_pairs = pd.concat(df['multilook_pairs'].values)
    logger.info('Total multilook pairs found: {:,}'.format(len(multilook_pairs)))

    return multilook_pairs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-o', '--out_multilook_fp', type=os.path.abspath,
                        help='Path to write multilook footprints to.')
    parser.add_argument('-oi', '--out_ids', type=os.path.abspath,
                        help='Path to write text file of all IDs in multilook footprint.')
    parser.add_argument('-mp', '--min_pairs', type=int, default=min_pairs)
    parser.add_argument('-ma', '--min_area', type=float, default=min_area)
    
    args = parser.parse_args()

    # import sys
    # sys.argv = []

    logger.info('Searching for multilook pairs meeting '
                'thresholds:\nMin. Pairs: {}\nMin. Area: {:,}'.format(min_pairs, min_area))
    multilook_pairs = get_multilook_pairs(min_pairs=args.min_pairs, min_area=args.min_area)
    logger.info('Writing multilook pairs to file: {}'.format(args.out_multilook_fp))
    write_gdf(multilook_pairs, args.out_multilook_fp)

    if args.out_ids:
        logger.info('Writing IDs to file: {}'.format(args.out_ids))
        all_ids = get_ids_from_multilook(multilook_pairs, src_id_fld='id', pairname_fld='pairname')
        with open(args.out_ids, 'w') as dst:
            for each_id in all_ids:
                dst.write(each_id)
                dst.write('\n')
    logger.info('Done.')
