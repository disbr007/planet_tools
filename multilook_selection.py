import argparse
import os

import pandas as pd
import geopandas as gpd

from lib.lib import write_gdf
from lib.lib import Postgres
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')
# TODO: Fix this location
logger = create_logger(__name__, 'fh', 'DEBUG',
                       filename=r'E:\disbr007\projects\planet\logs\multilook.log')

db = 'sandwich-pool.planet'
ml_mv = 'multilook_candidates'
scenes_onhand = 'scenes_onhand'
so_id = 'id'
so_geom = 'geom'
fn_id = 'fn_id'
fn_pn = 'fn_pairname'
eckertIV = r'+proj=eck4 +lon_0=0 +datum=WGS84 +units=m +no_defs'
min_area = 32_670_000  # meters^2
min_pairs = 3


def get_src_gdf(src_id, db_src):
    """Loads record(s) from db_src where "id" matches src_id. If "id" is
    unique, this should be one record."""
    sql = "SELECT * FROM {} WHERE {} = '{}'".format(scenes_onhand, so_id,
                                                    src_id)
    src = db_src.sql2gdf(sql_str=sql)
    return src


def get_multilook_pair_gdf(ids, db_src, geom_col=so_geom):
    """Loads one record per pair in list of ids from db_src"""
    pairs_sql = "SELECT * FROM {} WHERE {} IN ({})".format(scenes_onhand,
                                                           so_id,
                                                           str(ids)[1:-1])
    pairs = db_src.sql2gdf(sql_str=pairs_sql, geom_col=geom_col)

    return pairs


def get_ids_from_multilook(df, src_id_fld, pairname_fld):
    src_ids = list(df[src_id_fld].values)
    pair_ids = [i for sublist in df[pairname_fld].str.split('-').values
                for i in sublist]
    all_ids = set(src_ids + pair_ids)

    return all_ids


def multilook_intersections(src_id, pair_ids, footprints, min_pairs=3,
                            min_area=min_area):
    """Takes a source ID of one footprint and pair ids of all other
    footprints that overlap it. Computes the intersections of each pair
    of src-other and sorts by largest intersection. Starting with the
    largest area intersection, each subsequent intersection is tested to
     see if it meets the minimum area threshold. If it does, the
     intersection of the first intersection and next intersection is
     used for the next iteration.
    As soon as the minimum number of pairs is reached, each intersection
    is saved with a unique pairname (id1-id2-id3-...). The iteration
    continues until either all pairs have been tested, or an
    intersection is attempted that results in an area less than the
    minimum area. The geodataframe of all intersections meeting the
    provided criteria is returned.
    # TODO: Recompute the intersections of all others with the current
        intersection. For example:
        1. First and second overlap greater than min-area = intersection 1
        2. Recompute intersections of all other footprints with intersection 1
        3. Order by largest overlap area of these new intersections
        4. Test if largest overlap area results in new intersection > min_area
        5. Return to 1. Repeat until all pairs tested or
            new intersection < min_area
    Parameters
    ---------
    src : str
        ID of src footprint, must be in footprints['id']
    pairs : list
        IDs of all pairs to src, must all be in footprints['id']
    footprints : gpd.GeoDataframe
        GeoDataFrame of all footprints
    min_pairs : int
        The minimum number of pairs required to be kept
    min_area : float
        The minimum area to be kept, in units of crs.
    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame of all intersections meeting criteria.
    """
    # Get GeoDataFrames of footprint for src_id and pairs
    src = footprints[footprints['id'] == src_id]
    pairs = footprints[footprints['id'].isin(pair_ids)]

    # Find initial intersections between src footprint and all pairs
    intersections = gpd.overlay(src, pairs)
    intersections['area'] = intersections.geometry.area
    # Drop any intersections that area already smaller than min_area
    intersections = intersections[intersections['area'] > min_area]
    # Sort by area
    intersections.sort_values(by='area', ascending=False, inplace=True)
    intersections.rename(columns={'id_2': 'other_id',
                                  'fn_id_2': 'other_fn_id'},
                         inplace=True)
    intersections = intersections[['other_id', 'other_fn_id', 'geometry']]

    multilook_pairs = gpd.GeoDataFrame()
    # Start with src footprint
    prev_intersect = src
    # Initialize pairname field to be extended for each added footprint
    prev_intersect['pairname'] = prev_intersect['id']
    prev_intersect[fn_pn] = prev_intersect['fn_id']
    for row in intersections.itertuples(index=False):
        # Find intersection of previous intersection and current footprint
        sub_intersect = gpd.overlay(prev_intersect,
                                    gpd.GeoDataFrame([row],
                                                     geometry='geometry',
                                                     crs=src.crs))
        # Check for any matches
        if sub_intersect.empty:
            logger.debug('No new intersection found, moving to next source ID.')
            break

        # Calculate area of new intersection
        sub_intersect['area'] = sub_intersect.geometry.area

        # Check if min area has been met
        if sub_intersect['area'].values[0] < min_area:
            logger.debug('Minimum area criteria not met, moving to next '
                         'source ID.')
            break

        # Add new ID to pairname
        sub_intersect['pairname'] = sub_intersect['pairname'] + '-' + sub_intersect['other_id']
        sub_intersect[fn_pn] = sub_intersect[fn_pn] + '-' + sub_intersect['other_fn_id']
        sub_intersect.drop(columns=['other_id', 'other_fn_id'], inplace=True)
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
        df = db_src.sql2df(sql_str=sql)
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

    # Add filename_pairname column
    footprints[fn_id] = footprints['filename'].apply(lambda x: x[:-4])

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
    # parser.add_argument('-fn_pn', '--filename_pairname', action='store_true',
    #                     help='Use to create a new field in the output footprint '
    #                          'that is the pairname, but using filenames instead of '
    #                          'ids: E.g.:'
    #                          'Pairname: 20200618_152012_104e-20200623_152150_1020-20200623_152150_1020'
    #                          'Filename_pairname: ')
    #
    args = parser.parse_args()

    import sys
    sys.argv = [r'C:\code\planet_stereo\multilook_selection.py',
                '-o',
                '2020oct14_multilook_redo.geojson',
                '-mp', '3', '-ma', '30000000', '-fn_pn']

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
