import argparse
import os

import pandas as pd
import geopandas as gpd

from lib.lib import write_gdf
from lib.db import Postgres, intersect_aoi_where
from lib.logging_utils import create_logger

# TODO: See if this can be done on scenes2index table, to facilitate ordering only
#  scenes2index that meet multilook specifications. As written all overlappng scenes2index
#  would have to exist in the scenes_onhand table already to be considered for
#  selection. The challenge is not keying off of the filename field when
#  determining pairs. Another possibility is to make this an arugment:
#  select only all-onhand pairs or select from scenes2index table and determine
#  what is onhand and what needs to be ordered.

# TODO: When locating onhand scenes2index, ensure there is only one, as fn_pairname
#  is based on id

logger = create_logger(__name__, 'sh', 'INFO')

# Constants
ml_mv = 'multilook_candidates'
ml_mv_geom = 'geometry'
scenes = 'scenes2index'
scenes_onhand = 'scenes_onhand'
s_id = 'id'
so_id = 'id'
so_geom = 'geometry'
ml_id = 'src_id'
ml_pairname = 'pairname'
fn_id = 'fn_id'
fn_pn = 'fn_pairname'
PAIR_IDS = 'pair_ids'
COUNT_FLD = 'ct'

# Global Equal Area projection for area calculation
eckertIV = r'+proj=eck4 +lon_0=0 +datum=WGS84 +units=m +no_defs'

# Default arguments
DEF_MIN_AREA = 32_670_000  # meters^2
DEF_MIN_PAIRS = 3


def get_src_gdf(src_id, db_src):
    """Loads record(s) from db_src where "id" matches src_id. If "id" is
    unique, this should be one record."""
    sql = "SELECT * FROM {} WHERE {} = '{}'".format(scenes_onhand, so_id,
                                                    src_id)
    src = db_src.sql2gdf(sql_str=sql)
    return src


def get_multilook_pair_gdf(ids, db_src, geom_col=so_geom):
    """Loads each scene in list of ids as GeoDataFrame"""
    pairs_sql = "SELECT * FROM {} WHERE {} IN ({})".format(scenes_onhand,
                                                           s_id,
                                                           str(ids)[1:-1])
    pairs = db_src.sql2gdf(sql_str=pairs_sql, geom_col=geom_col)

    return pairs


def get_ids_from_multilook(df, src_id_fld, pairname_fld):
    src_ids = list(df[src_id_fld].values)
    pair_ids = [i for sublist in df[pairname_fld].str.split('-').values
                for i in sublist]
    all_ids = set(src_ids + pair_ids)

    return all_ids


def all_pairs_oh(pairname, oh_ids):
    pair_ids = pairname.split('-')
    oh_pair_ids = set(pair_ids).intersection(oh_ids)

    if len(oh_pair_ids) != pair_ids:
        return False
    else:
        return True


def multilook_intersections(src_id, pair_ids, footprints,
                            min_pairs=DEF_MIN_PAIRS,
                            min_area=DEF_MIN_AREA):
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

    Parameters
    ---------
    src : str
        ID of src footprint, must be in footprints['id']
    pairs : list
        IDs of all pairs to src, must all be in footprints['id']
    footprints : gpd.GeoDataframe
        GeoDataFrame of all footprints, used to get each scenes2index geometry for
        computing intersections
    min_pairs : int
        The minimum number of pairs required to be kept
    min_area : float
        The minimum area to be kept, in units of crs.
    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame of all intersections meeting criteria.
    # TODO: Recompute the intersections of all others with the current
        intersection. Currently, intersections are sorted by intersection area
        with the source footprint. There could be scenarios where the next
        largest overlap with the current intersection is different from the
        next largest overlap with the source footprint. Implementing this
        would however lead to significant increase in processing time, as
        rather than computing a single intersection at each step, intersections
        would have to be computed for all footprints
        If implemented, the flow would be:
        1. First and second overlap greater than min-area = intersection 1
        2. Recompute intersections of ALL other footprints with intersection 1
        3. Order by largest overlap area of these new intersections
        4. Test if largest overlap area results in new intersection > min_area
        5. Return to 1. Repeat until all pairs tested or
            new intersection < min_area
    """
    # Get GeoDataFrames of footprint for src_id and pairs
    src = footprints[footprints['id'] == src_id]
    pairs = footprints[footprints['id'].isin(pair_ids)]
    if len(src) == 0:
        logger.error('Source footprint not found.')
        return
    elif len(pairs) == 0:
        # TODO: sort this out - need to only work on rows where at least
        #  min_pairs are onhand
        # logger.warning('No pairs onhand for source ID: {}'.format(src_id))
        return
    # else:
    #     logger.info('Pairs found, proceeding..')

    # Find initial intersections between src footprint and all pairs
    intersections = gpd.overlay(src, pairs)
    intersections['area'] = intersections.geometry.area

    # Drop any intersections that area already smaller than min_area
    # TODO: Do this at the database creation level, if a standard min_area can
    #  be set
    intersections = intersections[intersections['area'] > min_area]

    # Sort by area
    intersections.sort_values(by='area', ascending=False, inplace=True)
    intersections.rename(columns={'id_2': 'other_id',
                                  'fn_id_2': 'other_fn_id'},
                         inplace=True)

    # Keep only relevant columns
    intersections = intersections[['other_id', 'other_fn_id', 'geometry']]

    # Initialize database to store pairs that meet criteria for this ID. This
    # dataframe will have repeats for each source ID, for each group of IDs
    # that meet criteria. E.g.:
    # ID -- pairname
    # 1  -- 1-2-3
    # 1  -- 1-2-3-4
    # 1  -- 1-2-3-4-5
    multilook_pairs = gpd.GeoDataFrame()

    # Start with src footprint
    prev_intersect = src

    # Initialize pairname field to be extended for each added footprint
    prev_intersect['pairname'] = prev_intersect['id']
    prev_intersect[fn_pn] = prev_intersect['fn_id']

    # Iterate over all intersections with the source footprint and determine if
    # they meet the minimum size criteria.
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

        # Calculate area of new intersection (intersection of all previous)
        sub_intersect['area'] = sub_intersect.geometry.area

        # Check if min area has been met
        if sub_intersect['area'].values[0] < min_area:
            # logger.debug('Minimum area criteria not met, moving to next '
            #              'source ID.')
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


def get_multilook_pairs(min_pairs=DEF_MIN_PAIRS, min_area=DEF_MIN_AREA,
                        aoi=None, onhand=True):
    """Loads all records in multilook_candidates table and determines
    if each combination of src_id and overlapping pair meets minimum
    number of pairs and minimum area requirements. See
    multilook_intersections for details."""
    logger.info('Loading multilook candidate groups from: '
                'planet.{}'.format(ml_mv))
    sql = "SELECT * FROM {}".format(ml_mv)
    sql += " WHERE {} > {}".format(COUNT_FLD, min_pairs)
    if aoi:
        logger.info('Loading AOI...')
        aoi_gdf = gpd.read_file(aoi)
        aoi_where = intersect_aoi_where(aoi_gdf, geom_col=ml_mv_geom)
        sql += ' AND {}'.format(aoi_where)

    with Postgres() as db_src:
        # logger.info('Loading multilook candidates...')
        df = db_src.sql2df(sql_str=sql)

    logger.info('Records loaded: {:,}'.format(len(df)))

    df[PAIR_IDS] = df.pairname.str.split('-')
    with Postgres() as db_src:
        logger.info('Loading source footprints for all IDs found in '
                    'multilook table, including pairnames...')
        all_ids = get_ids_from_multilook(df, src_id_fld='src_id',
                                         pairname_fld='pairname')
        logger.info('IDs found: {:,}'.format(len(all_ids)))
        footprints = get_multilook_pair_gdf(all_ids, db_src)
        logger.info('Footprints loaded: {:,}'.format(len(footprints)))

        # If onhand only, get onhand IDs while connected to database
        if onhand:
            logger.info('Keeping only onhand IDs including those in '
                        'pairnames...')
            oh_ids = db_src.get_values(scenes_onhand, so_id, distinct=True)
            # Drop records where source ID isn't onhand
            logger.debug('Dropping records with not-onhand source IDs...')
            df = df[df[ml_id].isin(oh_ids)]
            # Drop records where all IDs aren't onhand
            # logger.debug('Dropping records with not-onhand pair IDs...')
            # df = df[df[PAIR_IDS].apply(lambda x: all(pair_id in oh_ids
            #                                          for pair_id in x))]

    # Add filename column
    footprints[fn_id] = footprints['filename'].apply(lambda x: x[:-4])

    # Convert to equal area crs
    logger.debug('Converting to equal area crs: {}'.format(eckertIV))
    footprints = footprints.to_crs(eckertIV)

    # Ensure records remain
    if len(df) == 0:
        logger.info('No remaining pairs, exiting.')
        sys.exit(-1)

    logger.info('Finding multilook pairs meeting thresholds:\nMin. '
                'Pairs: {}\n'
                'Min. Area: {:,.2f}'.format(min_pairs, min_area))
    df['multilook_pairs'] = df.apply(
        lambda x: multilook_intersections(x[ml_id],
                                          x[PAIR_IDS],
                                          footprints=footprints,
                                          min_pairs=min_pairs,
                                          min_area=min_area),
        axis=1)

    logger.info('Merging multilook pair records into single dataframe...')
    multilook_pairs = pd.concat(df['multilook_pairs'].values)
    if len(multilook_pairs) == 0:
        logger.info('No multilook pairs found, exiting.')
        sys.exit(-1)
    # Reproject back to WGS84
    logger.debug('Reprojecting back to EPSG:4326...')
    multilook_pairs = multilook_pairs.to_crs('epsg:4326')

    logger.info('Total multilook pairs found: '
                '{:,}'.format(len(multilook_pairs)))

    return multilook_pairs


def multilook_selection(out_ids: str,
                        out_overlaps: str = None,
                        min_pairs: int = DEF_MIN_PAIRS,
                        min_area: int = DEF_MIN_AREA,
                        aoi: str = None
                        ):

    logger.info('Searching for multilook pairs meeting thresholds:\n'
                'Min. Pairs: {:,}\n'
                'Min. Area: {:,}'.format(min_pairs, min_area))
    multilook_pairs = get_multilook_pairs(min_pairs=min_pairs,
                                          min_area=min_area,
                                          aoi=aoi)

    logger.info('Writing multilook pairs to file: '
                '{}'.format(out_overlaps))
    write_gdf(multilook_pairs, out_overlaps)

    if out_ids:
        logger.info('Writing IDs to file: {}'.format(out_ids))
        # TODO: Confirm the ID field is correct (should it be 'src_id'?)
        all_ids = get_ids_from_multilook(multilook_pairs,
                                         src_id_fld=so_id,
                                         pairname_fld=ml_pairname)
        with open(out_ids, 'w') as dst:
            for each_id in all_ids:
                dst.write(each_id)
                dst.write('\n')

    logger.info('Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Locate multilook 'pairs' that meet the provided minimum "
                    "criteria: --min_pairs and --min_area."
    )
    parser.add_argument('-oi', '--out_ids', type=os.path.abspath,
                        required=True,
                        help='Path to write text file of all IDs in multilook '
                             'footprint.')
    parser.add_argument('-o', '--out_overlaps', type=os.path.abspath,
                        help='Path to write multilook footprints to.')

    parser.add_argument('--aoi', type=os.path.abspath,
                        help='Path to AOI polygon to select pairs with.')
    parser.add_argument('-mp', '--min_pairs', type=int,
                        default=DEF_MIN_PAIRS)
    parser.add_argument('-ma', '--min_area', type=float,
                        default=DEF_MIN_AREA)

    # import sys
    # sys.argv = [__file__,
    #             "-oi", r'C:\temp\mlids.txt',
    #             "-mp", "100"]

    args = parser.parse_args()

    min_pairs = args.min_pairs
    min_area = args.min_area
    out_ids = args.out_ids
    out_overlaps = args.out_overlaps
    aoi = args.aoi

    multilook_selection(out_ids=out_ids,
                        out_overlaps=out_overlaps,
                        min_pairs=min_pairs,
                        min_area=min_area,
                        aoi=aoi)
