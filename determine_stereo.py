from copy import deepcopy
import datetime
import os
import matplotlib.pyplot as plt
import numpy as np
from pprint import pprint

import pandas as pd
import geopandas as gpd
import shapely
from tqdm import tqdm

from logging_utils.logging_utils import create_logger


logger = create_logger(__name__, 'sh', 'DEBUG')

# # Args
# scenes_p = r'V:\pgc\data\scratch\jeff\projects\planet\scenes\n65w148\n65w148_2020_scenes.shp'
# days_threshold = 10
# ovlp_metric = 'percent'

# Constants
# Preexisting fields
fld_ins = 'instrument' # Name of field holding instrument code
fld_ins_name = 'instrument_name' # Name of field holding instrument name ('Dove-R', etc.)
fld_sid = 'strip_id'
fld_iid = 'id'
fld_acquired = 'acquired'
fld_geometry = 'geometry'
fld_epsg = 'epsg_code'
# Created fields
fld_area = 'utm_area'
fld_sqkm = 'utm_sqkm'
fld_ovlps = 'ovlps'
fld_pairs = 'pairs'
fld_pair_id = 'pair_id'
fld_ovlp_geom = 'ovlp_geom'
fld_ovlp_area = 'ovlp_area'
fld_ovlp_perc = 'ovlp_perc'
fld_avg_ovlp_area = 'avg_ovlp_area'
fld_avg_ovlp_perc = 'avg_ovlp_perc'
fld_days_window = 'days_window'
fld_min_date = 'min_date'
fld_max_date = 'max_date'
date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
date_format_alt = '%Y-%m-%dT%H:%M:%S'
date_window_format = '%Y-%m-%d'
rsuffix = '2' # suffix to add to 'right' field names


def datetime_cols_to_str(df):
    '''Convert any datetime column in df to a string'''
    df[df.select_dtypes(['datetime']).columns] = df[df.select_dtypes(['datetime']).columns].astype(str)


def days_window_to_str(date_window, date_format=date_window_format):
    """Convert tuple of two datetimes to str"""
    dw_str = '{} - {}'.format(date_window[0].strftime(date_format),
                              date_window[1].strftime(date_format))

    return dw_str


def get_overlap_perc(geom1, geom2):
    """Get overlap perctange from two shapely geometries"""
    g_intersection = geom1.intersection(geom2)
    g_union = geom1.union(geom2)
    ovlp_perc = round(g_intersection.area / g_union.area, 4) * 100

    return ovlp_perc


def get_instrument_names(instrument):
    """Get instrument name from code"""
    platform_lut = {'PS2': 'Dove',
                    'PS2.SD': 'Dove-R',
                    'PSB.SD': 'SuperDove'}

    return platform_lut[instrument]


def get_within_strip(scenes, sid, fld_sid=fld_sid):
    """Get only scenes with the same strip ID as sid"""
    strip_scenes = scenes[scenes[fld_sid] == sid]

    return strip_scenes


def create_days_window(date, days_threshold):
    """Create tuple of date-days_threshold and date+days_threshold"""
    min_date = date + datetime.timedelta(days=-days_threshold)
    max_date = date + datetime.timedelta(days=days_threshold)

    return (min_date, max_date)


def get_within_instrument(scenes, ins, fld_ins=fld_ins):
    """Get only scenes from the same instrument"""
    ins_scenes = scenes[scenes[fld_ins] == ins]

    return ins_scenes


def get_within_date_range(scenes, days_window, fld_acquired=fld_acquired):
    """Subset a df to only those within date range (min_date - max_date)"""
    # logger.debug('Scene date: {}'.format(scenes[fld_acquired]))
    # logger.debug('Days window: {}'.format(days_window))
    date_scenes = scenes[(scenes[fld_acquired] > days_window[0]) &
                         (scenes[fld_acquired] < days_window[1])]

    return date_scenes


def get_pairs(row, scenes, percent_overlap=False,
              within_strip=False, within_days=False, within_ins=False):
    """Finds the pairs for a given row, optionally, within its own strip,
    within a number of days, and/or within the same instrument"""
    # TODO: Handle no overlaps better
    iid = row[fld_iid]
    # Exlude self
    sid_scenes = scenes[(scenes[fld_iid] != iid)]

    # Subset scenes with initial parameters
    if within_strip:
        sid_scenes = get_within_strip(scenes=sid_scenes, sid=row[fld_sid])
    if within_ins:
        sid_scenes = get_within_instrument(scenes=sid_scenes, ins=row[fld_ins])
    if within_days:
        # Conversion of fld_acquired and fld_min/max_date to datetime expected previously
        sid_scenes = get_within_date_range(scenes=sid_scenes, days_window=row[fld_days_window])

    ovlps = {}
    # If potential pairs meet above (within strip and/or within date range)
    if len(sid_scenes) != 0:
        scene_gdf = gpd.GeoDataFrame([row], geometry='geometry', crs=scenes.crs)
        scene_ovl = gpd.overlay(scene_gdf, sid_scenes, how='intersection')

        # If overlaps found calculate overlap area or pecent
        if not len(scene_ovl) == 0:
            # Create dict of id: overlap area or percent
            if percent_overlap:
                for i, r in scene_ovl.iterrows():
                    ovlps[r['{}_2'.format(fld_iid)]] = {fld_ovlp_geom: r.geometry,
                                                        fld_ovlp_perc: get_overlap_perc(row.geometry, r.geometry)}
            else:
                for i, r in scene_ovl.iterrows():
                    ovlps[r['{}_2'.format(fld_iid)]] = {fld_ovlp_geom: r.geometry,
                                                        fld_ovlp_area: r.geometry.area}
        else:
            pass
            # logger.debug('No pair scenes found that overlap scene footprint.')
    else:
        pass
        # logger.debug('No pair scenes found matching inital criteria (date and/or w/in strip')

    return ovlps


def threshold_overlap(ovlp_dict, threshold_fld, threshold=None):
    """Remove any entries in a dictionary of overlaps where dict[threshold_fld] < threshold"""
    # Overlap dict may be empty if no pairs met initial params (date range, etc)
    if ovlp_dict:
        selected = {pair_id: subdict for pair_id, subdict in ovlp_dict.items() if subdict[threshold_fld] > threshold}
    else:
        selected = {}

    return selected


def select_by_overlap(scenes, days_threshold=None, percent_overlap=False, overlap_threshold=None,
                      within_strip=False, within_days=False, within_ins=False):
    """Finds the pairs for each scene in scenes, then selects only those above the given
    days_threshold and overlap_threshold"""
    if percent_overlap:
        threshold_fld = fld_ovlp_perc
    else:
        threshold_fld = fld_ovlp_area
    left_scenes = deepcopy(scenes)
    if days_threshold:
        within_days = True
        # Convert acquired field to datetime
        left_scenes[fld_acquired] = pd.to_datetime(left_scenes[fld_acquired], infer_datetime_format=True) # format='%Y-%m-%dT%H:%M:%S.%fZ')
        left_scenes[fld_days_window] = left_scenes[fld_acquired].apply(lambda x: create_days_window(x, days_threshold=days_threshold))

    left_scenes[fld_pairs] = left_scenes.apply(lambda x: get_pairs(x, scenes=left_scenes,
                                                                   within_strip=within_strip,
                                                                   within_days=within_days,
                                                                   within_ins=within_ins,
                                                                   percent_overlap=percent_overlap),
                                               axis=1)

    if len(left_scenes) > 0:
        # Keep only overlaps above threshold
        logger.debug('Thresholding overlap {}...'.format(threshold_fld))
        left_scenes[fld_pairs] = left_scenes[fld_pairs].apply(lambda x: threshold_overlap(x, threshold_fld=threshold_fld,
                                                                                          threshold=overlap_threshold))
        logger.debug('Removing scenes with no overlaps...')
        left_scenes = left_scenes[left_scenes[fld_pairs].map(lambda d: len(d.keys())) > 0]
        logger.debug('Remaining scenes with at least one pair above threshold: {}'.format(len(left_scenes)))

    # Convert days_window field
    datetime_cols_to_str(left_scenes)

    return left_scenes


def create_overlap_dataframe(left_scenes, src_scenes, fld_ovlp_metric):
    """Create a dataframe with one row per pair, with attributes from both pairs,
    the instersection as geometry, and the overlap (percentage or area)"""
    logger.debug('Creating overlaps dataframe...')
    ovlp_dicts = []
    # Loop over each scene
    for i, s in tqdm(left_scenes.iterrows(), total=len(left_scenes)):
        # Loop over each pair with the scene
        for pair_id, value in s[fld_pairs].items():
            # Copy source scene information - drop original geometry
            row_dict = s.drop([fld_geometry, fld_pairs]).to_dict()
            # row_dict = deepcopy(base_row_dict)
            # Copy pair scene attribute information
            match = src_scenes.loc[src_scenes[fld_iid] == pair_id].drop(columns=[fld_geometry])
            match.rename(columns={col: '{}{}'.format(col, rsuffix) for col in list(match)}, inplace=True)
            pair_atts = match.to_dict('r')[0]

            row_dict.update(pair_atts)

            # Add columns for overlap geometry and area
            row_dict[fld_ovlp_geom] = value[fld_ovlp_geom]
            row_dict[fld_ovlp_metric] = value[fld_ovlp_metric]
            ovlp_dicts.append(row_dict)

    # Create dataframe from rows (dicts)
    overlaps = gpd.GeoDataFrame.from_dict(ovlp_dicts)
    # Set CRS
    overlaps.crs = src_scenes.crs
    # Set geometry to overlap area
    overlaps.set_geometry(fld_ovlp_geom, inplace=True)
    # overlaps.drop(columns=fld_geometry, inplace=True)
    return overlaps


def determine_stereo(scenes_p, overlap_metric='area',
                     overlap_threshold=0, days_threshold=None,
                     within_days=False, within_strip=False, within_ins=False,
                     out_pairs_p=None):
    """Finds stereo pairs in a given scenes footprint, given an
    optional days threshold and overlap threshold """
    logger.info("Loading scenes footprint...")
    scenes = gpd.read_file(scenes_p)
    # scenes = scenes.iloc[0:250]

    logger.debug('Records loaded: {}'.format(len(scenes)))
    logger.info('Selecting features by overlap...')

    if overlap_metric == 'percent':
        percent_overlap = True
        threshold_fld = fld_ovlp_perc
    elif overlap_metric == 'area':
        percent_overlap = False
        threshold_fld = fld_ovlp_area
        # TODO: Fix area calculation -- calculate all areas ahead of time? (big refac.)
        utm_zone = int(scenes)[fld_epsg].mode().values[0]
        logger.debug('Using UTM zone: {}'.format(utm_zone))
        # Convert to UTM for area calulating
        if scenes.crs != 'epsg:{}'.format(utm_zone):
            scenes = scenes.to_crs('epsg:{}'.format(utm_zone))

    left_scenes = select_by_overlap(scenes=scenes, days_threshold=days_threshold,
                                    percent_overlap=percent_overlap, overlap_threshold=overlap_threshold,
                                    within_strip=within_strip, within_days=within_days, within_ins=within_ins)

    if len(left_scenes) > 0:
        stereo = create_overlap_dataframe(left_scenes=left_scenes, src_scenes=scenes, fld_ovlp_metric=threshold_fld)

        stereo[fld_days_window] = stereo[fld_days_window].apply(lambda x: days_window_to_str(x))

        if out_pairs_p:
            # TODO: Infer driver by extension
            stereo.to_file(out_pairs_p, driver='GeoJSON')
    else:
        stereo = None

    logger.info('Done.')

    return stereo

# st = determine_stereo(scenes_p=scenes_p, overlap_threshold=0, within_ins=True,
#                       within_days=True, days_threshold=5)