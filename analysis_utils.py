# -*- coding: utf-8 -*-
"""
Created on Tue May 28 13:05:51 2019

@author: disbr007
"""
import subprocess, os
import numpy as np

from shapely.geometry import Point, Polygon
import geopandas as gpd

from logging_utils.logging_utils import create_logger

## Set up logger
logger = create_logger(__name__, 'sh', 'INFO')


def run_subprocess(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    output, error = proc.communicate()


def grid_aoi(aoi_path, n_pts_x=None, n_pts_y=None,
             x_space=None, y_space=None, aoi_crs=None):
    # Read in AOI - assumes only one feature
    logger.debug('Creating grid in AOI...')
    if isinstance(aoi_path, gpd.GeoDataFrame):
        aoi_all = aoi_path
    elif isinstance(aoi_path, Polygon):
        aoi_all = gpd.GeoDataFrame(geometry=[aoi_path], crs=aoi_crs)
    else:
        if os.path.exists(aoi_path):
            aoi_all = gpd.read_file(aoi_path)
        else:
            logger.error('Could not locate: {}'.format(aoi_path))

    # Get first feature - should be only feature
    aoi = aoi_all.iloc[:1]

    # Get aoi bounding box
    minx, miny, maxx, maxy = aoi.geometry.bounds.values[0]
    x_range = maxx - minx
    y_range = maxy - miny
    # Determine spacing
    if x_space and y_space:
        logger.debug('Using provided spacing.')
    elif n_pts_x and n_pts_y:
        logger.debug('Determining spacing based on number of points requested.')
        x_space = x_range / n_pts_x
        y_space = y_range / n_pts_y
    logger.debug('Grid spacing\nx: {}\ny: {}'.format(round(x_space, 2), round(y_space, 2)))

    # Create x,y Point geometries
    x_pts = np.arange(minx, maxx, step=x_space)
    y_pts = np.arange(miny, maxy, step=y_space)
    mesh = np.array(np.meshgrid(x_pts, y_pts))
    xys = mesh.T.reshape(-1, 2)
    grid_points = [Point(x, y) for (x, y) in xys]

    # Make geodataframe of Point geometries
    grid = gpd.GeoDataFrame(geometry=grid_points, crs=aoi.crs)

    # Remove any grid points that do not fall in actual feature aoi_all
    grid['in'] = [pt.within(aoi.geometry.iloc[0]) for pt in grid.geometry]

    return grid[grid['in'] == True].drop(columns=['in'])


def get_count(geocells, fps, date_col=None):
    '''
    Gets the count of features in fps that intersect with each feature in geocells
    This method is essentially a many to many spatial join, so if two footprints
    overlaps a grid cell, there will be two of that grid cell in the resulting
    dataframe. These repeated cells are then counted and saved to the returned
    dataframe

    geocells: geodataframe of features to count within
    fps: geodataframe of polygons
    '''
    ## Confirm crs is the same
    logger.info('Counting footprints over each feature...')
    if geocells.crs != fps.crs:
        logger.info('Converting crs of grid to match footprint...')
        geocells = geocells.to_crs(fps.crs)

    logger.info('Performing spatial join...')
    ## Get a column from fps to use to test if sjoin found matches
    fp_col = fps.columns[1]
    sj = gpd.sjoin(geocells, fps, how='left', op='intersects')
    sj.index.names = ['count']
    sj.reset_index(inplace=True)

    logger.info('Getting count...')
    ## Remove no matches, group the rest, counting the index, and
    ## get minimum and maximum dates if requested
    agg = {'count': 'count'}
    if date_col:
        agg[date_col] = ['min', 'max']

    gb = sj[~sj[fp_col].isna()].groupby('count').agg(agg)
    ## Join geocells to dataframe with counts
    out = geocells.join(gb)

    out = gpd.GeoDataFrame(out, geometry=out.geometry, crs=geocells.crs)

    return out
