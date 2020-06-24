# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 09:38:56 2020

@author: disbr007
"""

import argparse
import os

import geopandas as gpd

from logging_utils.logging_utils import create_logger


# INPUTS
# Requried
AOI_PATH = r'E:\disbr007\UserServicesRequests\Projects\akhan\aoi_pts.shp'

logger = create_logger(__name__, 'sh', 'INFO')


def open_datasource(x):
    if isinstance(x, gpd.GeoDataFrame):
        ds = x
    elif os.path.exists(x):
        try:
            ds = gpd.read_file(x)
        except Exception as e:
            logger.error('Unable to open: {}'.format(x))
            raise e
    else:
        logger.error("""Unknown input type {}. Should be OGR datasource that
                        geopandas can read.""".format(x))
    
    return ds


def locate_utm_zone(feature, utm_zones):
    """
    Locates the UTM zone of a single feature, 
    using the features centroid.

    Parameters
    ----------
    feature : shapely.geometry.object
        A single feature, point, polygon or line. The centroid
        is used.
    utm_zones : geopandas.GeoDataFrame
        The UTM zone polygons. Must have 'Zone_Hemi' field.

    Returns
    -------
    STR : UTM Zone hemisphere and number.

    """
    for i, zone in utm_zones.iterrows():
        if zone.geometry.contains(feature.centroid):
            matched_zone = zone['Zone_Hemi']
            break
        else:
            matched_zone = 'No_match'
    
    return matched_zone


def convert_UTM_epsg(utm_zone):
    """
    Converts a utm zone to it's WGS84 EPSG number

    Parameters
    ----------
    utm_zone : STR
        UTM zone in format #,hemisphere (eg. 10,n).

    Returns
    -------
    INT : EPSG.

    """
    number, hemisphere = utm_zone.split(',')
    if hemisphere.lower() == 'n':
        hemi_indicator = 6
    elif hemisphere.lower() == 's':
        hemi_indicator = 7
    else:
        logger.error('Unable to parse utm zone: {}'.format(utm_zone))
    
    epsg = int('32{}{}'.format(hemi_indicator, number))
    
    return epsg
        

def utm_zone_locater(aoi_in, utm_zones_in=None, aoi_out_path=None):
    """
    Locates the UTM zone for each feature in AOI.

    Parameters
    ----------
    aoi_in : os.path.abspath
        Path to the AOI shapefile.
    utm_zones_in : os.path.abspath
        Path to the UTM zones shapefile, if not 
        provided, default location is used.
    aoi_out_path : os.path.abspath
        Path to write aoi shapefile to, with added UTM zone field.
    
    Returns
    -------
    geopandas.GeoDataFrame : AOI with added UTM zone field.
    """
    # Name of field to create in AOI dataframe to hold UTM zone
    UTM_ZONE = 'UTM_ZONE' 
    EPSG = 'EPSG'

    if not utm_zones_in:
        utm_zones_in = r'E:\disbr007\general\UTM_Zone_Boundaries\UTM_Zone_Boundaries.shp'
    
    aoi = open_datasource(aoi_in)

    utm_zones = open_datasource(utm_zones_in)
    
    aoi[UTM_ZONE] = aoi.geometry.apply(lambda x: locate_utm_zone(x, utm_zones))
    aoi[EPSG] = aoi[UTM_ZONE].apply(lambda x: convert_UTM_epsg(x))
    
    zone_counts = aoi.groupby([UTM_ZONE, EPSG]).agg({UTM_ZONE: 'count'})
    
    logger.info(zone_counts)
    
    if aoi_out_path:
        aoi.to_file(aoi_out_path)
    
    return aoi


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('aoi', type=os.path.abspath,
                        help='Path to the AOI shapefile to locate UTM zones for.')
    parser.add_argument('--UTM_zones', type=os.path.abspath,
                        help='Path to the UTM Zones shapefile.')
    parser.add_argument('--aoi_out', type=os.path.abspath,
                        help='Path to write new shapefile of AOI with UTM Zone field.')
    
    args = parser.parse_args()
    
    AOI_PATH = args.aoi
    UTM_ZONES_PATH = args.UTM_zones
    AOI_OUT_PATH = args.aoi_out
    
    utm_zone_locater(aoi_in=AOI_PATH, utm_zones_in=UTM_ZONES_PATH, aoi_out_path=AOI_OUT_PATH)