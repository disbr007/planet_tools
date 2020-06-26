import copy
import os

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, AutoLocator
import matplotlib.ticker as plticker
import matplotlib.dates as mdates
import numpy as np

import pandas as pd
import geopandas as gpd

from logging_utils.logging_utils import create_logger
from determine_stereo import determine_stereo
from analysis_utils import grid_aoi, get_count

logger = create_logger(__name__, 'sh', 'DEBUG')
plt.style.use('ggplot')

# Constants
# TODO: Create a config file of attributes -- load from that everywhere
# Existing fields
fld_iid = 'id'
fld_sid = 'strip_id'
fld_ins = 'instrument'
fld_acq = 'acquired'
fld_epsg = 'epsg_code'
fld_va = 'view_angle'
fld_ovlp_geom = 'ovlp_geom'
# Created fields
fld_ins_name = 'ins_name'
fld_sqkm = 'sqkm'
fld_pn = 'pairname'
fld_ovlp_perc = 'ovlp_perc'

def get_instrument_names(instrument):
    """Get instrument name from code"""
    platform_lut = {'PS2': 'Dove',
                    'PS2.SD': 'Dove-R',
                    'PSB.SD': 'SuperDove'}

    return platform_lut[instrument]


def create_pairname(iid1, iid2):
    pn = '{}-{}'.format(iid1, iid2)

    return pn


def y_fmt(y, pos):
    '''
    Formatter for y axis of plots. Returns the number with appropriate suffix
    y: value
    pos: *Not needed?
    '''
    decades = [1e9, 1e6, 1e3, 1e0, 1e-3, 1e-6, 1e-9 ]
    suffix  = ["G", "M", "k", "" , "m" , "u", "n"  ]
    if y == 0:
        return str(0)
    for i, d in enumerate(decades):
        if np.abs(y) >=d:
            val = y/float(d)
            signf = len(str(val).split(".")[1])
            if signf == 0:
                return '{val:d} {suffix}'.format(val=int(val), suffix=suffix[i])
            else:
                if signf == 1:
                    if str(val).split(".")[1] == "0":
                       return '{val:d}{suffix}'.format(val=int(round(val)), suffix=suffix[i])
                tx = "{"+"val:.{signf}f".format(signf = signf) +"} {suffix}"
                return tx.format(val=val, suffix=suffix[i])
    return y


# Paths
prj_path = r'V:\pgc\data\scratch\jeff\projects\planet'
scenes_path = os.path.join(prj_path, 'scenes', 'n65w148')
scenes_yr_p = os.path.join(scenes_path, r'n65w148_2019jun01_2020jun01_cc20.geojson')
scenes_sm_p = os.path.join(scenes_path, r'n65w148_2020dec01_2020jun01_cc20.geojson')
scenes_om_p = os.path.join(scenes_path, r'n65w148_2020may01_2020jun01_cc20.geojson')
# Analysis
analysis_path = os.path.join(prj_path, 'analysis')
# Density
dens_path = os.path.join(analysis_path, 'density')
# Pickles
pkl_path = os.path.join(analysis_path, 'pkl')
ovlp_strip_pkl = os.path.join(pkl_path, 'n65w148_1yr_ovlp_strip.pkl')
ovlp_days5_pkl = os.path.join(pkl_path, 'n65w148_1yr_ovlp_5d.pkl')
ovlp_days10_pkl = os.path.join(pkl_path, 'n65w148_1yr_ovlp_10d.pkl')
ovlp_days30_pkl = os.path.join(pkl_path, 'n65w148_1yr_ovlp_30d.pkl')

# Open
scenes_yr = gpd.read_file(scenes_yr_p) # one year
scenes_sm = gpd.read_file(scenes_sm_p) # six months
scenes_om = gpd.read_file(scenes_om_p) # one month


# Add instrument name field
scenes_yr[fld_ins_name] = scenes_yr[fld_ins].apply(lambda x: get_instrument_names(x))

# Convert to UTM
wgs = 'epsg:4236'
utm_zone = int(scenes_yr[fld_epsg].mode())
epsg = 'epsg:{}'.format(utm_zone)
scenes_yr = scenes_yr.to_crs(epsg)
# Create area field
scenes_yr[fld_sqkm] = scenes_yr.geometry.area / 10e6


# Basic Stats (by sensor, using yearly)

mmm = ['min', 'mean', 'max']
# Counts, scene area, view angle
ins_scene_agg = scenes_yr.groupby(fld_ins_name).agg({fld_iid: 'count',
                                                     fld_sqkm: mmm,
                                                     fld_va: mmm})
logger.info('\n{}'.format(ins_scene_agg))
# Scene area histogram by sensor
_h, bins = np.histogram([scenes_yr[fld_sqkm].min(), scenes_yr[fld_sqkm].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = scenes_yr.groupby([fld_ins_name, pd.cut(scenes_yr[fld_sqkm], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()

# Avg strip area
ins_strip_area_sum = scenes_yr.groupby([fld_ins_name, fld_sid]).agg({fld_sqkm: 'sum', fld_iid: 'count'})
ins_strip_area_sum = ins_strip_area_sum.reset_index()
ins_strip_area_agg = ins_strip_area_sum.groupby(fld_ins_name).agg({fld_sqkm: ['min', 'mean', 'max'],
                                                                   fld_iid: ['min', 'mean', 'max']})
logger.info('\n{}'.format(ins_strip_area_agg))
_h, bins = np.histogram([ins_strip_area_sum[fld_sqkm].min(), ins_strip_area_sum[fld_sqkm].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ins_strip_area_sum.groupby([fld_ins_name, pd.cut(ins_strip_area_sum[fld_sqkm], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()

# Acquired date histogram for a year (by week? by month?)
scenes_yr[fld_acq] = pd.to_datetime(scenes_yr[fld_acq])
scenes_yr.set_index(fld_acq, inplace=True)
agg = {fld_iid: 'count'}
scenes_yr_agg = scenes_yr.groupby([pd.Grouper(freq='M'), fld_ins_name]).agg(agg)
scenes_yr_agg = scenes_yr_agg.unstack(fld_ins_name)
scenes_yr_agg.columns = scenes_yr_agg.columns.droplevel()
# Convert nan to 0
scenes_yr_agg.fillna(value=0, inplace=True)

# Formatting
formatter = FuncFormatter(y_fmt)
fig, ax = plt.subplots(nrows=1, ncols=1)
# ax.xaxis_date()
# scenes_yr_agg.plot(kind='bar', stacked=True, ax=ax)
plotted = []
for ins in scenes_yr[fld_ins_name].unique():
    ax.bar(scenes_yr_agg.index, scenes_yr_agg[ins], width=15, edgecolor='#575757',
           bottom=sum(plotted), align='center')
    plotted.append(scenes_yr_agg[ins])

plt.setp(ax.xaxis.get_majorticklabels(), 'rotation', 90)
plt.setp(ax.xaxis.get_minorticklabels(), 'rotation', 90)
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
# ax.xaxis.set_major_locator(AutoLocator())
fig.show()


#### Overlap
days_threshold = 10000
ovlp_agg = {fld_sqkm: mmm, fld_ovlp_perc: mmm}

# Overlap within strip
logger.debug('Determining overlap within strip...')
ovlp_strip = determine_stereo(scenes_p=scenes_yr_p, overlap_metric='percent',
                              overlap_threshold=0, days_threshold=days_threshold,
                              within_days=False, within_strip=True, within_ins=True)

# ovlp_strip = pd.read_pickle(ovlp_strip_pkl)
# ovlp_strip = gpd.GeoDataFrame(ovlp_strip, geometry=fld_ovlp_geom, crs=wgs)
ovlp_strip = ovlp_strip.to_crs(epsg)
ovlp_strip[fld_ins_name] = ovlp_strip[fld_ins].apply(lambda x: get_instrument_names(x))
ovlp_strip[fld_pn] = ovlp_strip.apply(lambda x: create_pairname(x[fld_iid], x['{}2'.format(fld_iid)]), axis=1)
ovlp_strip[fld_sqkm] = ovlp_strip.geometry.area / 10e6
ovlp_strip_agg = ovlp_strip.groupby(fld_ins_name).agg(ovlp_agg)
logger.info('\n{}'.format(ovlp_strip_agg))

# Overlap area within strip histogram
_h, bins = np.histogram([ovlp_strip[fld_sqkm].min(), ovlp_strip[fld_sqkm].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_strip.groupby([fld_ins_name, pd.cut(ovlp_strip[fld_sqkm], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()
# Overlap perc within strip histogram
_h, bins = np.histogram([ovlp_strip[fld_ovlp_perc].min(), ovlp_strip[fld_ovlp_perc].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_strip.groupby([fld_ins_name, pd.cut(ovlp_strip[fld_ovlp_perc], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()


#### Overlap within dates (5, 10, 30)
logger.debug('Determining overlap within 5 days...')
ovlp_days5 = determine_stereo(scenes_p=scenes_yr_p, overlap_metric='percent',
                              overlap_threshold=0, days_threshold=5,
                              within_days=True, within_ins=True)
# ovlp_days5 = pd.read_pickle(ovlp_days5_pkl)
# ovlp_days5 = gpd.GeoDataFrame(ovlp_days5, geometry=fld_ovlp_geom, crs=wgs)
ovlp_days5 = ovlp_days5.to_crs(epsg)
ovlp_days5[fld_ins_name] = ovlp_days5[fld_ins].apply(lambda x: get_instrument_names(x))

ovlp_days5[fld_sqkm] = ovlp_days5.geometry.area / 10e6
ovlp_days5_agg = ovlp_days5.groupby(fld_ins_name).agg(ovlp_agg)
logger.info('\n{}'.format(ovlp_days5_agg))

# Overlap area within 5 days histogram
_h, bins = np.histogram([ovlp_days5[fld_sqkm].min(), ovlp_days5[fld_sqkm].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_days5.groupby([fld_ins_name, pd.cut(ovlp_days5[fld_sqkm], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()
# Overlap perc within 5 days histogram
_h, bins = np.histogram([ovlp_days5[fld_ovlp_perc].min(), ovlp_days5[fld_ovlp_perc].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_days5.groupby([fld_ins_name, pd.cut(ovlp_days5[fld_ovlp_perc], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()


#### 10 days
logger.debug('Determining overlap within 10 days...')
ovlp_days10 = determine_stereo(scenes_p=scenes_yr_p, overlap_metric='percent',
                               overlap_threshold=0, days_threshold=10,
                               within_days=True, within_ins=True)
# ovlp_days10 = pd.read_pickle(ovlp_days10_pkl)
# ovlp_days10 = gpd.GeoDataFrame(ovlp_days10, geometry=fld_ovlp_geom, crs=wgs)
ovlp_days10 = ovlp_days10.to_crs(epsg)
ovlp_days10[fld_ins_name] = ovlp_days10[fld_ins].apply(lambda x: get_instrument_names(x))
ovlp_days10[fld_sqkm] = ovlp_days10.geometry.area / 10e6
ovlp_days10_agg = ovlp_days10.groupby(fld_ins_name).agg(ovlp_agg)
logger.info('\n{}'.format(ovlp_days10_agg))

# Overlap area within 10 days histogram
_h, bins = np.histogram([ovlp_days10[fld_sqkm].min(), ovlp_days10[fld_sqkm].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_days10.groupby([fld_ins_name, pd.cut(ovlp_days10[fld_sqkm], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()
# Overlap perc within 10 days histogram
_h, bins = np.histogram([ovlp_days10[fld_ovlp_perc].min(), ovlp_days10[fld_ovlp_perc].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_days10.groupby([fld_ins_name, pd.cut(ovlp_days10[fld_ovlp_perc], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()


#### 30 days
logger.debug('Determining overlap within 30 days...')
ovlp_days30 = determine_stereo(scenes_p=scenes_yr_p, overlap_metric='percent',
                               overlap_threshold=0, days_threshold=30,
                               within_days=True, within_ins=True)

# ovlp_days30 = pd.read_pickle(ovlp_days30_pkl)
# ovlp_days30 = gpd.GeoDataFrame(ovlp_days30, geometry=fld_ovlp_geom, crs=wgs)
ovlp_days30 = ovlp_days30.to_crs(epsg)
ovlp_days30[fld_ins_name] = ovlp_days30[fld_ins].apply(lambda x: get_instrument_names(x))
ovlp_days30[fld_sqkm] = ovlp_days30.geometry.area / 10e6
ovlp_days30_agg = ovlp_days30.groupby(fld_ins_name).agg(ovlp_agg)
logger.info('\n{}'.format(ovlp_days30_agg))

# Overlap area within 30 days histogram
_h, bins = np.histogram([ovlp_days30[fld_sqkm].min(), ovlp_days30[fld_sqkm].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_days30.groupby([fld_ins_name, pd.cut(ovlp_days30[fld_sqkm], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()
# Overlap perc within 30 days histogram
_h, bins = np.histogram([ovlp_days30[fld_ovlp_perc].min(), ovlp_days30[fld_ovlp_perc].max()], bins=20)
bins = np.around(bins)
bins = [int(b) for b in bins]
s = ovlp_days30.groupby([fld_ins_name, pd.cut(ovlp_days30[fld_ovlp_perc], bins=bins)]).size()
s = s.unstack().T
fig, ax = plt.subplots(1,1)
s.plot(kind='bar', width=0.85, ax=ax, edgecolor='#575757', align='center', stacked=True, legend=False)
fig.show()


#### Density stats
# Create grid over AOI geocell
geocell = os.path.join(prj_path, r'geocells\geocell_n65w148.shp')
logger.debug('Creating grid over AOI: {}'.format(geocell))
grid = grid_aoi(geocell, n_pts_x=250, n_pts_y=250)

# Density of each sensor (monthly, 6 months, yearly)
logger.debug('Getting density...')
dove_yr = scenes_yr[scenes_yr[fld_ins_name] == 'Dove']
dove_r_yr = scenes_yr[scenes_yr[fld_ins_name] == 'Dove-R']
dove_sd_yr = scenes_yr[scenes_yr[fld_ins_name] == 'SuperDove']
# Density of all sensors (monthly, 6 months, yearly)
logger.debug('Computing SuperDove density...')
dove_sd_yr_dens = get_count(grid, dove_sd_yr)
dove_sd_yr_dens.to_file(os.path.join(dens_path, 'n65w148_dove_sd_yr_density.geojson'), driver='GeoJSON')
logger.debug('Computing Dove-R density...')
dove_r_yr_dens = get_count(grid, dove_r_yr)
dove_r_yr_dens.to_file(os.path.join(dens_path, 'n65w148_dove_r_yr_density.geojson'), driver='GeoJSON')
logger.debug('Computing Dove density...')
dove_yr_dens = get_count(grid, dove_yr)
dove_yr_dens.to_file(os.path.join(dens_path, 'n65w148_dove_yr_density.geojson'), driver='GeoJSON')



