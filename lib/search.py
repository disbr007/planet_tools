from copy import deepcopy
from calendar import monthrange
from datetime import datetime
import json
import math
import os
import requests
import sys
import threading
from multiprocessing.dummy import Pool as ThreadPool

import geopandas as gpd
import pandas as pd
from retrying import retry
from shapely.geometry import Point, Polygon
from tqdm import tqdm

from lib.lib import read_ids, write_gdf
from lib.db import Postgres
from lib.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# TODO: convert to search_session class (all fxns that take session)
# TODO: Add filter: id NOT IN [list of existing IDs]

# config = os.path.join('config', 'saved_searches.yaml')

# API URLs and key
PLANET_URL = r'https://api.planet.com/data/v1'
SEARCH_URL = '{}/searches'.format(PLANET_URL)
STATS_URL = '{}/stats'.format(PLANET_URL)
PLANET_API_KEY = os.getenv('PL_API_KEY')
if not PLANET_API_KEY:
    logger.error('Error retrieving API key. Is PL_API_KEY env. variable set?')

# Threading
thread_local = threading.local()

# CREATE SEARCHES
# Footprint tables
SCENES_TBL = 'scenes2index'
scenes_onhand = 'scenes_onhand'

# To ensure passed filter type is valid
filter_types = ('DateRangeFilter', 'RangeFilter', 'UpdateFilter',
                'GeometryFilter', 'NumberInFilter', 'StringInFilter')

# String keys values in filters
ftype = 'type'
and_filter = 'AndFilter'
or_filter = 'OrFilter'
not_filter = 'NotFilter'
filter_type = 'filter_type'
field_name = 'field_name'
config = 'config'
operator = 'operator'
drf = 'DateRangeFilter'
gf = 'GeometryFilter'
nif = 'NumberInFilter'
rf = 'RangeFilter'
sif = 'StringInFilter'
lte = 'lte'
gte = 'gte'

op_symbol = 'op_symbol'
sym_gte = '>='
sym_lte = '<='
sym_equal = '='

# Fields
f_id = 'id'

# For parsing attribute arguements
# TODO: move this to a config file?
attrib_arg_lut = {
    'min_date':             {filter_type: drf, operator: gte,  op_symbol: sym_gte,   field_name: 'acquired'},
    'max_date':             {filter_type: drf, operator: lte,  op_symbol: sym_lte,   field_name: 'acquired'},
    'max_cc':               {filter_type: rf,  operator: lte,  op_symbol: sym_lte,   field_name: 'cloud_cover',},
    'min_ulx':              {filter_type: rf,  operator: gte,  op_symbol: sym_gte,   field_name: 'origin_x'},
    'max_ulx':              {filter_type: rf,  operator: lte,  op_symbol: sym_lte,   field_name: 'origin_x'},
    'min_uly':              {filter_type: rf,  operator: gte,  op_symbol: sym_gte,   field_name: 'origin_y'},
    'max_uly':              {filter_type: rf,  operator: lte,  op_symbol: sym_lte,   field_name: 'origin_y'},
    'provider':             {filter_type: sif, operator: None, op_symbol: sym_equal, field_name: 'provider'},
    'satellite_id':         {filter_type: sif, operator: None, op_symbol: sym_equal, field_name: 'satellite_id'},
    'instrument':           {filter_type: sif, operator: None, op_symbol: sym_equal, field_name: 'instrument'},
    'strip_id':             {filter_type: sif, operator: None, op_symbol: sym_equal, field_name: 'strip_id'},
    'id':                   {filter_type: sif, operator: None, op_symbol: sym_equal, field_name: 'id'},
    'min_sun_azimuth':      {filter_type: rf,  operator: gte,  op_symbol: sym_gte,   field_name: 'sun_azimuth'},
    'max_sun_azimuth':      {filter_type: rf,  operator: lte,  op_symbol: sym_lte,   field_name: 'sun_azimuth'},
    'min_sun_elevation':    {filter_type: rf,  operator: gte,  op_symbol: sym_gte,   field_name: 'sun_elevation'},
    'max_sun_elevation':    {filter_type: rf,  operator: lte,  op_symbol: sym_lte,   field_name: 'sun_elevation'},
    'quality_category':     {filter_type: sif, operator: None, op_symbol: sym_equal, field_name: 'quality_category'},
    'max_usable_data':      {filter_type: rf,  operator: gte,  op_symbol: sym_gte,   field_name: 'usable_data'},
    'ground_control':       {filter_type: rf,  operator: gte,  op_symbol: sym_gte,   field_name: 'ground_control'}
}


def create_master_geom_filter(vector_file):
    if not isinstance(vector_file, gpd.GeoDataFrame):
        # Read in AOI
        aoi = gpd.read_file(vector_file)
    else:
        aoi = vector_file
    # Create list of geometries to put in separate filters
    geometries = aoi.geometry.values
    json_geoms = [json.loads(gpd.GeoSeries([g]).to_json())['features'][0]['geometry']
                  for g in geometries]
    # Create each geometry filter
    geom_filters = []
    for jg in json_geoms:
        geom_filter = {
            ftype: gf,
            field_name: 'geometry',
            config: jg
        }
        geom_filters.append(geom_filter)
    # If more than one, nest in OrFilter, otherwise return single
    # GeometryFilter
    if len(geom_filters) > 1:
        master_geom_filter = {
            ftype: or_filter,
            config: geom_filters
        }
    else:
        master_geom_filter = geom_filters[0]

    return master_geom_filter


def create_attribute_filter(arg_name, arg_value):
    # Look up arg name, get filter type, field name, and comparison
    filter_params = attrib_arg_lut[arg_name]
    # If date (yyyy-mm-dd) provided, add time string to value
    if filter_params[filter_type] == drf:
        arg_value += "T00:00:00.000Z"

    if filter_params[operator]:
        arg_config = {filter_params[operator]: arg_value}
    else:
        arg_config = arg_value
    # Create filter
    filter = {
        ftype: filter_params[filter_type],
        field_name: filter_params[field_name],
        config: arg_config
    }

    return filter


def create_ids_filter(ids, field='id'):
    ids_filter = {ftype: sif,
                  field_name: field,
                  config: ids}
    return ids_filter


def create_master_attribute_filter(attrib_args):
    """Create an AND filter from all attribute arguments provided.
    Parameters
    ----------
    attrib_args : dict
        Dict of {attribute: value}
    Returns
    ---------
    dict : attribute filter
    """
    # Remove arguments not provided
    attribute_kwargs = {k: v for k, v in attrib_args.items() if v}
    attribute_filters = []
    for arg_name, arg_value in attribute_kwargs.items():
        att_filter = create_attribute_filter(arg_name, arg_value)
        attribute_filters.append(att_filter)

    if len(attribute_filters) > 1:
        master_attribute_filter = {
            ftype: and_filter,
            config: attribute_filters
        }
    else:
        master_attribute_filter = attribute_filters[0]

    return master_attribute_filter


def monthlist(start_date, end_date):
    """
    Create a list of tuples of (YYYY, MM) for each month
    between start_date and end_date.
    Parameters
    ----------
    start_date: str
        YYYY-MM-DD
    end_date: str
        YYYY-MM-DD

    Returns
    ----------
    list: tuple of strings like (YYYY, MM)

    """
    start, end = [datetime.strptime(_, "%Y-%m-%d") for _ in [start_date, end_date]]
    total_months = lambda dt: dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start)-1, total_months(end)):
        y, m = divmod(tot_m, 12)
        year = datetime(y, m+1, 1).strftime("%Y")
        month = datetime(y, m+1, 1).strftime("%m")
        mlist.append((year, month))

    return mlist


def create_months_filter(months, min_date=None, max_date=None,
                         month_min_days=None, month_max_days=None):
    """
    Create an OrFilter which has a subfilter for each month that is a
    DateRangeFilter for every year between min date and max date for
    that month. Eg, if months = ['01']:
        (acquired >= 2019-01-01 and acquired is <= 2019-01-31) OR
        (acquired >= 2020-01-01 and acquired is <= 2020-01-31)

    Parameters:
    -----------
        months : list
        min_date: str
            Date, like '2020-10-01'
        max_date: str
            Date, like '2020-10-31'
        month_min_days: dict
            {01: 10,
             02: 15,
             ...}
        month_max_days: dict
            {01: 25,
             02: 23,
             ...}
    """
    time_suffix = "T00:00:00.00Z"
    date_format = '%Y-%m-%d'
    if not min_date:
        min_date = '2015-01-01'
        logger.warning('Using default minimum date of {} for creation of month filters.'.format(min_date))
    if not max_date:
        max_date = datetime.now().strftime(date_format)
        logger.warning('Using default maximum date of {} for creation of month filters.'.format(max_date))
    dates = [min_date, max_date]
    all_months = monthlist(min_date, max_date)

    # Select only months provided
    selected_months = [(y, m) for y, m in all_months if m in months]
    mfs = []
    for year, month in selected_months:
        # If limits on days are provided, use those
        if month_min_days and month in month_min_days.keys():
            first_day = month_min_days[month]
        else:
            first_day = '01'
        if month_max_days and month in month_max_days.keys():
            last_day = month_max_days[month]
        else:
            _, last_day = monthrange(int(year), int(month))

        month_begin = '{}-{}-{}{}'.format(year, month, first_day, time_suffix)
        month_end = '{}-{}-{}{}'.format(year, month, last_day, time_suffix)

        mf = {
            ftype: drf,
            field_name: "acquired",
            config: {
                gte: month_begin,
                lte: month_end
            }
        }
        mfs.append(mf)

    months_filters = {
        ftype: or_filter,
        config: mfs
    }

    return months_filters


def create_noh_filter(tbl=scenes_onhand):
    # TODO: This does not work with as it exceeds the Planet API payload size
    #  when more than approximately 35k ids are included. Workaround is to
    #  subset the IDs using the same parameters as the search filter, and
    #  add a check that the number of IDs in the not_filter is < 35k. This
    #  still won't scale perfectly but is a start for reducing returned
    #  results.
    with Postgres('sandwich-pool.planet') as db:
        sql = "SELECT {} FROM {}".format(f_id, tbl)
        oh_ids = list(set(list(db.sql2df(sql_str=sql, columns=[f_id])[f_id])))

    sf = {
        ftype: sif,
        field_name: f_id,
        config: oh_ids
    }
    nf = {
        ftype: not_filter,
        config: sf
    }

    return nf


def filter_from_arg(filter_arg):
    """
    Convert string of : separated arguments to a formated
    search_filter dict
    """
    # logger.debug('Filter arg: {}'.format(filter_arg))
    filter_types = ('DateRangeFilter', 'RangeFilter', 'UpdateFilter',
                    'GeometryFilter', 'NumberInFilter', 'StringInFilter')
    filter_type = filter_arg[0]
    if filter_type not in filter_types:
        logger.error('Unrecognized filter type: {}'.format(filter_type))
        raise Exception
    if filter_type in ('DateRangeFilter', 'RangeFilter', 'UpdateFilter'):
        if len(filter_arg) != 4:
            logger.error('Unexpected number of arguments: {}'.format(len(filter_arg)))
            logger.error('Arguments: {}'.format(filter_arg))
            raise Exception
        # TODO: If DateRangeFilter, convert input to properly formatted date
        field_name = filter_arg[1]
        compare = filter_arg[2]
        value = filter_arg[3]
        if filter_type == 'RangeFilter':
            value = float(value)
        if filter_type == 'DateRangeFilter':
            # Format the date (yyyy-mm-dd) with time for API call
            value += "T00:00:00.000Z"
        if compare not in ('gte', 'gt', 'lte', 'lt'):
            logger.error('Unrecognized comparison argument: {}'.format(compare))
            raise Exception
        config = {compare: value}
    elif filter_type in ('NumberInFilter', 'StringInFilter'):
        field_name = filter_arg[1]
        config = filter_arg[2:]
    elif filter_type == 'GeometryFilter':
        if len(filter_arg) == 2:
            field_name = 'geometry'
            vector_file = filter_arg[1]
        elif len(filter_arg) == 3:
            field_name = filter_arg[1]
            vector_file = filter_arg[2]
        logger.debug('Loading geometry from vector file: {}'.format(vector_file))
        assert os.path.exists(vector_file)
        # config = vector2geometry_config(vector_file=vector_file)
        config = create_master_geom_filter(vector_file)
    else:
        field_name, config = filter_arg[1], filter_arg[2]

    search_filter = {
        "type": filter_type,
        "field_name": field_name,
        "config": config
        }
    # logger.debug('Created search filter:\nType: {}\nField: {}\nConfig: {}'.format(filter_type,
    #                                                                               field_name,
    #                                                                               config))

    return search_filter


def create_asset_filter(asset):
    asset_filter = {
        "type": "AssetFilter",
        "config": [asset]
    }

    return asset_filter


def create_master_filter(search_filters):
    if len(search_filters) > 1:
        master_filter = {
            "type": "AndFilter",
            "config": search_filters
        }
    else:
        master_filter = search_filters[0]

    return master_filter


def create_search_request(name, item_types, search_filters):
    logger.debug('Creating search request...')
    # Create a master filter - all filters applied as AND
    master_filter = create_master_filter(search_filters=search_filters)
    # Create search request json
    search_request = {
        "name": name,
        "item_types": item_types,
        "filter": master_filter
    }

    return search_request


def create_saved_search(search_request, overwrite_saved=False):
    """Creates a saved search on the Planet API and returns the search ID."""
    with requests.Session() as s:
        logger.debug('Authorizing using Planet API key...')
        s.auth = (PLANET_API_KEY, '')
        saved_searches = get_all_searches(s)
        search_name = search_request["name"]

        # Determine if a saved search with the provided name exists
        ids_with_same_name = [x for x in saved_searches.keys() if saved_searches[x]['name'] == search_name]

        if ids_with_same_name:
            logger.warning("Saved search with name '{}' already exists.".format(search_name))
            if overwrite_saved:
                logger.warning("Overwriting saved search with same name.")
                for overwrite_id in ids_with_same_name:
                    overwrite_url = '{}/{}'.format(SEARCH_URL, overwrite_id)
                    r = s.delete(overwrite_url)
                    if r.status_code == 204:
                        logger.debug('Deleted saved search: {}'.format(overwrite_id))
            else:
                logger.warning('Overwrite not specified, exiting')
                sys.exit()
        # Create new saved search
        logger.info('Creating new saved search: {}'.format(search_name))
        saved_search = s.post(SEARCH_URL, json=search_request)
        logger.debug('Search creation request status: {}'.format(saved_search.status_code))
        if saved_search.status_code == 200:
            saved_search_id = saved_search.json()['id']
            logger.debug('New search created successfully: {}'.format(saved_search_id))
        else:
            logger.error('Error creating new search.')
            saved_search_id = None

    return saved_search_id


def get_all_searches(session):
    logger.debug('Getting saved searches...')
    res = session.get(SEARCH_URL, params={"search_type": "saved"})
    searches = res.json()["searches"]

    saved_searches = dict()
    for se in searches:
        saved_searches[se["id"]] = {k: se[k] for k in se.keys()}

    logger.debug('Saved searches found: {}'.format(len(saved_searches.keys())))
    logger.debug('Saved search IDs:\n{}'.format('\n'.join(saved_searches.keys())))

    return saved_searches


def get_search_id(session, search_name):
    """Return the saved search ID for the given search name"""
    all_searches = get_all_searches(session)
    matches = [s for s in all_searches if s["name"]]
    search_id = all_searches[search_name]["id"]

    return search_id


def get_saved_search(session, search_id=None, search_name=None):
    all_searches = get_all_searches(session=session)
    if search_id:
        ss = all_searches[search_id]
    elif search_name:
        ss_id = get_search_id(session=session, search_name=search_name)
        ss = all_searches[search_id]
    else:
        logger.error('Must provide one of search_id or search_name')

    return ss


def create_search(name, item_types,
                  ids=None,
                  aoi=None,
                  attrib_args=None,
                  months=None,
                  month_min_day_args=None,
                  month_max_day_args=None,
                  filters=None,
                  asset_filters=None,
                  load_filter=None,
                  not_on_hand=False,
                  fp_not_on_hand=False,
                  get_count_only=False,
                  overwrite_saved=False,
                  save_filter=False,
                  dryrun=False,
                  **kwargs):
    """
    Create a saved search using the Planet API, which gets a search
    ID. The search ID can then be used to retrieve footprints.

    Parameters
    ----------
    name : str
        The name to use when saving the search
    item_types : list
        The item_types to include in the search, e.g. ['PSScene4Band']
    ids : list, path to text file of IDs
        A list of scene ID's to select.
    attrib_args : dict
        Dictionary of arguments and values to use with
        create_attribute_filter to create filters
    months : list
        The months to include in the search.
    month_min_day : list
        List of lists of ['zero-padded month', 'zero-padded min-day']
    month_max_day : list
        List of lists of ['zero-padded month', 'zero-padded max-day']
    filters : list
        List of dictionarys that are already formated filters, see:
        https://developers.planet.com/docs/data/searches-filtering/
    asset_filters : list
        List of assests to include in search, e.g. ['basic_analytic']
    load_filter : str
        Path to json file containing filter(s) to load and use.
    not_on_hand : bool
        Create a NOT filter with all IDs currently on hand.
    fp_not_on_hand : bool
        Create a NOT filter with all IDs currently in 'scenes2index' table
    get_count_only : bool
        Get the count of the search and stop - do not get footprints
    overwrite_saved : bool
        Overwrite previously created search if exists with same name
    save_filter : str
        Path to write filter as json, can then be used with load_filter
    dryrun : bool
        Create filters and get count without saving search.

    Returns
    -------
    tuple : (str:saved search id, int:search count with filters)
    """

    logger.info('Creating saved search...')

    search_filters = []

    if attrib_args and any([v for k, v in attrib_args.items()]):
        master_attribute_filter = create_master_attribute_filter(attrib_args)
        # print(master_attribute_filter)
        search_filters.append(master_attribute_filter)

    # Parse AOI to filter
    if aoi is not None:
        aoi_attribute_filter = create_master_geom_filter(vector_file=aoi)
        search_filters.append(aoi_attribute_filter)

    # Parse raw filters
    if filters:
        search_filters.extend([filter_from_arg(f) for f in filters])

    if ids:
        if isinstance(ids, str):
            # Assume path
            include_ids = read_ids(ids)
        else:
            include_ids = ids
        ids_filter = create_ids_filter(include_ids)
        search_filters.append(ids_filter)

    if months:
        month_min_days = None
        month_max_days = None
        if month_min_day_args:
            month_min_days = {month: day for month, day in month_min_day_args}
        if month_max_day_args:
            month_max_days = {month: day for month, day in month_max_day_args}
        mf = create_months_filter(months,
                                  min_date=attrib_args['min_date'],
                                  max_date=attrib_args['max_date'],
                                  month_min_days=month_min_days,
                                  month_max_days=month_max_days)
        search_filters.append(mf)

    # Parse any provided filters
    if load_filter:
        addtl_filter = json.load(open(load_filter))
        search_filters.append(addtl_filter)

    # Parse any asset filters
    if asset_filters:
        for af in asset_filters:
            f = create_asset_filter(af)
            search_filters.append(f)

    if not_on_hand:
        logger.warning('Not on hand filter may fail due to payload size '
                       'restriction on Planet API.')
        noh_filter = create_noh_filter(tbl=scenes_onhand)
        search_filters.append(noh_filter)

    if fp_not_on_hand:
        logger.warning('Not on hand filter may fail due to payload size '
                       'restriction on Planet API.')
        fp_noh_filter = create_noh_filter(tbl=SCENES_TBL)
        fp_noh_filter['config']['config'] = fp_noh_filter['config']['config'][:10000]
        search_filters.append(fp_noh_filter)

    # Create search request using the filters created above
    for f in search_filters:
        if f['type'] == 'NotFilter':
            pf = deepcopy(f)
            # print(len(pf['config']['config']))
            pf['config']['config'] = pf['config']['config'][:20]
            # pprint(pf)

    sr = create_search_request(name=name, item_types=item_types, search_filters=search_filters)

    if save_filter:
        if os.path.basename(save_filter) == 'default.json':
            save_filter = os.path.join(os.path.dirname(__file__),
                                       'config',
                                       'search_filters',
                                       '{}.json'.format(name))
        logger.debug('Saving filter to: {}'.format(save_filter))
        with open(save_filter, 'w') as src:
            json.dump(sr, src)

    if logger.level == 10:
        # pprint was happening even when logger.level = 20 (INFO)
        logger.debug('Search request:\n{}'.format(sr))

    # if get_count:
    total_count = get_search_count(sr)
    logger.debug('Count for new search "{}": {:,}'.format(name, total_count))
    if get_count_only:
        return None, total_count

    if not dryrun:
        if total_count > 0:
            # Submit search request as saved search
            ss_id = create_saved_search(search_request=sr,
                                        overwrite_saved=overwrite_saved)
            if ss_id:
                logger.info('Successfully created new search. '
                            'Search ID: {}'.format(ss_id))
        else:
            logger.warning('Search returned no results - skipping saving.')
            ss_id = None
    else:
        ss_id = None

    return ss_id, total_count


def delete_saved_search(session, search_name=None, search_id=None,
                        dryrun=False):
    all_searches = get_all_searches(session)
    if search_id:
        if search_id in all_searches.keys():
            delete_id = search_id
            delete_url = "{}/{}".format(SEARCH_URL, delete_id)
    elif search_name:
        ids_names = [(s_id, all_searches[s_id]['name']) for s_id in all_searches
                     if all_searches[s_id]['name'] == search_name]
        if len(ids_names) == 0:
            logger.warning('No search with name {} found.'.format(search_name))
        elif len(ids_names) > 1:
            logger.warning('Multiple searches found with name {}\n{}'.format(search_name, ids_names))
        else:
            delete_id = ids_names[0]
        delete_url = "{}/{}".format(SEARCH_URL, delete_id)

    logger.debug('ID to delete: {}'.format(delete_id))

    if not dryrun:
        r = session.delete(delete_url)
        if r.status_code == 204:
            logger.info('Successfully deleted search.')


def get_search_count(search_request):
    sr_copy = deepcopy(search_request)
    name = sr_copy['name']
    stats_request = dict()
    stats_request['filter'] = sr_copy['filter']
    stats_request['item_types'] = sr_copy['item_types']
    stats_request["interval"] = "year"

    buckets_key = 'buckets'
    count_key = 'count'

    with requests.Session() as session:
        logger.debug('Authorizing using Planet API key...')
        session.auth = (PLANET_API_KEY, '')
        stats = session.post(STATS_URL, json=stats_request)
        if not str(stats.status_code).startswith('2'):
            logger.error(stats.status_code)
            logger.error(stats.reason)
            logger.error('Error connecting to {} with request:'
                         '\n{}'.format(STATS_URL, str(stats_request)[0:500]))
            if len(str(stats_request)) > 500:
                logger.error('...{}'.format(str(stats_request)[-500:]))
            logger.debug(str(stats_request)[500:])
        logger.debug(stats)

    # pprint(stats_request)
    total_count = sum(bucket[count_key]
                      for bucket in stats.json()[buckets_key])
    logger.info('Total count for search request "{}": {:,}'.format(name,
                                                                    total_count))

    return total_count


def get_session():
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
        thread_local.session.auth = (PLANET_API_KEY, '')
    return thread_local.session


def response2gdf(response):
    """Converts API response to a geodataframe."""
    # Coordinate system of features returned by Planet API
    crs = 'epsg:4326'
    # Response feature property keys
    features_key = 'features'
    id_key = 'id'
    geometry_key = 'geometry'
    type_key = 'type'
    coords_key = 'coordinates'
    properties_key = 'properties'
    # Response feature values
    polygon_type = 'Polygon'
    point_type = 'Point'
    # All properites provided in API reponse
    property_atts = ['acquired', 'anomalous_pixels', 'cloud_cover',
                     'columns', 'epsg_code', 'ground_control', 'gsd',
                     'instrument', 'item_type', 'origin_x', 'origin_y',
                     'pixel_resolution', 'provider', 'published',
                     'quality_category', 'rows', 'satellite_id',
                     'strip_id', 'sun_azimuth', 'sun_elevation',
                     'updated', 'view_angle']

    features = response.json()[features_key]
    # Format response in dictionary format supported by geopandas
    reform_feats = {att: [] for att in property_atts}
    reform_feats[id_key] = []
    reform_feats[geometry_key] = []

    for feat in features:
        # Get ID
        reform_feats[id_key].append(feat[id_key])
        # Get geometry as shapely object
        geom_type = feat[geometry_key][type_key]
        if geom_type == polygon_type:
            geometry = Polygon(feat[geometry_key][coords_key][0])
        elif geom_type == point_type:
            geometry = Point(feat[geometry_key][coords_key][0])
        reform_feats[geometry_key].append(geometry)
        # Get all properties
        for att in property_atts:
            try:
                reform_feats[att].append(feat[properties_key][att])
            except KeyError:
                reform_feats[att].append(None)

    gdf = gpd.GeoDataFrame(reform_feats, crs=crs)
    # gdf['acquired'] = pd.to_datetime(gdf['acquired'], format="%Y-%m-%dT%H%M%S%fZ")

    return gdf


def get_search_page_urls(saved_search_id, total_count):
    # TODO: Speed this up, bottle neck / is it possible to speed up?
    #   How much longer would it take to just process each page...?
    # TODO: Possibly rewrite as loop up to total number of scenes2index in search id
    def fetch_pages(search_url, all_pages):
        # logger.debug('Fetching page...')
        session = get_session()
        all_pages.append(search_url)
        res = session.get(search_url)
        if res.status_code != 200:
            logger.error('Error connecting to search API: {}'.format(first_page_url))
            logger.error('Status code: {}'.format(res.status_code))
            logger.error('Reason: {}'.format(res.reason))
            if res.status_code == 408:
                logger.warning('Request timeout.')
            raise ConnectionError
        page = res.json()
        next_url = page['_links'].get('_next')
        logger.debug(next_url)

        return next_url
        # if next_url:
        #     fetch_pages(next_url, all_pages)

    # 250 is max page size
    feat_per_page = 250

    logger.debug('Getting page urls for search...')
    all_pages = []

    total_pages = math.ceil(total_count / feat_per_page) + 1
    logger.debug('Total pages for search: {}'.format(total_pages))
    pbar = tqdm(total=total_pages, desc='Parsing response pages')
    first_page_url = '{}/{}/results?_page_size={}'.format(SEARCH_URL, saved_search_id, feat_per_page)
    next_page = first_page_url
    while next_page:
        # pbar.write('Parsing: {}'.format(next_page))
        next_page = fetch_pages(next_page, all_pages=all_pages)
        pbar.update(1)
    logger.debug('Pages: {}'.format(len(all_pages)))

    return all_pages


@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000,
       stop_max_delay=30000)
def process_page(page_url):
    session = get_session()
    res = session.get(page_url)
    if res.status_code == 429:
        logger.debug('Response: {} - rate limited - retrying...'.format(res.status_code))
        raise Exception("Rate limit error. Retrying...")
    if res.status_code != 200:
        logger.error('Error connecting to search API: {}'.format(page_url))
        logger.error('Status code: {}'.format(res.status_code))
        logger.error('Reason: {}'.format(res.reason))
        raise ConnectionError

    gdf = response2gdf(response=res)

    return gdf


def get_features(saved_search_id, total_count):

    all_pages = get_search_page_urls(saved_search_id=saved_search_id,
                                     total_count=total_count)

    threads = 1
    thread_pool = ThreadPool(threads)
    logger.debug('Getting features...')
    results = thread_pool.map(process_page, all_pages)

    logger.info('Combining page results...')
    master_footprints = pd.concat(results)

    return master_footprints


def select_scenes(search_id, dryrun=False):

    # Test a request
    session = get_session()
    r = session.get(PLANET_URL)
    if not r.status_code == 200:
        logger.error('Error connecting to Planet Data API.')

    logger.info('Performing query using saved search ID: {}'.format(search_id))

    # Get a total count for the given filters
    # Get search request of passed search ID
    sr = get_saved_search(session=session, search_id=search_id)
    sr_name = sr['name']
    total_count = get_search_count(search_request=sr)

    # Perform requests to API to return features, which are converted to footprints in a geodataframe
    master_footprints = gpd.GeoDataFrame()
    if not dryrun:
        master_footprints = get_features(saved_search_id=search_id, total_count=total_count)
    logger.info('Total features processed: {:,}'.format(len(master_footprints)))

    return master_footprints, sr_name


def write_scenes(scenes, out_name=None, out_path=None, out_dir=None):
    if out_dir:
        # Use search request name as output
        out_path = os.path.join(out_dir, '{}.geojson'.format(out_name))
    # TODO: Check if out_scenes exists (earlier) abort if not overwrite
    logger.info('Writing selected features to file: {}'.format(out_path))
    write_gdf(scenes, out_path)


def get_search_footprints(out_path=None, out_dir=None,
                          to_scenes_tbl=False, dryrun=False,
                          search_id=None, **kwargs):
    if not PLANET_API_KEY:
        logger.error('Error retrieving API key. Is PL_API_KEY env. variable '
                     'set?')

    scenes, search_name = select_scenes(search_id=search_id, dryrun=dryrun)
    if len(scenes) == 0:
        logger.warning('No scenes2index found. Exiting.')
        sys.exit()
    if any([out_path, out_dir]):
        if out_dir:
            write_scenes(scenes, out_name=search_name, out_dir=out_dir)
        else:
            write_scenes(scenes, out_path=out_path)

    if to_scenes_tbl:
        with Postgres() as db:
            db.insert_new_records(scenes,
                                  table=SCENES_TBL,
                                  dryrun=dryrun)

    return scenes
