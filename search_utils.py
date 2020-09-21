from copy import deepcopy
from calendar import monthrange
from datetime import datetime, timedelta
import json
import requests
import os
import sys
from pprint import pprint

import geopandas as gpd

from db_utils import Postgres
from logging_utils.logging_utils import create_logger

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

# Tables
oh_tbl = 'scenes_onhand'

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
attrib_arg_lut = {
    'min_date':             {filter_type: drf, operator: gte,  op_symbol:sym_gte, field_name: 'acquired'},
    'max_date':             {filter_type: drf, operator: lte,  op_symbol:sym_lte, field_name: 'acquired'},
    'max_cc':               {filter_type: rf,  operator: lte,  op_symbol:sym_lte, field_name: 'cloud_cover',},
    'min_ulx':              {filter_type: rf,  operator: gte,  op_symbol:sym_gte, field_name: 'origin_x'},
    'max_ulx':              {filter_type: rf,  operator: lte,  op_symbol:sym_lte, field_name: 'origin_x'},
    'min_uly':              {filter_type: rf,  operator: gte,  op_symbol:sym_gte, field_name: 'origin_y'},
    'max_uly':              {filter_type: rf,  operator: lte,  op_symbol:sym_lte, field_name: 'origin_y'},
    'provider':             {filter_type: sif, operator: None, op_symbol:sym_equal, field_name: 'provider'},
    'satellite_id':         {filter_type: sif, operator: None, op_symbol:sym_equal, field_name: 'satellite_id'},
    'instrument':           {filter_type: sif, operator: None, op_symbol:sym_equal, field_name: 'instrument'},
    'strip_id':             {filter_type: sif, operator: None, op_symbol:sym_equal, field_name: 'strip_id'},
    'min_sun_azimuth':      {filter_type: rf,  operator: gte,  op_symbol:sym_gte, field_name: 'sun_azimuth'},
    'max_sun_azimuth':      {filter_type: rf,  operator: lte,  op_symbol:sym_lte, field_name: 'sun_azimuth'},
    'min_sun_elevation':    {filter_type: rf,  operator: gte,  op_symbol:sym_gte, field_name: 'sun_elevation'},
    'max_sun_elevation':    {filter_type: rf,  operator: lte,  op_symbol:sym_lte, field_name: 'sun_elevation'},
    'quality_category':     {filter_type: sif, operator: None, op_symbol:sym_equal, field_name: 'quality_category'},
    'max_usable_data':      {filter_type: rf,  operator: gte,  op_symbol:sym_gte, field_name: 'usable_data'},
    'ground_control':       {filter_type: rf,  operator: gte,  op_symbol:sym_gte, field_name: 'ground_control'}
}


def create_master_geom_filter(vector_file):
    # Read in AOI
    aoi = gpd.read_file(vector_file)
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
    # If more than one, nest in OrFilter, otherwise return single GeometryFilter
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


def create_master_attribute_filter(attribute_args):
    # Create an AND filter from all attribute arguments provided
    # Remove arguments not provided
    attribute_kwargs = [kwa for kwa in attribute_args._get_kwargs() if kwa[1]]
    attribute_filters = []
    for arg_name, arg_value in attribute_kwargs:
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


def vector2geometry_config(vector_file):
    """Convert vector file to geojson geometry"""
    if type(vector_file) == str:
        gdf = gpd.read_file(vector_file)
    else:
        # assume gdf
        gdf = vector_file
    geoms = gdf.geometry.values
    geom = gdf.geometry.values[0]
    gs = gpd.GeoSeries([geom])
    config = json.loads(gs.to_json())['features'][0]['geometry']

    return config


def monthlist(start_date, end_date):
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
    Create an OrFilter which has a subfilter for each monnth that is a
    DateRangeFilter for every year between min date and max date for
    that month. Eg, if months = ['01']:
        (acquired >= 2019-01-01 and acquired is <= 2019-01-31) OR
        (acquired >= 2020-01-01 and acquired is <= 2020-01-31)
    Parameters:
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


def create_noh_filter(tbl=oh_tbl):
    with Postgres('sandwich-pool.planet') as db:
        sql = "SELECT {} FROM {}".format(f_id, tbl)
        oh_ids = list(set(list(db.sql2df(sql=sql, columns=[f_id])[f_id])))

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
    """Convert string of : separated arguments to form a filter
    to a formated search_filter dict"""
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
        config = vector2geometry_config(vector_file=vector_file)
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
    # logger.debug('Creating search request...')
    # Create a master filter - all filters applied as AND
    master_filter = create_master_filter(search_filters=search_filters)
    # Create search request json
    search_request = {
        "name": name,
        "item_types": item_types,
        "filter": master_filter
    }
    print('length of search request: {:,}'.format(len(str(search_request))))
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
            logger.info('New search created successfully: {}'.format(saved_search_id))
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


def delete_saved_search(session, search_name=None, search_id=None, dryrun=False):
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
            logger.error('Error connecting to {} with request:\n{}'.format(STATS_URL, str(stats_request)[0:500]))
            if len(str(stats_request)) > 500:
                logger.error('...{}'.format(str(stats_request)[-500:]))
            logger.debug(str(stats_request))
        logger.debug(stats)


    total_count = sum(bucket[count_key] for bucket in stats.json()[buckets_key])
    logger.debug('Total count for search request "{}": {:,}'.format(name, total_count))

    return total_count
