# Constants
WINDOWS = 'Windows'
LINUX = 'Linux'
AWS = 'aws'
ZIP = 'zip'

# Planet API
PL_API_KEY = 'PL_API_KEY'  # environmental variable name
ORDERS_URL = "https://api.planet.com/compute/ops/orders/v2"
PLANET_URL = r'https://api.planet.com/data/v1'
SEARCH_URL = '{}/searches'.format(PLANET_URL)
STATS_URL = '{}/stats'.format(PLANET_URL)

# config.json keys
DOWNLOAD_LOC = 'download_loc'
UNIQUE_ID = "unique_id"

# Database
SANDWICH = 'sandwich'  # short name in db_utils.config.json
PLANET = 'planet'  # database name in db_utils.json
SANDWICH_POOL_PLANET = 'sandwich-pool.planet'

# Table names
SCENES = 'scenes'
SCENES_ONHAND = 'scenes_onhand'
MULTILOOK_CANDIDATES = 'multilook_candidates'
STEREO_CANDIDATES = 'stereo_candidates'
OFF_NADIR = 'off_nadir'

# Fields
ACQUIRED = 'acquired'
INSTRUMENT = 'instrument'
GEOMETRY = 'geometry'
# stereo_candidates + xtrackcc20
DATE_DIFF = 'date_diff'
ACQUIRED1 = '{}1'.format(ACQUIRED)
INSTRUMENT1 = '{}1'.format(INSTRUMENT)
INSTRUMENT2 = '{}2'.format(INSTRUMENT)
VIEW_ANGLE_DIFF = 'view_angle_diff'
OFF_NADIR_DIFF = 'off_nadir_diff'
OVLP_PERC = 'ovlp_perc'
OVLP_GEOM = 'ovlp_geom'
# scenes_onhand
IDENTIFIER = 'identifier'
ACQUISTIONDATETIME = 'acquisitionDateTime'
BUNDLE_TYPE = 'bundle_type'
CENTROID = 'centroid'
CENTER_X = 'center_x'
CENTER_Y = 'centery_y'
SHELVED_LOC = 'shelved_loc'
# off_nadir
SCENE_NAME = 'scene_name'
AZIMUTH = 'azimuth'
OFF_NADIR_FLD = 'off_nadir'
OFF_NADIR_SIGNED = 'off_nadir_signed'
# multilook_candidates
SRC_ID = 'src_id'
PAIRNAME = 'pairname'
CT = 'ct'

# Keys in metadata.json
# LOCATION = 'location'
ID = 'id'
STRIP_ID = 'strip_id'
# ORDER_ID = 'order_id'

# Order + scene manifests
# Manifest Keys
FILES = 'files'
MEDIA_TYPE = 'media_type'
PATH = 'path'
# Manifest Constants
# Keys in manifests
DIGESTS = 'digests'
ANNOTATIONS = 'annotations'
SIZE = 'size'
MD5 = 'md5'
SHA256 = 'sha256'
PLANET_ASSET_TYPE = 'planet/asset_type'
PLANET_BUNDLE_TYPE = 'planet/bundle_type'
PLANET_ITEM_ID = 'planet/item_id'
PLANET_ITEM_TYPE = 'planet/item_type'
# Fields added to scene level manifests when creating from
# order manifest
RECEIVED_DATETIME = 'received_datetime'
# Suffix to append to filenames for individual manifest files
MANIFEST_SUFFIX = 'manifest'
# Included in unusable data mask files paths
UDM = 'udm'
# Start of media_type in master manifest that indicates imagery
IMAGE = 'image'

# Footprinting
# Fields that are created
REL_LOCATION = 'rel_location'

# Off nadir csv
SAT_OFF_NADIR = 'sat.off_nadir'
SAT_AZIMUTH = 'sat.satellite_azimuth_mean'
