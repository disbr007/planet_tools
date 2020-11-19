
import geopandas as gpd
from tqdm import tqdm

from lib.search import create_search
from lib.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# Params
search_name = 'antarctica_geocells'
item_types = ['PSScene4Band']
name_field = 'name'
count_field = 'search_count'
# Load geocells
antarctic_geocells_p = r'E:\disbr007\projects\planet\scratch' \
                       r'\ant_geocells_all.shp'

geocells = gpd.read_file(antarctic_geocells_p)
geocells.set_index(name_field, inplace=True)
geocells[count_field] = 0

# Iterate over each geocell, getting count for each
for i, r in tqdm(geocells.iterrows(), total=len(geocells)):
    cell_name = r.name
    gc = gpd.GeoDataFrame([r], geometry='geometry',
                          crs=geocells.crs)
    _, gc_ct = create_search(name=search_name,
                          item_types=item_types,
                          aoi=gc,
                          get_count_only=True)
    if gc_ct != 0:
        print(gc_ct)
    geocells.at[cell_name, count_field] = gc_ct