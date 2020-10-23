import pandas as pd
import geopandas as gpd
from db_utils import Postgres

# scenes = gpd.read_file(r'V:\pgc\data\scratch\jeff\projects\planet'
# #                        r'\multilook\multilook_scenes_onhand.shp')
# pairs = gpd.read_file(r'V:\pgc\data\scratch\jeff\projects\planet\deliveries\2020oct14_multilook_redo.geojson')
#
# pairs['pan_filename_pairname'] = pairs['fn_pairname'].apply(
#     lambda x: '_pan-'.join(x.split('-')) + '_pan')
#
# all_good = pairs.apply(lambda x: (len(x['pan_filename_pairname'].split('-')) == x['pair_count']), axis=1)
# pairs.to_file(r'V:\pgc\data\scratch\jeff\projects\planet'
#               r'\deliveries\2020oct14_multilook'
#               r'\2020oct14_multilook_pairs.geojson',
#               driver='GeoJSON')
# pairs.to_csv(r'V:\pgc\data\scratch\jeff\projects\planet\deliveries\2020oct14_multilook\2020oct14_multilook_pairs.csv')
print('loading local scenes')
with Postgres('sandwich-pool.planet') as db_src:
    gdf = db_src.sql2gdf("SELECT * FROM scenes")
print('local scenes loaded: {}'.format(len(gdf)))
print('writing')
gdf.to_file(r'V:\pgc\data\scratch\jeff\projects\planet'
            r'\scenes\local_scenes.geojson',
            driver='GeoJSON')
print('done')