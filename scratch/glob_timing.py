import pandas as pd
import geopandas as gpd
from db_utils import Postgres

scenes = gpd.read_file(r'V:\pgc\data\scratch\jeff\projects\planet'
                       r'\multilook\multilook_scenes_onhand.shp')
pairs = gpd.read_file(r'V:\pgc\data\scratch\jeff\projects\planet'
                   r'\multilook\multilook_pairs.shp')

pairs['pan_filename_pairname'] = pairs.filename_p.apply(
    lambda x: '_pan-'.join(x.split('-')) + '_pan')

pairs.to_file(r'V:\pgc\data\scratch\jeff\projects\planet'
              r'\deliveries\2020oct14_multilook'
              r'\2020oct14_multilook_pairs.geojson',
              driver='GeoJSON')
pairs.to_csv(r'V:\pgc\data\scratch\jeff\projects\planet\deliveries\2020oct14_multilook\2020oct14_multilook_pairs.csv')


ml = gpd.read_file(r'V:\pgc\data\scratch\jeff\projects\planet\deliveries\2020oct14_multilook\2020oct14_multilook.geojson')

with Postgres('sandwich-pool.planet') as db_src:
    df = db_src.sql2df("SELECT * FROM scenes_metadata")


x = pd.merge(ml, df[['id', 'azimuth', 'off_nadir_signed']],
             on='id')

x.drop_duplicates(subset='id', inplace=True)
x.to_file(r'V:\pgc\data\scratch\jeff\projects\planet\deliveries\2020oct14_multilook\2020oct14_multilook.geojson',
          driver='GeoJSON')
x.to_csv(r'V:\pgc\data\scratch\jeff\projects\planet\deliveries\2020oct14_multilook\2020oct14_multilook.csv')