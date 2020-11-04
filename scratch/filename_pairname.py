import geopandas as gpd

from pathlib import Path
from lib.lib import Postgres


scenes_onhand = 'scenes_onhand'
so_id = 'id'
pairname_field = 'pairname'
filename_field = 'filename'

def get_ids_from_multilook(df, src_id_fld, pairname_fld):
    src_ids = list(df[src_id_fld].values)
    pair_ids = [i for sublist in df[pairname_fld].str.split('-').values for i in sublist]
    all_ids = set(src_ids + pair_ids)

    return all_ids


def get_filename_pairname(pairname, lut_df, id_fld=so_id, filename_field=filename_field):
    ids = pairname.split('-')
    filename_pairname_strs = []
    for i in ids:
        filename = lut_df.loc[df[id_fld] == i][filename_field].values
        if filename.any():
            if len(filename) == 1:
                filename = Path(filename[0]).stem
                filename_pairname_strs.append(filename)
            # else:
            #     logger.debug('Multiple records found for ID: {}'.format(i))
        # else:
        #     logger.debug('No record found in {} for ID: {}'.format(scenes_onhand, i))

    filename_pairname = '-'.join(filename_pairname_strs)

    return filename_pairname


multilook_p = r'E:\disbr007\projects\planet\multilook\ml_test_all.shp'

m = gpd.read_file(multilook_p)

all_ids = get_ids_from_multilook(m, 'id', 'pairname')

with Postgres('sandwich-pool.planet') as db_src:
    sql = "SELECT * FROM {} WHERE {} IN ({})".format(scenes_onhand, so_id, str(all_ids)[1:-1])
    df = db_src.sql2df(sql)


m['filename_pairname'] = m[pairname_field].apply(lambda x: get_filename_pairname(x, df))
