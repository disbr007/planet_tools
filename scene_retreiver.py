import os
from pathlib import Path
import shutil

from tqdm import tqdm

from db_utils import Postgres, ids2sql
from lib import read_ids, write_gdf
from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')

# Args
scene_ids_path = (r'V:\pgc\data\scratch\jeff\projects\planet\deliveries'
                  r'\2020aug17\front_range_stereo_pairs2020aug17_scenes.txt')
footprint_path = r''
destination_path = r'V:\pgc\data\scratch\jeff\projects\planet\deliveries\2020aug17\scenes'
out_footprint = None
transfer_method = 'copy'

# Params
db = 'sandwich-pool.planet'
scenes_onhand_table = 'scenes_onhand'
scene_id = 'id'
# TODO: Change this to 'location' and convert to platform specific path in script
location = 'win_loc'
opf = None
# Transfer methods
link = 'link'
copy = 'copy'

# Convert string paths to pathlib.Path
scene_ids_path = Path(scene_ids_path)
footprint_path = Path(footprint_path)
destination_path = Path(destination_path)


# Get scene ids
if scene_ids_path:
    scene_ids = read_ids(scene_ids_path)
    logger.info('Total IDs found: {}'.format(len(scene_ids)))
    scene_ids = set(scene_ids)
    logger.info('Unique IDs found: {}'.format(len(scene_ids)))


sql = """
SELECT * FROM {}
WHERE {} IN ({})""".format(scenes_onhand_table, scene_id,
                           ids2sql(scene_ids))

with Postgres(db) as db_src:
    gdf = db_src.sql2gdf(sql=sql)
    # TODO: Remove this, there should be no DUPs in scenes table
    gdf = gdf.drop_duplicates(subset=scene_id)
    logger.info('IDs found in {}: {}'.format(scenes_onhand_table, len(gdf)))

# Create glob generators for each scene to find to all scene files (metadata, etc.)
scene_path_globs = [Path(p).parent.glob('{}*'.format(sid)) for p, sid in zip(list(gdf[location]),
                                                                             list(gdf[scene_id]))]

logger.info('Locating scene files...')
src_files = []
for g in tqdm(scene_path_globs):
    for f in g:
        src_files.append(f)

# Create destination folder structure
# TODO: Option to build directory tree the same way we will index (and other options, --opf)
if opf:
    pass
else:
    src_dsts = [(src, destination_path / src.name) for src in src_files]

# Move files
# TODO: Make this a loop of src, dst using destinations built above
logger.info('Moving files...')
pbar = tqdm(src_dsts)
for sf, df in pbar:
    # Check for existence of destination path
    if df.exists():
        logger.debug('Destination file exists, skipping: {}'.format(sf.name))
        continue
    pbar.write('Copying {} -> {}'.format(sf, df))
    if transfer_method == link:
        os.symlink(sf, destination_path)
    else:
        shutil.copy2(sf, destination_path)

logger.info('File transfer complete.')

if out_footprint:
    logger.info('Writing footprint to file: {}'.format(out_footprint))
    write_gdf(gdf, out_footprint=out_footprint)

    logger.info('Footprint writing complete.')