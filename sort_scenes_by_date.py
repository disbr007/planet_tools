import argparse
import os
from pathlib import Path
import shutil

from tqdm import tqdm

from lib.logging_utils import create_logger


logger = create_logger(__name__, 'sh', 'INFO')

tm_link = 'link'
tm_copy = 'copy'


def sort_scene_by_date(src_dir, dst_dir,
                       year_dir=True, month_dir=True, day_dir=True,
                       hour_dir=False, minute_dir=False, second_dir=False,
                       transfer_method=tm_copy, dryrun=False):
    data_dir = Path(src_dir)
    dst_dir = Path(dst_dir)

    logger.info('Locating scene files...')
    scenes = data_dir.rglob('*.tif')

    logger.info('Copying scenes to shelved locations...')
    pbar = tqdm(scenes)
    for s in pbar:
        sp = Path(s)
        if 'udm' in sp.stem:
            continue
        # TODO: Improve finding SID / scene file, etc
        sid = '_'.join(sp.stem.split('_')[0:3])
        year = sp.stem[0:4]
        month = sp.stem[4:6]
        day = sp.stem[6:8]
        hour = sp.stem[10:12]
        minute = sp.stem[12:14]
        second = sp.stem[14:16]
        subdir = dst_dir
        if year_dir:
            subdir = subdir / year
        if month_dir:
            subdir = subdir / month
        if day_dir:
            subdir = subdir / day
        if hour_dir:
            subdir = subdir / hour
        if minute_dir:
            subdir = subdir / minute
        if second_dir:
            subdir = subdir / second

        if not subdir.exists():
            os.makedirs(subdir)

        # TODO: Change this to use PlanetScene.scene_files
        scene_files = Path(sp).parent.glob('{}*'.format(sid))
        for sf in scene_files:
            df = subdir / sf.name
            if df.exists():
                logger.debug('Destination file exists, skipping: {}'.format(sp.name))
                continue
            if not dryrun:
                if transfer_method == tm_link:
                    os.link(sf, df)
                else:
                    shutil.copy2(sf, df)
            # pbar.write('Copied {} ->\n\t{}'.format(sf, df))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--src_dir', type=os.path.abspath,
                        help='Path to directory to shelve.')
    parser.add_argument('--dst_dir', type=os.path.abspath,
                        help='Path to destination directory')
    parser.add_argument('-tm', '--transfer_method', choices=[tm_copy, tm_link],
                        help='Transfer method to use.')
    parser.add_argument('--dryrun', action='store_true')
    
    args = parser.parse_args()

    src_dir = args.src_dir
    dst_dir = args.dst_dir
    transfer_method = args.transfer_method
    dryrun = args.dryrun

    sort_scene_by_date(src_dir=src_dir, dst_dir=dst_dir,
                       transfer_method=transfer_method, dryrun=dryrun)