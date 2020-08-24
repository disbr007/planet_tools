import argparse
import os
import pathlib
from pathlib import Path
import shutil

from tqdm import tqdm

from logging_utils.logging_utils import create_logger


logger = create_logger(__name__, 'sh', 'INFO')

tm_link = 'link'
tm_copy = 'copy'


def shelve_scenes(src_dir, dst_dir, transfer_method=tm_copy, dryrun=False):
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
        sid = '_'.join(sp.stem.split('_')[0:4])
        year = sp.stem[0:4]
        month = sp.stem[4:6]
        year_mo_dir = dst_dir / year / month
        if not year_mo_dir.exists():
            os.makedirs(year_mo_dir)

        scene_files = Path(sp).parent.glob('{}*'.format(sid))
        for sf in scene_files:
            df = year_mo_dir / sf.nam
            if df.exists():
                logger.debug('Destination file exists, skipping: {}'.format(sp.name))
                continue
            if not dryrun:
                if transfer_method == tm_link:
                    os.symlink(sf, df)
                else:
                    shutil.copy2(sf, df)
            pbar.write('Copied {} ->\n\t{}'.format(sp, df))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--src_dir', type=os.path.abspath,
                        help='Path to directory to shelve.')
    parser.add_argument('--dst_dir', type=os.path.abspath,
                        help='Path to destination directory')
    parser.add_argument('-tm', '--transfer_method',
                        help='Transfer method to use.')
    parser.add_argument('--dryrun', action='store_true')
    
    args = parser.parse_args()

    src_dir = args.src_dir
    dst_dir = args.dst_dir
    transfer_method = args.transfer_method
    dryrun = args.dryrun

    shelve_scenes(src_dir=src_dir, dst_dir=dst_dir,
                  transfer_method=transfer_method, dryrun=dryrun)