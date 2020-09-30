import argparse
from pathlib import Path
import os
import shutil

from tqdm import tqdm

from logging_utils.logging_utils import create_logger

logger = create_logger(__name__, 'sh', 'INFO')


def structure_scenes(src_dir, dst_dir, days=False, move=False, dryrun=False):
    src_dir = Path(src_dir)
    dst_dir = Path(dst_dir)
    logger.info('Parsing source directory and determining destination file paths...')
    srcs_dsts = []
    for root, dirs, files in os.walk(src_dir):
        for f in files:
            year = f[:4]
            month = f[4:6]
            day = f[6:8]

            dst = dst_dir / year / month
            if days == True:
                dst = dst / day

            dst = dst / f

            src_dst = (Path(os.path.join(root, f)), dst)
            srcs_dsts.append(src_dst)

    logger.info('Performing copies/moves...')
    # TODO: Make function in lib: do_copy(srcs_dsts, move=False, dryrun=False)
    for src, dst in tqdm(srcs_dsts):
        if not dryrun:
            if not dst.parent.exists():
                os.makedirs(dst.parent)
            if not dst.exists():
                try:
                    if move:
                        shutil.move(src, dst)
                    else:
                        shutil.copy2(src, dst)
                except:
                    logger.warning('Error copying: {}\n\t->{}'.format(src, dst))
                    raise
            else:
                logger.debug('Destination exists, skipping: {}'.format(dst))

    logger.info('Done.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('src_directory', type=os.path.abspath,
                        help='Directory holding scenes.')
    parser.add_argument('dst_directory', type=os.path.abspath,
                        help='Parent directory to build year/month/(day) '
                             'structure within.')
    parser.add_argument('--days', action='store_true',
                        help='Use to create folders for each day.')
    parser.add_argument('--move', action='store_true',
                        help='Move files instead of copying.')
    parser.add_argument('--dryrun', action='store_true',
                        help='Print actions without performing.')

    args = parser.parse_args()

    structure_scenes(src_dir=args.src_directory, dst_dir=args.dst_directory,
                     days=args.days, move=args.move,
                     dryrun=args.dryrun)
