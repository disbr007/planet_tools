import os
from pathlib import Path
import json

import geopandas as gpd

from logging_utils.logging_utils import create_logger, create_logfile_path
from db_utils import Postgres


logger = create_logger(__name__, 'sh', 'INFO')
# logger = create_logger(__name__, 'fh', 'DEBUG',
                       # filename=create_logfile_path(Path(__file__).stem))

create_logfile_path(Path(__file__).stem)

