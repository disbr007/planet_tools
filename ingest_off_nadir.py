import os

import pandas as pd

export_dir = r'V:\pgc\data\scratch\jeff\projects\planet\scenes\paris_export\pl_fp_2019oct01_2020jun30\nasa_csv_PE-33905'

for csv in os.listdir(export_dir):
    csv_path = os.path.join(export_dir, csv)
