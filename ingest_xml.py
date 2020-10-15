import os

from tqdm import tqdm
import pandas as pd

from db_utils import Postgres
from logging_utils.logging_utils import create_logger
from index_utils import attributes_from_xml

logger = create_logger(__name__, 'sh', 'INFO')


data_dir = r'V:\pgc\data\scratch\jeff\projects\planet\data'
xml_table = 'xml_metadata'

xmls = []
for root, dirs, files in os.walk(data_dir):
    for f in files:
        if f.endswith('metadata.xml'):
            xmls.append(os.path.join(root, f))


logger.info('Parsing XML files...')
all_atts = []
for xml in tqdm(xmls):
    attributes = attributes_from_xml(xml)sc
    all_atts.append(attributes)


logger.info('Converting to dataframe...')
xml_df = pd.DataFrame(all_atts)

logger.info('Adding rows to table: {}'.format(xml_table))
with Postgres('sandwich-pool.planet') as db:
    db.insert_new_records(records=xml_df, table=xml_table,
                          unique_id='identifier')
