import re
import os
from pathlib import Path
import glob

di = r'C:\temp'
d = r'C:\temp\scratch\*.shp'
p = Path(r'C:\scratch')
#%%
%%timeit
glob.glob(d)
#%%
%%timeit
p.rglob('*shp')
#%%
def find_shp(di):
    m = []
    for root, dirs, files in os.walk(di):
        for f in files:
            if f.endswith('shp'):
            # if re.match('.*shp', f):
                m.append(f)

%timeit find_shp(di)
#%%