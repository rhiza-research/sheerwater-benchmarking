

from subprocess import Popen
import pandas as pd
import os

dates = pd.date_range("2015-01-01", "2023-01-01", freq='90D')
date_tups = []
for i, d in enumerate(dates):
    if i == 0:
        continue

    date_tups.append((dates[i-1].date(), dates[i].date()))

for start, end in date_tups:
    cmd = f'coiled run --detach --forward-gcp-adc -- python tools/fuxi-download.py --start-time {start} --end-time {end} > /dev/null 2>&1 &'
    #cmd = ['coiled', 'run',  f'python tools/fuxi-download.py --start-time {start} --end-time {end}']

    os.system(cmd)
