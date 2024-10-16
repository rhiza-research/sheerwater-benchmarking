"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.climatology import climatology_raw
from sheerwater_benchmarking.baselines import climatology_agg


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
aggs = [1, 7, 14]
masks = ["lsm"]

start_time = "1979-01-01"
end_time = "2025-01-01"

first_year = 1991
last_year = 2020

UPDATE_CLIM = True
UPDATE_CLIM_FCST = False

for var, grid in product(vars, grids):
    # Update standard 30-year climatology
    if UPDATE_CLIM:
        ds = climatology_raw(var, first_year, last_year, grid=grid,
                             remote=True, remote_config={
                                 'n_workers': 10,
                                 'idle_timeout': '240 minutes',
                                 'name': 'genevieve'
                             },
                             recompute=True, force_overwrite=True)

    if UPDATE_CLIM_FCST:
        for agg in aggs:
            ds = climatology_agg(start_time, end_time, variable=var,
                                 first_year=first_year, last_year=last_year,
                                 grid=grid, agg=agg, mask=mask,
                                 recompute=True, force_overwrite=True,
                                 remote=True, remote_config={
                                     'n_workers': 10,
                                     'idle_timeout': '240 minutes',
                                     'name': 'genevieve'
                                 },
                                 )
