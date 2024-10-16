"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.climatology import climatology
from sheerwater_benchmarking.baselines import climatology_agg


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
aggs = [1, 7, 14]
masks = ["lsm"]

start_time = "1979-01-01"
end_time = "2025-01-01"

first_year = 1991
last_year = 2020

UPDATE_CLIM = False
UPDATE_CLIM_FCST = True

for var, agg, grid, mask in product(vars, aggs, grids, masks):
    # Update standard 30-year climatology
    if UPDATE_CLIM:
        ds = climatology(first_year, last_year, variable=var, grid=grid, mask=mask,
                         recompute=True, remote=True, force_overwrite=True)

    if UPDATE_CLIM_FCST:
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
