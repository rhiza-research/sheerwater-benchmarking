"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.reanalysis import era5_agg
from sheerwater_benchmarking.reanalysis.era5 import era5_rolled


vars = ["tmp2m", "precip"]
grids = ['global0_25', 'global1_5']
regions = ['global']
aggs = [7, 14]
masks = ["lsm"]
anoms = [True, False]
clim_params = {'first_year': 1991, 'last_year': 2020}

start_time = "1979-01-01"
end_time = "2024-11-01"

UPDATE_ROLLED = True
UPDATE_AGG = False

for var, agg, grid, anom in product(vars, aggs, grids, anoms):
    if UPDATE_ROLLED:
        # Update the rolled data for global grids
        cp = clim_params if anom else None
        ds = era5_rolled(start_time, end_time, variable=var, grid=grid, agg=agg,
                         anom=anom, clim_params=cp,
                         recompute=False, remote=True, force_overwrite=False,
                         remote_config={'name': 'genevieve'})

    if UPDATE_AGG:
        for mask, region in product(masks, regions):
            cp = clim_params if anom else None
            ds = era5_agg(start_time, end_time, variable=var, grid=grid, agg=agg,
                          anom=anom, clim_params=cp,
                          mask=mask, region=region,
                          recompute=True, remote=True, force_overwrite=True)
