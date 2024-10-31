"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.reanalysis import era5_agg
from sheerwater_benchmarking.reanalysis.era5 import era5_rolled, era5_daily, era5_daily_regrid


vars = ["tmp2m", "precip"]
grids = ['global0_25']
# grids = ['global0_25', 'global1_5']
regions = ['global']
aggs = [14, 7]
masks = ["lsm"]
# anoms = [True, False]
anoms = [False]
clim_params = {'first_year': 1991, 'last_year': 2020}

start_time = "1979-01-01"
end_time = "2024-11-01"

UPDATE_DAILY = False
UPDATE_DAILY_REGRID = False
UPDATE_ROLLED = True
UPDATE_AGG = False

FLAG = False
for var, grid in product(vars, grids):
    if UPDATE_DAILY:
        ds = era5_daily(start_time, end_time, variable=var, grid=grid,
                        recompute=True, remote=True, force_overwrite=True,
                        remote_name='genevieve', remote_config='xlarge_cluster')
    if UPDATE_DAILY_REGRID:
        ds = era5_daily_regrid(start_time, end_time, variable=var, grid=grid,
                               recompute=True, remote=True, force_overwrite=True,
                               remote_name='genevieve', remote_config='xlarge_cluster')

    for agg, anom in product(aggs, anoms):
        if UPDATE_ROLLED:
            # Update the rolled data for global grids
            ds = era5_rolled(start_time, end_time, variable=var,
                             agg=agg, grid=grid,
                             recompute=True, remote=True, force_overwrite=True,
                             remote_name='genevieve', remote_config='xlarge_cluster')

        for mask, region in product(masks, regions):
            if UPDATE_AGG:
                ds = era5_agg(start_time, end_time, variable=var,
                              agg=agg, anom=anom, clim_params=clim_params,
                              grid=grid, mask=mask, region=region,
                              recompute=True, remote=True, force_overwrite=True,
                              remote_config={'name': 'genevieve'})
