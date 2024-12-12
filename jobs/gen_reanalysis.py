"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.reanalysis import era5_agg
from sheerwater_benchmarking.reanalysis.era5 import era5_rolled, era5_daily, era5_daily_regrid
from sheerwater_benchmarking.utils import start_remote


vars = ["tmp2m", "precip"]
# grids = ['global0_25']
# grids = ['global1_5']
grids = ['global0_25', 'global1_5']
regions = ['global']
aggs = [14, 7]
# aggs = [14, 7]
masks = ["lsm"]
anoms = [True, False]
# anoms = [False]
clim_params = {'first_year': 1985, 'last_year': 2014}

start_time = "1979-01-01"
end_time = "2024-12-31"

UPDATE_DAILY = False
UPDATE_DAILY_REGRID = True
UPDATE_ROLLED = False
UPDATE_AGG = False
FLAG = False

start_remote(remote_config='xlarge_cluster')

for var, grid in product(vars, grids):
    if UPDATE_DAILY:
        ds = era5_daily(start_time, end_time, variable=var, grid=grid,
                        recompute=True, remote=True, force_overwrite=True,
                        remote_name='genevieve', remote_config='xlarge_cluster')
    if UPDATE_DAILY_REGRID:
        ds = era5_daily_regrid(start_time, end_time, variable=var, method='conservative', grid=grid,
                               recompute=True, remote=True, force_overwrite=True)

    for agg, anom in product(aggs, anoms):
        if UPDATE_ROLLED:
            # Update the rolled data for global grids
            ds = era5_rolled(start_time, end_time, variable=var,
                             agg_days=agg, grid=grid,
                             recompute=True, remote=True, force_overwrite=True,
                             remote_name='genevieve', remote_config='xlarge_cluster')

        for mask, region in product(masks, regions):
            if UPDATE_AGG:
                ds = era5_agg(start_time, end_time, variable=var,
                              agg_days=agg, anom=anom, clim_params=clim_params,
                              grid=grid, mask=mask, region=region,
                              remote=True, remote_config='xlarge_cluster', remote_name='lead-bias',
                              force_overwrite=True,
                              #   recompute=True, force_overwrite=True,
                              )
