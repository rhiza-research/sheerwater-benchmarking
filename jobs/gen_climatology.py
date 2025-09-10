"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.climatology import (
    climatology_raw,  climatology_rolling_agg, climatology_linear_weights, climatology_agg_raw)
from sheerwater_benchmarking.utils import start_remote


# vars = ["precip"]
vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
# grids = ["global1_5"]
# grids = ["global0_25"]
# aggs = [7, 14]
# aggs = [7]
agg_days = [7, 14]

start_time = "1979-01-01"
end_time = "2024-12-31"
forecast_start_time = "2015-05-14"
forecast_end_time = "2023-06-30"
prob_types = ["deterministic", "probabilistic"]
# prob_types = ["deterministic"]
regions = ["global"]
masks = ["lsm"]
# 30 years after the start time
rolling_start_time = "2009-01-01"
clim_years = 30

first_year = 1985
last_year = 2014

first_val_year = 1991
last_val_year = 2020

UPDATE_CLIM = False
UPDATE_CLIM_ABC = False
UPDATE_CLIM_ROLLING = False
UPDATE_CLIM_TREND = False
UPDATE_CLIM_AGG = True
UPDATE_CLIM_ROLLING_ABC = False

start_remote(remote_config='xlarge_cluster')

for var, grid in product(vars, grids):
    # Update standard 30-year climatology
    if UPDATE_CLIM:
        ds = climatology_raw(var, first_year, last_year, grid=grid,
                             remote=True, recompute=True, force_overwrite=True)

    if UPDATE_CLIM_ABC:
        for mask, region in product(masks, regions):
            ds = climatology_abc(var, first_year, last_year, grid=grid,
                                 mask=mask, region=region,
                                 remote=True,  recompute=True, force_overwrite=True)

    for agg_days in agg_days:
        if UPDATE_CLIM_ROLLING:
            ds = climatology_rolling_agg(rolling_start_time, end_time, variable=var,
                                         clim_years=clim_years, agg_days=agg_days, grid=grid,
                                         remote=True, recompute=True, force_overwrite=True
                                         )

        if UPDATE_CLIM_ROLLING_ABC:
            for mask, region in product(masks, regions):
                ds = climatology_rolling_abc(rolling_start_time, end_time, variable=var,
                                             clim_years=clim_years, agg_days=agg_days, grid=grid,
                                             mask=mask, region=region,
                                             remote=True,  recompute=True, force_overwrite=True
                                             )

        if UPDATE_CLIM_TREND:
            ds = climatology_linear_weights(var, first_year=first_year, last_year=last_year,
                                            agg_days=agg_days, grid=grid,
                                            remote=True, recompute=True, force_overwrite=True
                                            )

        for prob_type in prob_types:
            if UPDATE_CLIM_AGG:
                ds = climatology_agg_raw(variable=var,
                                         first_year=first_year, last_year=last_year,
                                         prob_type=prob_type, agg_days=agg_days, grid=grid)
