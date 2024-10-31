"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.baselines.climatology import (
    climatology_raw,  climatology_rolling_agg, climatology_linear_weights, climatology_agg_raw,
    climatology_abc, climatology_rolling_abc)


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
# grids = ["global1_5"]
# grids = ["global0_25"]
aggs = [7, 14]

start_time = "1979-01-01"
end_time = "2024-01-01"
forecast_start_time = "2015-05-14"
forecast_end_time = "2023-06-30"
# prob_types = ["deterministic", "probabilistic"]
prob_types = ["deterministic"]
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

for var, grid in product(vars, grids):
    # Update standard 30-year climatology
    if UPDATE_CLIM:
        ds = climatology_raw(var, first_year, last_year, grid=grid,
                             remote=True, remote_name='genevieve', remote_config='xlarge_cluster',
                             recompute=True, force_overwrite=True)

    if UPDATE_CLIM_ABC:
        for mask, region in product(masks, regions):
            ds = climatology_abc(var, first_year, last_year, grid=grid,
                                 mask=mask, region=region,
                                 remote=True, remote_name='genevieve', remote_config='xlarge_cluster',
                                 recompute=True, force_overwrite=True)

    for agg in aggs:
        if UPDATE_CLIM_ROLLING:
            ds = climatology_rolling_agg(rolling_start_time, end_time, variable=var,
                                         clim_years=clim_years, agg=agg, grid=grid,
                                         remote=True, remote_name='genevieve', remote_config='xlarge_cluster',
                                         recompute=True, force_overwrite=True
                                         )

        if UPDATE_CLIM_ROLLING_ABC:
            for mask, region in product(masks, regions):
                ds = climatology_rolling_abc(rolling_start_time, end_time, variable=var,
                                             clim_years=clim_years, agg=agg, grid=grid,
                                             mask=mask, region=region,
                                             remote=True, remote_name='genevieve', remote_config='xlarge_cluster',
                                             recompute=True, force_overwrite=True
                                             )

        if UPDATE_CLIM_TREND:
            ds = climatology_linear_weights(var, first_year=first_year, last_year=last_year,
                                            agg=agg, grid=grid,
                                            remote=True, remote_name='genevieve', remote_config='xlarge_cluster',
                                            recompute=True, force_overwrite=True)

        for prob_type in prob_types:
            if UPDATE_CLIM_AGG:
                # ds = climatology_agg_raw(variable=var,
                #                          first_year=first_year, last_year=last_year,
                #                          prob_type=prob_type, agg=agg, grid=grid,
                #                          recompute=True, force_overwrite=True,
                #                          remote=True, remote_name='genevieve', remote_config='xlarge_cluster'
                #                          )
                ds = climatology_agg_raw(variable=var,
                                         first_year=first_val_year, last_year=last_val_year,
                                         prob_type=prob_type, agg=agg, grid=grid,
                                         recompute=True, force_overwrite=True,
                                         remote=True, remote_name='genevieve', remote_config='xlarge_cluster'
                                         )
