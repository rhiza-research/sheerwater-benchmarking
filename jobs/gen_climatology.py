"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.climatology import (climatology_raw, climatology_agg,
                                                 climatology_rolling_agg, climatology_trend)


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
# grids = ["global0_25"]
aggs = [7, 14]

start_time = "1979-01-01"
end_time = "2025-01-01"
forecast_start_time = "2015-05-14"
forecast_end_time = "2023-06-30"
prob_types = ["deterministic", "probabilistic"]
# prob_types = ["deterministic"]
# 30 years after the start time
rolling_start_time = "2009-01-01"
clim_years = 30

first_year = 1985
last_year = 2014

UPDATE_CLIM = False
UPDATE_CLIM_ROLLING = True 
UPDATE_CLIM_TREND = False 
UPDATE_CLIM_AGG = False 
UPDATE_CLIM_FCST = False
UPDATE_CLIM_FCST_ROLLING = False

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

    for agg in aggs:
        if UPDATE_CLIM_ROLLING:
            ds = climatology_rolling_agg(rolling_start_time, end_time, variable=var,
                                         clim_years=clim_years, agg=agg, grid=grid,
                                         remote=True, remote_config={
                                             'n_workers': 10,
                                             'idle_timeout': '240 minutes',
                                             'name': 'genevieve'
                                         },
                                        #  recompute=True, force_overwrite=True
                                        )

        if UPDATE_CLIM_TREND:
            ds = climatology_trend(var, first_year=first_year, last_year=last_year,
                                    agg=agg, grid=grid,
                                    remote=True, remote_config={
                                        'n_workers': 25,
                                        'idle_timeout': '240 minutes',
                                        'name': 'genevieve2'
                                    },
                                    recompute=True, force_overwrite=True)

        for prob_type in prob_types:
            if UPDATE_CLIM_AGG:
                ds = climatology_agg(variable=var,
                                     first_year=first_year, last_year=last_year,
                                     prob_type=prob_type, agg=agg, grid=grid,
                                    #  recompute=True, force_overwrite=True,
                                     remote=True, remote_config={
                                         'n_workers': 10,
                                         'idle_timeout': '240 minutes',
                                         'name': 'genevieve'
                                     },
                                     )
            # if UPDATE_CLIM_FCST:
            #     ds = climatology_timeseries(forecast_start_time, forecast_end_time, variable=var,
            #                                 first_year=first_year, last_year=last_year,
            #                                 prob_type=prob_type,
            #                                 grid=grid, agg=agg,
            #                                 recompute=True, force_overwrite=True,
            #                                 remote=True, remote_config={
            #                                     'n_workers': 10,
            #                                     'idle_timeout': '240 minutes',
            #                                     'name': 'genevieve'
            #                                 },
            #                                 )
