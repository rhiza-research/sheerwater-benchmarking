"""Generate land-sea masks for all grids and bases."""
from itertools import product
from sheerwater_benchmarking.climatology import (climatology_raw, climatology_rolling_raw, climatology_agg)
from sheerwater_benchmarking.baselines.climatology import (climatology_rolling_agg, climatology_forecast)


vars = ["tmp2m", "precip"]
grids = ["global0_25", "global1_5"]
aggs = [7, 14]

start_time = "1979-01-01"
end_time = "2025-01-01"
forecast_start_time = "2015-05-14"
forecast_end_time = "2023-06-30"
prob_types = ["deterministic", "probabilistic"]
# 30 years after the start time
rolling_start_time = "2009-01-01"
clim_years = 30

first_year = 1986
last_year = 2015

UPDATE_CLIM = False
UPDATE_CLIM_ROLLING = False
UPDATE_CLIM_FCST = True
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

    if UPDATE_CLIM_ROLLING:
        ds = climatology_rolling_raw(rolling_start_time, end_time, variable=var, clim_years=clim_years, grid=grid,
                                     remote=True, remote_config={
                                         'n_workers': 10,
                                         'idle_timeout': '240 minutes',
                                         'name': 'genevieve'
                                     },
                                     recompute=True, force_overwrite=True)

    for agg in aggs:
        if UPDATE_CLIM_FCST:
            for prob_type in prob_types:
                ds = climatology_forecast(forecast_start_time, forecast_end_time, variable=var,
                                          first_year=first_year, last_year=last_year,
                                          prob_type=prob_type,
                                          grid=grid, agg=agg,
                                          recompute=True, force_overwrite=True,
                                          remote=True, remote_config={
                                              'n_workers': 10,
                                              'idle_timeout': '240 minutes',
                                              'name': 'genevieve'
                                          },
                                          )
        if UPDATE_CLIM_FCST_ROLLING:
            ds = climatology_rolling_agg(rolling_start_time, end_time, variable=var,
                                         clim_years=clim_years,
                                         grid=grid, agg=agg,
                                         recompute=True, force_overwrite=True,
                                         remote=True,
                                         remote_config={
                                             'name': 'genevieve2',
                                             'n_workers': 10,
                                             'idle_timeout': '240 minutes'
                                         })
