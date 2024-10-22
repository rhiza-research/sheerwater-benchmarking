"""Runs metrics and updates the caches."""
from sheerwater_benchmarking.metrics import spatial_metric
import itertools

from sheerwater_benchmarking.baselines import
supported_metrics = {
    "climatology_rolling_forecast": ["mae"],
    "climatology_forecast_2015": ["mae"],
    "salient": ["mae", "crps"],
    "ecmwf_er": ["mae"],
}

supported_grids = {
    "climatology_rolling_forecast": ["global1_5", "global0_25"],
    "climatology_forecast_2015": ["global1_5", "global0_25"],
    "salient": ["global1_5", "global0_25"],
    "ecmwf_er": ["global1_5"],
}


start_year = "2016"
end_year = "2022"

# define all the variables to run over
forecasts = ["salient", "ecmwf_er"]
truths = ["era5"]
baselines = [None, "ecmwf_er", "climatology_rolling_forecast", "climatology_forecast_2015"]
leads = ["week1", "week2", "week3", "week4", "week5"]
metrics = ["mae", "crps"]
variables = ["precip", "tmp2m"]
grids = ["global1_5", "global0_25"]

# Make a loop that iterates these combinations
combos = itertools.product(forecasts, truths, baselines, leads, metrics, variables, grids)
for forecast, truth, baseline, lead, metric, variable, grid in combos:
    # Make sure the forecasts and baseline isn't the same
    if forecast == baseline:
        print("Skipping run of same forecast and baseline")
        continue

    # Make sure the forecast and baseline support the metric
    if metric not in supported_metrics[forecast] or (baseline and metric not in supported_metrics[baseline]):
        print("Skipping run for unsupported metric")
        continue

    # Call the spatial metric with the terracotta backend
    run = f"{forecast}_{truth}_{baseline}_{lead}_{metric}_{variable}_{grid}"
    print(f"Running metric {run}")
    spatial_metric(start_year, end_year, variable, lead, forecast, truth, metric, baseline, grid=grid,
                   cache=True, backend='terracotta', force_overwrite=True, remote=True)
