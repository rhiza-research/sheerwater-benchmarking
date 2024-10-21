"""Runs metrics and updates the caches."""
from sheerwater_benchmarking.metrics import spatial_metric
import itertools

supported_metrics = {
    "salient": ["mae", "crps"],
    "ecmwf_er": ["mae"]
}

supported_grids = {
    "salient": ["global1_5", "global0_25"],
    "ecmwf_er": ["global1_5"]
}


start_year = "2016"
end_year = "2022"

# define all the variables to run over
forecasts = ["salient", "ecmwf_er"]
truths = ["era5"]
baselines = [None, "ecmwf_er"]
leads = ["week1", "week2", "week3", "week4", "week5"]
metrics = ["mae", "crps"]
variables = ["precip", "tmp2m"]
#grids = ["global1_5", "global0_25"]
grids = ["global1_5"]

# Make a loop that iterates these combinations
combos = itertools.product(forecasts, truths, baselines, leads, metrics, variables, grids)
for run in combos:

    forecast = run[0]
    truth = run[1]
    baseline = run[2]
    lead = run[3]
    metric = run[4]
    variable = run[5]
    grid = run[6]

    # Make sure the forecasts and baseline isn't the same
    if forecast == baseline:
        print("Skipping run of same forecast and baseline")
        continue

    # Make sure the forecast and baseline support the metric
    if metric not in supported_metrics[forecast] or (baseline and metric not in supported_metrics[baseline]):
        print("Skipping run for unsupported metric")
        continue

    # Call the spatial metric with the terracotta backend
    print(f"Running metric {run}")
    spatial_metric(start_year, end_year, variable, lead, forecast, truth, metric, baseline, grid=grid,
                   cache=True, backend='terracotta', force_overwrite=True, remote=True)

