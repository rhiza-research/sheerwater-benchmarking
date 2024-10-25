"""Runs metrics and updates the caches."""
from sheerwater_benchmarking.metrics import summary_metrics_table
import itertools

supported_metrics = {
    "climatology_rolling": ["mae"],
    "climatology_2015": ["mae"],
    "salient": ["mae", "crps"],
    "ecmwf_ifs_er": ["mae"],
    "ecmwf_er_deb": ["mae"],
}

supported_grids = {
    "climatology_rolling": ["global1_5", "global0_25"],
    "climatology_2015": ["global1_5", "global0_25"],
    "salient": ["global1_5", "global0_25"],
    "ecmwf_ifs_er": ["global1_5"],
    "ecmwf_er_deb": ["global1_5"],
}


start_year = "2016"
end_year = "2022"

# define all the variables to run over
forecasts = ["salient", "ecmwf_ifs_er"]
truths = ["era5"]
#baselines = [None, "ecmwf_ifs_er", "climatology_rolling", "climatology_2015"]
#baselines = [None, "ecmwf_er", "climatology_rolling", "climatology_2015"]
#baselines = ["ecmwf_ifs_er", "ecmwf_ifs_er_debiased", "climatology_2015"]
baselines = ["ecmwf_ifs_er", "climatology_2015"]
leads = ["week1", "week2", "week3", "week4", "week5"]
#metrics = ["mae", "crps"]
metrics = ["mae"]
variables = ["precip", "tmp2m"]
#grids = ["global1_5", "global0_25"]
#grids = ["global1_5"]
grids = ["global0_25"]
region = "east_africa"

# Make a loop that iterates these combinations
#combos = itertools.product(forecasts, truths, baselines, leads, metrics, variables, grids)
#for forecast, truth, baseline, lead, metric, variable, grid in combos:
#    # Make sure the forecasts and baseline isn't the same
#    if forecast == baseline:
#        print("Skipping run of same forecast and baseline")
#        continue
#
#    # Make sure the forecast and baseline support the metric
#    if metric not in supported_metrics[forecast] or (baseline and metric not in supported_metrics[baseline]):
#        print("Skipping run for unsupported metric")
#        continue
#
#    # Call the spatial metric with the terracotta backend
#    run = f"{forecast}_{truth}_{baseline}_{lead}_{metric}_{variable}_{grid}"
#    print(f"Running metric {run}")
#    spatial_metric(start_year, end_year, variable, lead, forecast, truth, metric, baseline, grid=grid,
#                   cache=True, backend='terracotta', force_overwrite=True, remote=True)

combos = itertools.product(metrics, variables, grids, baselines)
for metric, variable, grid, baseline in combos:
    print(metric, baseline)
    summary_metrics_table("2016-01-01", "2023-01-01", variable, "era5", metric,
                          baseline=baseline, grid=grid, region=region,
                          remote=True, force_overwrite=True, backend='postgres',
                          remote_config=['big_scheduler','xxlarge_cluster','big_node'])
