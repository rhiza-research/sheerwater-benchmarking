"""Runs metrics and updates the caches."""
import argparse
import itertools

from sheerwater_benchmarking.metrics import summary_metrics_table

parser = argparse.ArgumentParser()
parser.add_argument("--start-time", default="2016-01-01", type=str)
parser.add_argument("--end-time", default="2023-01-01", type=str)
parser.add_argument("--baseline", type=str, nargs='*')
parser.add_argument("--variable", type=str, nargs='*')
parser.add_argument("--metric", type=str, nargs='*')
parser.add_argument("--grid", type=str, nargs='*')
parser.add_argument("--region", type=str, nargs='*')
parser.add_argument("--time-grouping", type=str, nargs='*')
args = parser.parse_args()

baselines = ["ecmwf_ifs_er", "ecmwf_ifs_er_debiased",
             "climatology_2015", "climatology_trend_2015", "climatology_rolling"]
if args.baseline:
    baselines = args.baseline

metrics = ["mae", "crps", "acc", "bias", "rmse"]
if args.metric:
    metrics = args.metric

variables = ["precip", "tmp2m"]
if args.variable:
    variables = args.variable

grids = ["global0_25", "global1_5"]
if args.grid:
    grids = args.grid

regions = ["africa", "east_africa", "global", "conus"]
if args.region:
    regions = args.region

time_groupings = [None, "month_of_year", "year"]
if args.time_grouping:
    time_groupings = args.time_grouping
    time_groupings = [x if x != 'None' else None for x in time_groupings]

combos = itertools.product(metrics, variables, grids, baselines, regions, time_groupings)
for metric, variable, grid, baseline, region, time_grouping in combos:
    print(metric, baseline)
    summary_metrics_table(args.start_time, args.end_time, variable, "era5", metric,
                          baseline=baseline, time_grouping=time_grouping, grid=grid, region=region,
                          remote=True, force_overwrite=True, backend='postgres', recompute=True,
                          remote_config=['large_scheduler', 'xxlarge_cluster', 'large_node'])
