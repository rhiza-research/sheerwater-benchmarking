"""Runs metrics and updates the caches."""
import argparse
import itertools
import traceback

import dask

from sheerwater_benchmarking.metrics import global_metric
from sheerwater_benchmarking.utils import start_remote

parser = argparse.ArgumentParser()
parser.add_argument("--start-time", default="2016-01-01", type=str)
parser.add_argument("--end-time", default="2022-12-31", type=str)
parser.add_argument("--forecast", type=str, nargs='*')
parser.add_argument("--variable", type=str, nargs='*')
parser.add_argument("--metric", type=str, nargs='*')
parser.add_argument("--grid", type=str, nargs='*')
parser.add_argument("--lead", type=str, nargs='*')
parser.add_argument("--remote", action=argparse.BooleanOptionalAction, default=True)
args = parser.parse_args()

forecasts = ["salient", "ecmwf_ifs_er", "ecmwf_ifs_er_debiased",
             "climatology_2015", "climatology_trend_2015", "climatology_rolling"]
if args.forecast:
    forecastss = args.forecast

metrics = ["mae", "crps", "acc", "rmse", "bias"]
if args.metric:
    metrics = args.metric

variables = ["precip", "tmp2m"]
if args.variable:
    variables = args.variable

grids = ["global0_25", "global1_5"]
if args.grid:
    grids = args.grid

leads = ["week1", "week2", "week3", "week4", "week5", "week6"]
if args.lead:
    leadss = args.lead

if args.remote:
    start_remote(remote_config=['xlarge_cluster', 'large_scheduler'])

#combos = itertools.product(metrics, variables, grids, baselines, regions, time_groupings)
combos = itertools.product(metrics, variables, grids, leads, forecasts)
for metric in metrics:
    for variable in variables:
        for grid in grids:
            for lead in leads:
                for forecast in forecasts:
                    try:
                        global_metric(args.start_time, args.end_time, variable, lead, forecast, "era5", metric, grid=grid)
                    except Exception as e:
                        print(f"Failed to run global metric {forecast} {lead} {grid} {variable} {metric}: {traceback.format_exc()}")

