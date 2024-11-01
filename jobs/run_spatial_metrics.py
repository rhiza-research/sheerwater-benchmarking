"""Runs metrics and updates the caches."""
import argparse
import itertools

import dask

from sheerwater_benchmarking.metrics import grouped_metric
from sheerwater_benchmarking.utils import start_remote

parser = argparse.ArgumentParser()
parser.add_argument("--start-time", default="2016-01-01", type=str)
parser.add_argument("--end-time", default="2022-12-31", type=str)
parser.add_argument("--baseline", type=str, nargs='*')
parser.add_argument("--forecast", type=str, nargs='*')
parser.add_argument("--variable", type=str, nargs='*')
parser.add_argument("--metric", type=str, nargs='*')
parser.add_argument("--grid", type=str, nargs='*')
parser.add_argument("--region", type=str, nargs='*')
parser.add_argument("--lead", type=str, nargs='*')
parser.add_argument("--time-grouping", type=str, nargs='*')
parser.add_argument("--parallel", action=argparse.BooleanOptionalAction, default=False)
args = parser.parse_args()

baselines = ["ecmwf_ifs_er", "ecmwf_ifs_er_debiased",
             "climatology_2015", "climatology_trend_2015", "climatology_rolling"]
if args.baseline:
    baselines = args.baseline

forecasts = ["salient", "ecmwf_ifs_er", "ecmwf_ifs_er_debiased",
             "climatology_2015", "climatology_trend_2015", "climatology_rolling"]
if args.forecast:
    forecastss = args.forecast

metrics = ["mae", "crps"]
if args.metric:
    metrics = args.metric

variables = ["precip", "tmp2m"]
if args.variable:
    variables = args.variable

grids = ["global0_25", "global1_5"]
if args.grid:
    grids = args.grid

regions = ["africa", "east_africa", "global"]
if args.region:
    regions = args.region

leads = ["week1", "week2", "week3", "week4", "week5"]
if args.lead:
    leadss = args.lead

time_groupings = [None, "month_of_year", "year"]
if args.time_grouping:
    time_groupings = args.time_grouping
    time_groupings = [x if x != 'None' else None for x in time_groupings]

start_remote(remote_config=['large_cluster'])

#combos = itertools.product(metrics, variables, grids, baselines, regions, time_groupings)
combos = itertools.product(metrics, variables, grids, regions, leads, forecasts)
for metric in metrics:
    for variable in variables:
        for grid in grids:
            for region in regions:
                for lead in leads:
                    output = []
                    for forecast in forecasts:

                        if args.parallel:
                            ds = dask.delayed(grouped_metric)(args.start_time, args.end_time, variable, lead, forecast, "era5", metric, spatial=True, grid=grid, region=region,
                                              force_overwrite=True)
                            output.append(ds)
                        else:
                            grouped_metric(args.start_time, args.end_time, variable, lead, forecast, "era5", metric, spatial=True, grid=grid, region=region,
                                           force_overwrite=True, storage_backend='terracotta')

                    if args.parallel:
                        dask.compute(output)
