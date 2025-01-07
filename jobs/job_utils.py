"""Utilities for running jobs."""
import argparse
import dask
import itertools


def parse_args():
    """Parses arguments for jobs."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-time", default="2016-01-01", type=str)
    parser.add_argument("--end-time", default="2022-12-31", type=str)
    parser.add_argument("--forecast", type=str, nargs='*')
    parser.add_argument("--truth", type=str, default="era5")
    parser.add_argument("--variable", type=str, nargs='*')
    parser.add_argument("--metric", type=str, nargs='*')
    parser.add_argument("--grid", type=str, nargs='*')
    parser.add_argument("--region", type=str, nargs='*')
    parser.add_argument("--lead", type=str, nargs='*')
    parser.add_argument("--time-grouping", type=str, nargs='*')
    parser.add_argument("--backend", type=str, default=None)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--recompute", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--remote", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--ground-truth", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument("--remote-name", type=str, default=None)
    parser.add_argument("--remote-config", type=str, nargs='*')
    args = parser.parse_args()

    if args.ground_truth:
        forecasts = ["era5", "chiprs", "imerg"]
    else:
        forecasts = ["perpp", "salient", "ecmwf_ifs_er", "ecmwf_ifs_er_debiased",
                     "climatology_2015", "climatology_trend_2015", "climatology_rolling"]
    if args.forecast:
        forecasts = args.forecast

    if args.ground_truth:
        truth = "ghcn"
    else:
        truth = args.truth

    if args.ground_truth:
        metrics = ["mae", "rmse", "bias"]
    else:
        metrics = ["mae", "crps", "acc", "rmse", "bias"]
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

    if args.ground_truth:
        leads = ["daily", "weekly", "biweekly", "monthly"]
    else:
        leads = ["week1", "week2", "week3", "week4", "week5", "week6", "weeks34", "weeks56"]

    if args.lead:
        leads = args.lead

    time_groupings = [None, "month_of_year", "year"]
    if args.time_grouping:
        time_groupings = args.time_grouping
        time_groupings = [x if x != 'None' else None for x in time_groupings]

    remote_config = ["large_cluster"]
    if args.remote_config:
        remote_config = args.remote_config

    return (args.start_time, args.end_time, forecasts, truth, metrics, variables, grids,
            regions, leads, time_groupings, args.parallelism,
            args.recompute, args.backend, args.remote_name, args.remote, remote_config)

def run_in_parallel(func, iterable, parallelism):
    """Run a function in parallel with dask delayed.

    Args:
        func(callable): A function to call. Must take one of iterable as an argument.
        iterable (iterable): Any iterable object to pass to func.
        parallelism (int): Number of func(iterables) to run in parallel at a time.
    """
    iterable, copy = itertools.tee(iterable)
    length = len(list(copy))
    if parallelism <= 1:
        for i, it in enumerate(iterable):
            print(f"Running {i+1}/{length}")
            func(it)
    else:
        counter = 0
        for it in itertools.batched(iterable, parallelism):
            output = []
            print(f"Running {counter+1}...{counter+parallelism}/{length}")
            for i in it:
                out = dask.delayed(func)(i)
                output.append(out)

            dask.compute(output)
            counter = counter + parallelism
