import argparse
import dask
import itertools

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-time", default="2016-01-01", type=str)
    parser.add_argument("--end-time", default="2022-12-31", type=str)
    parser.add_argument("--forecast", type=str, nargs='*')
    parser.add_argument("--variable", type=str, nargs='*')
    parser.add_argument("--metric", type=str, nargs='*')
    parser.add_argument("--grid", type=str, nargs='*')
    parser.add_argument("--region", type=str, nargs='*')
    parser.add_argument("--lead", type=str, nargs='*')
    parser.add_argument("--time-grouping", type=str, nargs='*')
    parser.add_argument("--baseline", type=str, nargs='*')
    parser.add_argument("--backend", type=str, default=None)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--recompute", action=argparse.BooleanOptionalAction, default=False)
    args = parser.parse_args()

    forecasts = ["salient", "ecmwf_ifs_er", "ecmwf_ifs_er_debiased",
                 "climatology_2015", "climatology_trend_2015", "climatology_rolling"]
    if args.forecast:
        forecasts = args.forecast

    baselines = ["ecmwf_ifs_er", "ecmwf_ifs_er_debiased",
                 "climatology_2015", "climatology_trend_2015", "climatology_rolling"]
    if args.baseline:
        baselines = args.baseline

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

    leads = ["week1", "week2", "week3", "week4", "week5", "week6"]
    if args.lead:
        leadss = args.lead

    time_groupings = [None, "month_of_year", "year"]
    if args.time_grouping:
        time_groupings = args.time_grouping
        time_groupings = [x if x != 'None' else None for x in time_groupings]

    return args.start_time, args.end_time, forecasts, metrics, variables, grids, regions, leads, time_groupings, baselines, args.parallelism, args.recompute, args.backend

def run_in_parallel(func, iterable, parallelism):
    iterable, copy = itertools.tee(iterable)
    length = len(list(copy))
    if parallelism <= 1:
        for i, l in enumerate(iterable):
            print(f"Running {i+1}/{length}")
            func(l)
    else:
        counter = 0
        for l in itertools.batched(iterable, parallelism):
            output = []
            print(f"Running {counter+1}...{counter+parallelism}/{length}")
            for i in l:
                out = dask.delayed(func)(i)
                output.append(out)

            dask.compute(output)
            counter = counter + parallelism

