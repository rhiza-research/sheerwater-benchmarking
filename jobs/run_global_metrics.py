#!/usr/bin/env python
"""Runs metrics and updates the caches."""
import itertools
import traceback

from sheerwater_benchmarking.metrics import global_metric
from sheerwater_benchmarking.utils import start_remote
from jobs import parse_args, run_in_parallel

from sheerwater_benchmarking.metrics import is_precip_only
from sheerwater_benchmarking.metrics import is_coupled

(start_time, end_time, forecasts, truth, metrics, variables, grids,
 regions, leads, time_groupings, parallelism,
 recompute, backend, remote_name, remote, remote_config) = parse_args()

if remote:
    start_remote(remote_config=remote_config, remote_name=remote_name)

combos = itertools.product(metrics, variables, grids, leads, forecasts)

def run_grouped(combo):
    """Run global metrics."""
    metric, variable, grid, lead, forecast = combo

    if metric == 'rmse':
        metric = 'mse'

    if is_coupled(metric):
        print("Skipping coupled metric in global run.")
        return

    if is_precip_only(metric) and variable != 'precip':
        print(f"Skipping {metric} for not precip variable.")
        return

    try:
        global_metric(start_time, end_time, variable, lead, forecast, truth, metric, grid=grid,
                       force_overwrite=True, filepath_only=True, recompute=recompute)
    except KeyboardInterrupt as e:
        raise(e)
    except: # noqa:E722
        print(f"Failed to run global metric {forecast} {lead} {grid} {variable} {metric}: {traceback.format_exc()}")

if __name__ == "__main__":
    run_in_parallel(run_grouped, combos, parallelism)