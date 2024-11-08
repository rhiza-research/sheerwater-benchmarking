#!/usr/bin/env python
"""Runs metrics and updates the caches."""
import itertools
import traceback

from sheerwater_benchmarking.metrics import summary_metrics_table, biweekly_summary_metrics_table
from sheerwater_benchmarking.utils import start_remote
from jobs import parse_args, run_in_parallel

start_time, end_time, forecasts, metrics, variables, grids, regions, leads, time_groupings, baselines, \
    parallelism, recompute, backend, remote_name, remote, remote_config = parse_args()

if remote:
    start_remote(remote_config=remote_config, remote_name=remote_name)

combos = itertools.product(metrics, variables, grids, regions, time_groupings, baselines)

filepath_only = True
if backend is not None:
    filepath_only = False


def run_metrics_table(combo):
    metric, variable, grid, region, time_grouping, baseline = combo

    try:
        summary_metrics_table(start_time, end_time, variable, "era5", metric, baseline=baseline, time_grouping=time_grouping, grid=grid, region=region,
                              force_overwrite=True, filepath_only=filepath_only, recompute=recompute, storage_backend=backend)
    except Exception as e:
        print(f"Failed to run table metric {grid} {variable} {metric} {
              region} {baseline} {time_grouping}: {traceback.format_exc()}")

    try:
        biweekly_summary_metrics_table(start_time, end_time, variable, "era5", metric, baseline=baseline, time_grouping=time_grouping, grid=grid, region=region,
                                       force_overwrite=True, filepath_only=filepath_only, recompute=recompute, storage_backend=backend)
    except Exception as e:
        print(f"Failed to run biweekly table metric {grid} {variable} {metric} {
              region} {baseline} {time_grouping}: {traceback.format_exc()}")


if __name__ == "__main__":
    run_in_parallel(run_metrics_table, combos, parallelism)
