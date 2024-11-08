#!/usr/bin/env python
"""Runs metrics and updates the caches."""
import itertools
import traceback


from sheerwater_benchmarking.metrics import grouped_metric
from sheerwater_benchmarking.utils import start_remote
from jobs import parse_args, run_in_parallel

start_time, end_time, forecasts, metrics, variables, grids, regions, leads, time_groupings, baselines, \
    parallelism, recompute, backend, remote_name, remote = parse_args()

if remote:
    start_remote(remote_config=['large_cluster'], remote_name=remote_name)

combos = itertools.product(metrics, variables, grids, regions, leads, forecasts, time_groupings)


def run_grouped(combo):
    """Run grouped metric for a combination of parameters."""
    print(combo)
    metric, variable, grid, region, lead, forecast, time_grouping = combo

    try:
        grouped_metric(start_time, end_time, variable, lead, forecast, "era5", metric, spatial=True,
                       time_grouping=time_grouping, grid=grid, region=region,
                       force_overwrite=True, filepath_only=True, recompute=recompute, storage_backend=backend)
    except Exception:
        print(f"Failed to run global metric {forecast} {lead} {grid} {variable} {metric}: {traceback.format_exc()}")


if __name__ == "__main__":
    run_in_parallel(run_grouped, combos, parallelism)
