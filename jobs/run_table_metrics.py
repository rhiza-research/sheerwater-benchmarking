#!/usr/bin/env python
"""Runs metrics and updates the caches."""
import itertools
import traceback

from sheerwater_benchmarking.metrics import summary_metrics_table
from sheerwater_benchmarking.utils import start_remote
from jobs import parse_args, run_in_parallel

from sheerwater_benchmarking.metrics import is_precip_only
from sheerwater_benchmarking.metrics import is_coupled

(start_time, end_time, forecasts, truth, metrics,
 variables, grids, regions, leads,
 time_groupings, parallelism, recompute,
 backend, remote_name, remote, remote_config) = parse_args()

if remote:
    start_remote(remote_config=remote_config, remote_name=remote_name)

combos = itertools.product(metrics, variables, grids, regions, time_groupings)

filepath_only = True
if backend is not None:
    filepath_only = False


def run_metrics_table(combo):
    """Run table metrics."""
    metric, variable, grid, region, time_grouping = combo

    if is_coupled(metric) and time_grouping is not None:
        print(f"Cannot run coupled metric {metric} with a time grouping.")
        return

    if is_precip_only(metric) and variable != 'precip':
        print(f"Skipping {metric} for not precip variable.")
        return

    if (metric == 'seeps' or metric == 'pearson') and grid == 'global0_25':
        print(f"Skipping seeps and pearson at 0.25 grid for now")
        return

    try:
        summary_metrics_table(start_time, end_time, variable, truth, metric,
                              time_grouping=time_grouping, grid=grid, region=region,
                              force_overwrite=True, filepath_only=filepath_only,
                              recompute=recompute, storage_backend=backend)
    except KeyboardInterrupt as e:
        raise(e)
    except:  # noqa: E722
        print(f"Failed to run metric {grid} {variable} {metric} \
                {region} {time_grouping}: {traceback.format_exc()}")


if __name__ == "__main__":
    run_in_parallel(run_metrics_table, combos, parallelism)
