#!/usr/bin/env python
"""Runs metrics and updates the caches."""
import itertools
import traceback

from sheerwater_benchmarking.metrics import summary_metrics_table
from sheerwater_benchmarking.utils import start_remote
from jobs import parse_args, run_in_parallel, prune_metrics

(start_time, end_time, forecasts, truth, metrics,
 variables, grids, regions, leads,
 time_groupings, parallelism, recompute,
 backend, remote_name, remote, remote_config) = parse_args()

if remote:
    start_remote(remote_config=remote_config, remote_name=remote_name)

combos = itertools.product(metrics, variables, grids, regions, [None], [None], time_groupings, truth)
combos = prune_metrics(combos)

filepath_only = True
if backend is not None:
    filepath_only = False


def run_metrics_table(combo):
    """Run table metrics."""
    metric, variable, grid, region, _, _, time_grouping, truth = combo

    try:
        return seasonal_metrics_table(start_time, end_time, variable, truth, metric,
                              time_grouping=time_grouping, grid=grid, region=region,
                              force_overwrite=True, filepath_only=filepath_only,
                              recompute=recompute, storage_backend=backend)
    except KeyboardInterrupt as e:
        raise(e)
    except NotImplementedError:
        print(f"Metric {forecast} {lead} {grid} {variable} {metric} not implemented: {traceback.format_exc()}")
        return "Not Impelemnted"
    except:  # noqa: E722
        print(f"Failed to run metric {grid} {variable} {metric} \
                {region} {time_grouping}: {traceback.format_exc()}")
        return None


if __name__ == "__main__":
    run_in_parallel(run_metrics_table, combos, parallelism)
