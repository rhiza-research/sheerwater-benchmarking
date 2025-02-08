#!/usr/bin/env python
"""Runs metrics and updates the caches."""
import itertools
import traceback

from sheerwater_benchmarking.metrics import global_metric
from sheerwater_benchmarking.utils import start_remote
from jobs import parse_args, run_in_parallel, prune_metrics

def run_table(combo):
    """Run table metrics."""
    metric, variable, grid, region, _, _, time_grouping, truth = combo

    try:
        return summary_metrics_table(start_time, end_time, variable, truth, metric,
                              time_grouping=time_grouping, grid=grid, region=region,
                              force_overwrite=True, filepath_only=True,
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

def run_grouped(combo):
    """Run grouped metrics."""
    print(combo)
    metric, variable, grid, region, lead, forecast, time_grouping, truth = combo

    try:
        return grouped_metric(start_time, end_time, variable, lead, forecast, truth, metric,
                       spatial=False, time_grouping=time_grouping, grid=grid, region=region,
                       force_overwrite=True, filepath_only=True, recompute=recompute)
    except KeyboardInterrupt as e:
        raise(e)
    except NotImplementedError:
        print(f"Metric {forecast} {lead} {grid} {variable} {metric} not implemented: {traceback.format_exc()}")
        return "Not Impelemnted"
    except: # noqa:E722
        print(f"Failed to run global metric {forecast} {lead} {grid} {variable} {metric}: {traceback.format_exc()}")
        return None

def run_global(combo):
    """Run global metrics."""
    metric, variable, grid, _, lead, forecast, _, truth = combo

    if metric == 'rmse':
        metric = 'mse'

    try:
        return global_metric(start_time, end_time, variable, lead, forecast, truth, metric, grid=grid,
                       force_overwrite=True, filepath_only=True, recompute=recompute)
    except KeyboardInterrupt as e:
        raise(e)
    except NotImplementedError:
        print(f"Metric {forecast} {lead} {grid} {variable} {metric} not implemented: {traceback.format_exc()}")
        return "Not Impelemnted"
    except: # noqa:E722
        print(f"Failed to run global metric {forecast} {lead} {grid} {variable} {metric}: {traceback.format_exc()}")
        return None

if __name__ == "__main__":

    (start_time, end_time, forecasts, truth, metrics, variables, grids,
     regions, leads, time_groupings, parallelism,
     recompute, backend, remote_name, remote, remote_config) = parse_args()

    if remote:
        start_remote(remote_config=remote_config, remote_name=remote_name)

    global_combos = itertools.product(metrics, variables, grids, [None], leads, forecasts, [None], truth)
    global_combos = prune_metrics(global_combos, global_run=True)

    grouped_combos = itertools.product(metrics, variables, grids, regions, leads, forecasts, time_groupings, truth)
    grouped_combos = prune_metrics(grouped_combos)

    table_combos = itertools.product(metrics, variables, grids, regions, [None], [None], time_groupings, truth)
    table_combos = prune_metrics(table_combos)

    print("Running global metrics.")
    run_in_parallel(run_global, global_combos, parallelism)

    print("Running grouped metrics.")
    run_in_parallel(run_grouped, grouped_combos, parallelism)

    print("Running table metrics.")
    run_in_parallel(run_table, table_combos, parallelism)
