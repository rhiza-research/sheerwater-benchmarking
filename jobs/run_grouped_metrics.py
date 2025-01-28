#!/usr/bin/env python
"""Runs metrics and updates the caches."""
import itertools
import traceback


from sheerwater_benchmarking.metrics import grouped_metric
from sheerwater_benchmarking.utils import start_remote
from jobs import parse_args, run_in_parallel

(start_time, end_time, forecasts, truth, metrics, variables, grids,
 regions, leads, time_groupings, parallelism,
 recompute, backend, remote_name, remote, remote_config) = parse_args()

if remote:
    start_remote(remote_config=remote_config, remote_name=remote_name)

combos = itertools.product(metrics, variables, grids, regions, leads, forecasts, time_groupings)


def run_grouped(combo):
    """Run grouped metrics."""
    print(combo)
    metric, variable, grid, region, lead, forecast, time_grouping = combo

    if is_precip_only(metric) and variable != 'precip':
        print(f"Skipping {metric} for not precip variable.")
        return

    if metric == 'seeps' and grid == 'global0_25':
        print(f"Skipping seeps at 0.25 grid for now")
        return

    try:
        grouped_metric(start_time, end_time, variable, lead, forecast, truth, metric,
                       spatial=False, time_grouping=time_grouping, grid=grid, region=region,
                       force_overwrite=True, filepath_only=True, recompute=recompute)
    except KeyboardInterrupt as e:
        raise(e)
    except: # noqa:E722
        print(f"Failed to run global metric {forecast} {lead} {grid} {variable} {metric}: {traceback.format_exc()}")


if __name__ == "__main__":
    run_in_parallel(run_grouped, combos, parallelism)
