"""Verification metrics for forecasts."""

from sheerwater_benchmarking.utils import (cacheable, dask_remote, start_remote, plot_ds)  # noqa: F401
from sheerwater_benchmarking.metrics import grouped_metric, eval_metric, global_metric, summary_metrics_table  # noqa: F401


if __name__ == "__main__":
    """Temporary runners to generate rainy onset tables."""
    start_remote(remote_name='genevieve', remote_config='xlarge_cluster')
    # Runners to generate the tables
    start_time = '2016-01-01'
    end_time = '2022-12-31'

    # variables = ['precip', 'tmp2m']
    # variables = ['rainy_onset_no_drought', 'rainy_onset']
    variables = ['rainy_onset_no_drought']
    # metrics = ['mae', 'rmse', 'bias', 'acc', 'heidke', 'pod', 'far', 'ets', 'mape', 'smape', 'bias_score', 'seeps']
    metrics = ['mae', 'bias', 'rmse']
    truth = 'era5'
    # time_grouping = [None, 'month', 'year']
    time_grouping = [None]
    region = 'kenya'
    grid = 'global1_5'
    mask = 'lsm'

    for variable in variables:
        for metric in metrics:
            for tg in time_grouping:
                ds = summary_metrics_table(start_time, end_time, variable, truth, metric,
                                           time_grouping=tg, grid=grid, mask=mask, region=region,
                                           recompute=True, force_overwrite=True,
                                           backend='postgres')
