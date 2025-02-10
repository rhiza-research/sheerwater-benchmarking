"""Verification metrics for forecasts."""

import numpy as np
import xarray as xr

from sheerwater_benchmarking.utils import (cacheable, dask_remote, start_remote, plot_ds)
from sheerwater_benchmarking.metrics import grouped_metric, eval_metric, global_metric, grouped_metric


@dask_remote
def _summary_metrics_table(start_time, end_time, variable,
                           truth, metric, leads, forecasts,
                           time_grouping=None,
                           grid='global1_5', mask='lsm', region='global'):
    """Internal function to compute summary metrics table for flexible leads and forecasts."""
    # For the time grouping we are going to store it in an xarray with dimensions
    # forecast and time, which we instantiate
    results_ds = xr.Dataset(coords={'forecast': forecasts, 'time': None})

    for forecast in forecasts:
        for i, lead in enumerate(leads):
            print(f"""Running for {forecast} and {lead} with variable {variable},
                      metric {metric}, grid {grid}, and region {region}""")
            # First get the value without the baseline
            try:
                ds = grouped_metric(start_time, end_time, variable,
                                    lead=lead, forecast=forecast, truth=truth,
                                    metric=metric, time_grouping=time_grouping, spatial=False,
                                    grid=grid, mask=mask, region=region,
                                    retry_null_cache=True)
            except NotImplementedError:
                ds = None

            if ds:
                ds = ds.rename({variable: lead})
                ds = ds.expand_dims({'forecast': [forecast]}, axis=0)
                results_ds = xr.combine_by_coords([results_ds, ds])

    if not time_grouping:
        results_ds = results_ds.reset_coords('time', drop=True)

    df = results_ds.to_dataframe()

    # Reorder the columns if necessary
    df = df[leads]

    # Rename the index
    df = df.reset_index().rename(columns={'index': 'forecast'})
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric',
                       'time_grouping', 'grid', 'mask', 'region'],
           cache=True)
def summary_metrics_table(start_time, end_time, variable,
                          truth, metric, time_grouping=None,
                          forecasts=None, leads=None,
                          grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    if forecasts is None:
        forecasts = ['fuxi', 'salient', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015',
                     'climatology_trend_2015', 'climatology_rolling']
    if leads is None:
        leads = ["week1", "week2", "week3", "week4", "week5", "week6"]
    df = _summary_metrics_table(start_time, end_time, variable, truth, metric, leads, forecasts,
                                time_grouping=time_grouping,
                                grid=grid, mask=mask, region=region)

    print(df)
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric',
                       'time_grouping', 'grid', 'mask', 'region'],
           cache=True)
def station_metrics_table(start_time, end_time, variable,
                          truth, metric, time_grouping=None,
                          grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['era5', 'chirps', 'imerg']
    leads = ["daily", "weekly", "biweekly", "monthly"]
    df = _summary_metrics_table(start_time, end_time, variable, truth, metric, leads, forecasts,
                                time_grouping=time_grouping,
                                grid=grid, mask=mask, region=region)

    print(df)
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric',
                       'time_grouping', 'grid', 'mask', 'region'],
           cache=True)
def biweekly_summary_metrics_table(start_time, end_time, variable,
                                   truth, metric, time_grouping=None,
                                   grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['perpp', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015',
                 'climatology_trend_2015', 'climatology_rolling']
    leads = ["weeks34", "weeks56"]
    df = _summary_metrics_table(start_time, end_time, variable, truth, metric, leads, forecasts,
                                time_grouping=time_grouping,
                                grid=grid, mask=mask, region=region)

    print(df)
    return df


__all__ = ['summary_metrics_table', 'biweekly_summary_metrics_table', 'station_metrics_table']

if __name__ == "__main__":
    start_remote(remote_name='genevieve')
    # Runners to generate the tables
    start_time = '2016-01-01'
    end_time = '2022-12-31'

    # variables = ['precip', 'tmp2m']
    variables = ['rainy_onset']
    # metrics = ['mae', 'rmse', 'bias', 'acc', 'heidke', 'pod', 'far', 'ets', 'mape', 'smape', 'bias_score', 'seeps']
    metrics = ['mae']
    truth = 'era5'
    # time_grouping = [None, 'month', 'year']
    time_grouping = [None]
    region = 'kenya'
    grid = 'global1_5'
    mask = 'lsm'
    forecasts = ['climatology_2015', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased']
    leads = ['day1', 'day8', 'day15', 'day22', 'day29']
    # ds = eval_metric(start_time, end_time, 'rainy_onset', 'day1', 'climatology_2015', 'era5',
    #                  'mae', spatial=True, avg_time=False,
    #                  grid="global1_5", mask='lsm', region='global')
    # ds = global_metric(start_time, end_time, 'rainy_onset', 'day1', 'climatology_2015', 'era5',
    #                    'mae', grid="global1_5", mask='lsm', region='global')
    # ds = grouped_metric(start_time, end_time, 'rainy_onset', 'day1', 'climatology_2015', 'era5',
    #                     'mae', spatial=False, grid="global1_5", mask='lsm', region='africa',
    #                     cache=False)
    # import pdb; pdb.set_trace()
    # # plot_ds(ds, variable='rainy_onset')
    # import pdb
    # pdb.set_trace()

    for variable in variables:
        for metric in metrics:
            for tg in time_grouping:
                summary_metrics_table(start_time, end_time, variable, truth, metric,
                                      forecasts=forecasts, leads=leads,
                                      time_grouping=tg, grid=grid, mask=mask, region=region)
    # station_metrics_table(start_time, end_time, variable, truth, metric, tg)
    # biweekly_summary_metrics_table(start_time, end_time, variable, truth, metric, tg)
