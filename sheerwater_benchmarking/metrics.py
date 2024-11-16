"""Verification metrics for forecasts."""
from importlib import import_module

import xarray as xr

from sheerwater_benchmarking.baselines import climatology_forecast
from sheerwater_benchmarking.utils import cacheable, dask_remote, clip_region, is_valid
from weatherbench2.metrics import _spatial_average

PROB_METRICS = ['crps']  # a list of probabilistic metrics
CLIM_METRICS = ['acc']  # a list of metrics that use require a climatology input
COUPLED_METRICS = ['acc']  # a list of metrics that are coupled in space and time


def get_datasource_fn(datasource):
    """Import the datasource function from any available source."""
    try:
        mod = import_module("sheerwater_benchmarking.reanalysis")
        fn = getattr(mod, datasource)
    except (ImportError, AttributeError):
        try:
            mod = import_module("sheerwater_benchmarking.forecasts")
            fn = getattr(mod, datasource)
        except (ImportError, AttributeError):
            try:
                mod = import_module("sheerwater_benchmarking.baselines")
                fn = getattr(mod, datasource)
            except (ImportError, AttributeError):
                raise ImportError(f"Could not find datasource {datasource}.")

    return fn


def get_metric_fn(prob_type, metric, spatial=True):
    """Import the correct metrics function from weatherbench."""
    # Make sure things are consistent
    if prob_type == 'deterministic' and metric in PROB_METRICS:
        raise ValueError("Cannot run probabilistic metric on deterministic forecasts.")
    elif (prob_type == 'ensemble' or prob_type == 'quantile') and metric not in PROB_METRICS:
        raise ValueError("Cannot run deterministic metric on probabilistic forecasts.")

    wb_metrics = {
        'crps': ('xskillscore', 'crps_ensemble', {}),
        'crps-q': ('weatherbench2.metrics', 'QuantileCRPS', {'quantile_dim': 'member'}),
        'spatial-crps': ('xskillscore', 'crps_ensemble', {'dim': 'time'}),
        'spatial-crps-q': ('weatherbench2.metrics', 'SpatialQuantileCRPS', {'quantile_dim': 'member'}),
        'mae': ('weatherbench2.metrics', 'MAE', {}),
        'spatial-mae': ('weatherbench2.metrics', 'SpatialMAE', {}),
        'acc': ('weatherbench2.metrics', 'ACC', {}),
        'spatial-acc': ('weatherbench2.metrics', 'SpatialACC', {}),
        'mse': ('weatherbench2.metrics', 'MSE', {}),
        'spatial-mse': ('weatherbench2.metrics', 'SpatialMSE', {}),
        'rmse': ('weatherbench2.metrics', 'MSE', {}),
        'spatial-rmse': ('weatherbench2.metrics', 'SpatialMSE', {}),
        'bias': ('weatherbench2.metrics', 'Bias', {}),
        'spatial-bias': ('weatherbench2.metrics', 'SpatialBias', {}),
    }

    if spatial:
        metric = 'spatial-' + metric

    if prob_type == 'quantile':
        metric = metric + '-q'

    try:
        metric_lib, metric_mod, metric_kwargs = wb_metrics[metric]
        mod = import_module(metric_lib)
        metric_fn = getattr(mod, metric_mod)
        return metric_fn, metric_kwargs, metric_lib
    except (ImportError, AttributeError):
        raise ImportError(f"Did not find implementation for metric {metric}")


@dask_remote
def eval_metric(start_time, end_time, variable, lead, forecast, truth,
                metric, spatial=True, avg_time=False,
                grid="global1_5", mask='lsm', region='global'):
    """Compute a metric without aggregated in time or space at a specific lead."""
    if metric in PROB_METRICS:
        prob_type = "probabilistic"
    else:
        prob_type = "deterministic"

    # Fail early for RMSE
    if metric == 'rmse':
        raise NotImplementedError("To take RMSE call eval_metric with MSE and take the square root.")

    # Get the forecast
    fcst_fn = get_datasource_fn(forecast)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                   prob_type=prob_type, grid=grid, mask=mask, region=region)
    if not is_valid(fcst, variable, mask=mask, region=region, grid=grid, valid_threshold=0.98):
        # Forecast is not valid for this region, assuming not implemented
        print(f"Forecast {forecast} is not valid for region {region}.")
        return None

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    # Check to see the prob type attribute
    enhanced_prob_type = fcst.attrs['prob_type']

    if metric in CLIM_METRICS:
        # Get the appropriate climatology dataframe for metric calculation
        clim_ds = climatology_forecast(start_time, end_time, variable, lead, first_year=1991, last_year=2020,
                                       trend=False, prob_type='deterministic', grid=grid, mask=mask, region=region)

    metric_fn, metric_kwargs, metric_lib = get_metric_fn(
        enhanced_prob_type, metric, spatial=spatial)

    # Run the metric without aggregating in time or space
    obs = obs.sel(time=fcst.time)
    if metric_lib == 'xskillscore':
        # drop all times not in fcst
        if metric == 'crps':
            fcst = fcst.chunk(member=-1, time=1, lat=250, lon=250)  # member must be -1 to succeed
            # drop all times not in fcst
            m_ds = metric_fn(observations=obs, forecasts=fcst, mean=avg_time, **metric_kwargs)
        else:
            raise NotImplementedError("Only CRPS is implemented for xskillscore.")
    else:
        if metric == 'acc':
            assert avg_time, "ACC must be averaged in time"
            fcst = fcst - clim_ds
            obs = obs - clim_ds
        m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
        if spatial:
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    return m_ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'lead', 'forecast',
                       'truth', 'metric', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               },
           },
           cache=True)
def global_metric(start_time, end_time, variable, lead, forecast, truth,
                  metric, grid="global1_5", mask='lsm', region='global'):
    """Compute a metric without aggregating space but not in time at a specific lead."""
    if region != 'global':
        raise ValueError('Global metric must be run with region global.')
    m_ds = eval_metric(start_time, end_time, variable, lead, forecast, truth,
                       metric, spatial=True, avg_time=False,
                       grid=grid, mask=mask, region=region)
    return m_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast',
                       'truth', 'metric', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               },
           },
           cache=True)
def aggregated_global_metric(start_time, end_time, variable, lead, forecast, truth,
                             metric, grid="global1_5", mask='lsm', region='global'):
    """Compute a metric without aggregating space, but aggregate in time at a specific lead."""
    if region != 'global':
        raise ValueError('Global metric must be run with region global.')

    m_ds = eval_metric(start_time, end_time, variable, lead, forecast, truth,
                       metric, spatial=True, avg_time=True,
                       grid=grid, mask=mask, region=region)
    return m_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast',
                       'truth', 'metric', 'time_grouping', 'spatial', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": -1},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, "time": 30}
               },
           },
           cache=True)
def grouped_metric(start_time, end_time, variable, lead, forecast, truth,
                   metric, time_grouping=None, spatial=False, grid="global1_5",
                   mask='lsm', region='africa'):
    """Compute a grouped metric for a forecast at a specific lead."""
    if metric in COUPLED_METRICS and time_grouping is not None:
        raise NotImplementedError("Cannot run time grouping for coupled metrics.")

    # Get the completely aggregated metric
    if metric in COUPLED_METRICS and not spatial:
        ds = eval_metric(start_time, end_time, variable, lead=lead,
                         forecast=forecast, truth=truth, spatial=False, avg_time=True,
                         metric=metric, grid=grid, mask=mask, region=region)
        for coord in ds.coords:
            if coord not in ['time']:
                ds = ds.reset_coords(coord, drop=True)
        return ds

    if metric in COUPLED_METRICS:
        # Get the unaggregated global metric
        ds = aggregated_global_metric(start_time, end_time, variable, lead=lead,
                                      forecast=forecast, truth=truth,
                                      metric=metric, grid=grid, mask=mask, region='global')
    else:
        called_metric = metric
        if metric == 'rmse':
            called_metric = 'mse'

        # Get the unaggregated global metric
        ds = global_metric(start_time, end_time, variable, lead=lead,
                           forecast=forecast, truth=truth,
                           metric=called_metric, grid=grid, mask=mask, region='global')
        # Group the time column based on time grouping
        if time_grouping:
            if time_grouping == 'month_of_year':
                # TODO if you want this as a name: ds.coords["time"] = ds.time.dt.strftime("%B")
                ds.coords["time"] = ds.time.dt.month
            elif time_grouping == 'year':
                ds.coords["time"] = ds.time.dt.year
            elif time_grouping == 'quarter_of_year':
                ds.coords["time"] = ds.time.dt.quarter
            else:
                raise ValueError("Invalid time grouping")

            ds = ds.groupby("time").mean()
        else:
            # Average in time
            ds = ds.mean(dim="time")

    if not is_valid(ds, variable, mask, region, grid, valid_threshold=0.98):
        # Something has gone wrong in metric calculation
        raise RuntimeError("Metric output is invalid. This is likely due to a bug in the metric calculation.")

    # Clip it to the region
    ds = clip_region(ds, region)

    for coord in ds.coords:
        if coord not in ['time', 'lat', 'lon']:
            ds = ds.reset_coords(coord, drop=True)

    # Average in space
    if not spatial:
        ds = _spatial_average(ds, lat_dim='lat', lon_dim='lon', skipna=True)

    if metric == 'rmse':
        ds = ds ** 0.5

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'metric', 'baseline',
                       'time_grouping', 'spatial', 'grid', 'mask', 'region'],
           cache=False)
def skill_metric(start_time, end_time, variable, lead, forecast, truth,
                 metric, baseline, time_grouping=None, spatial=False, grid="global1_5",
                 mask='lsm', region='global'):
    """Compute skill either spatially or as a region summary."""
    try:
        m_ds = grouped_metric(start_time, end_time, variable, lead, forecast,
                              truth, metric, time_grouping, spatial=spatial,
                              grid=grid, mask=mask, region=region)
    except NotImplementedError:
        return None

    if not m_ds:
        return None

    # Get the baseline if it exists and run its metric
    base_ds = grouped_metric(start_time, end_time, variable, lead, baseline,
                             truth, metric, time_grouping, spatial=spatial, grid=grid, mask=mask, region=region)

    if not base_ds:
        raise NotImplementedError("Cannot compute skill for null base")

    print("Got metrics. Computing skill")

    # Compute the skill
    # TODO - think about base metric of 0
    m_ds = (1 - (m_ds/base_ds))

    return m_ds


@dask_remote
def _summary_metrics_table(start_time, end_time, variable,
                           truth, metric, leads, forecasts,
                           time_grouping=None,
                           grid='global1_5', mask='lsm', region='global'):
    """Internal function to compute summary metrics table for flexible leads and forecasts."""
    # Turn the dict into a pandas dataframe with appropriate columns
    leads_skill = [lead + '_skill' for lead in leads]

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
                                    grid=grid, mask=mask, region=region)
            except NotImplementedError:
                ds = None

            if ds:
                ds = ds.rename({variable: lead})
                ds = ds.expand_dims({'forecast': [forecast]}, axis=0)
                results_ds = xr.combine_by_coords([results_ds, ds])

    if not time_grouping:
        results_ds = results_ds.reset_coords('time', drop=True)

    df = results_ds.to_dataframe()

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
                          grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['salient', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015',
                 'climatology_trend_2015', 'climatology_rolling']
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


__all__ = ['eval_metric', 'global_metric', 'aggregated_global_metric',
           'grouped_metric', 'skill_metric',
           'summary_metrics_table', 'biweekly_summary_metrics_table']
