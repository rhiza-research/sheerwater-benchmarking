"""Verification metrics for forecasts."""

import numpy as np
from importlib import import_module
from inspect import signature
import xskillscore
import weatherbench2

import xarray as xr

from sheerwater_benchmarking.baselines import climatology_2020, seeps_wet_threshold, seeps_dry_fraction
from sheerwater_benchmarking.utils import (cacheable, dask_remote, clip_region, is_valid,
                                           lead_to_agg_days, lead_or_agg)
from weatherbench2.metrics import _spatial_average

PROB_METRICS = ['crps']  # a list of probabilistic metrics
COUPLED_METRICS = ['acc', 'pearson']  # a list of metrics that are coupled in space and time
CONTINGENCY_METRICS = ['pod', 'far', 'ets', 'bias_score']  # a list of dichotomous contingency metrics
CATEGORICAL_CONTINGENCY_METRICS = ['heidke']  # a list of contingency metrics
PRECIP_ONLY_METRICS = ["heidke", "pod", "far", "ets", "mape", "smape", "bias_score", "seeps"]


def is_precip_only(metric):
    if '-' in metric:
        metric = metric.split('-')[0]

    return metric in PRECIP_ONLY_METRICS


def is_categorical(metric):
    if '-' in metric:
        metric = metric.split('-')[0]

    return metric in CATEGORICAL_CONTINGENCY_METRICS


def is_contingency(metric):
    if '-' in metric:
        metric = metric.split('-')[0]

    return metric in CONTINGENCY_METRICS or metric in CATEGORICAL_CONTINGENCY_METRICS


def get_bins(metric):
    if is_contingency(metric) and len(metric.split('-')) != 2:
        if is_categorical(metric) and len(metric.split('-')) <= 2:
            raise ValueError(f"Categorical contingency metric {metric} must be in the format 'metric-edge-edge...'")
        elif not is_categorical(metric):
            raise ValueError(f"Dichotomous contingency metric {metric} must be in the format 'metric-edge'")

    bins = [int(x) for x in metric.split('-')[1:]]
    bins = [-np.inf] + bins + [np.inf]
    bins = np.array(bins)
    return bins


def is_coupled(metric):
    return is_contingency(metric) or metric in COUPLED_METRICS


def spatial_mape(fcst, truth, avg_time=False, skipna=True):
    ds = abs(fcst - truth) / np.maximum(abs(truth), 1e-10)
    if avg_time:
        ds = ds.mean(dim="time", skipna=skipna)
    return ds


def mape(fcst, truth, avg_time=False, skipna=True):
    ds = spatial_mape(fcst, truth, avg_time=avg_time, skipna=skipna)
    if avg_time:
        ds = ds.mean(skipna=skipna)
    else:
        ds = ds.mean(dim=['lat', 'lon'], skipna=skipna)
    return ds


def spatial_smape(fcst, truth, avg_time=False, skipna=True):
    ds = abs(fcst - truth) / (abs(fcst) + abs(truth))
    if avg_time:
        ds = ds.mean(dim="time", skipna=skipna)
    return ds


def smape(fcst, truth, avg_time=False, skipna=True):
    ds = spatial_smape(fcst, truth, avg_time=avg_time, skipna=skipna)
    if avg_time:
        ds = ds.mean(skipna=skipna)
    else:
        ds = ds.mean(dim=['lat', 'lon'], skipna=skipna)
    return ds


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
                try:
                    mod = import_module("sheerwater_benchmarking.data")
                    fn = getattr(mod, datasource)
                except (ImportError, AttributeError):
                    raise ImportError(f"Could not find datasource {datasource}.")

    return fn


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

    # Decide if this is a forecast with a lead or direct datasource with just an agg
    sparse = False  # A variable used to indicate whether the data sources themselves are sparse
    metric_sparse = False  # A variable used to indicate whether the metric induces sparsity
    if 'lead' in signature(fcst_fn).parameters:
        if lead_or_agg(lead) == 'agg':
            raise ValueError("Evaluating the function {forecast} must be called with a lead, not an aggregation")

        fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                       prob_type=prob_type, grid=grid, mask=mask, region=region)

        # Check to see the prob type attribute
        enhanced_prob_type = fcst.attrs['prob_type']

    else:
        if lead_or_agg(lead) == 'lead':
            raise "Evaluating the function {forecast} must be called with an aggregation, but not at a lead."

        fcst = fcst_fn(start_time, end_time, variable, agg_days=lead_to_agg_days(lead),
                       grid=grid, mask=mask, region=region)

        # Prob type is always deterministic for truth sources
        enhanced_prob_type = "deterministic"

    # assign sparsity if it exists
    if 'sparse' in fcst.attrs:
        sparse = fcst.attrs['sparse']

    # This checks if the forecast is valid for non-spatial metrics (which in practice is only coupled metrics)
    if not spatial and not sparse and not is_valid(fcst, variable, mask=mask,
                                                   region=region, grid=grid, valid_threshold=0.98):
        # If averaging over space, we must check if the forecast is valid
        print(f"Forecast {forecast} is not valid for region {region}.")
        return None

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, agg_days=lead_to_agg_days(lead),
                   grid=grid, mask=mask, region=region)

    # assign sparsity if it exists
    if 'sparse' in obs.attrs:
        sparse |= obs.attrs['sparse']

    # Make sure the prob type is consistent
    if enhanced_prob_type == 'deterministic' and metric in PROB_METRICS:
        raise ValueError("Cannot run probabilistic metric on deterministic forecasts.")
    elif (enhanced_prob_type == 'ensemble' or enhanced_prob_type == 'quantile') and metric not in PROB_METRICS:
        raise ValueError("Cannot run deterministic metric on probabilistic forecasts.")

    # Check to verify we aren't averaging inappropriately
    if is_coupled(metric) and not avg_time:
        raise ValueError("Coupled metrics must be averaged in time")

    if is_precip_only(metric) and variable != 'precip':
        raise ValueError(f"{metric} Can only be run with precipitation.")

    # drop all times not in fcst
    valid_times = set(obs.time.values).intersection(set(fcst.time.values))
    obs = obs.sel(time=list(valid_times))

    # Convert observation and forecast times to seconds since epoch
    if np.issubdtype(obs[variable].dtype, np.datetime64) or (obs[variable].dtype == np.dtype('<M8[ns]')):
        # Forecast must be datetime64
        assert np.issubdtype(fcst[variable].dtype, np.datetime64) or (fcst[variable].dtype == np.dtype('<M8[ns]'))
        obs = obs.astype('int64') / 1e9
        fcst = fcst.astype('int64') / 1e9
        # NaT get's converted to -9.22337204e+09, so filter that to a proper nan
        obs = obs.where(obs > -1e9, np.nan)
        fcst = fcst.where(fcst > -1e9, np.nan)

    ############################################################
    # Call the metrics with their various libraries
    ############################################################

    # Get the appropriate climatology dataframe for metric calculation
    if metric == 'seeps':
        wet_threshold = seeps_wet_threshold(first_year=1991, last_year=2020, agg_days=lead_to_agg_days(lead), grid=grid)
        dry_fraction = seeps_dry_fraction(first_year=1991, last_year=2020, agg_days=lead_to_agg_days(lead), grid=grid)
        clim_ds = xr.merge([wet_threshold, dry_fraction])

        metric_kwargs = {
            'climatology': clim_ds,
            'dry_threshold_mm': 0.25,
            'precip_name': 'precip',
            'min_p1': 0.03,
            'max_p1': 0.93,
        }

        metric_sparse = True

        if spatial:
            m_ds = weatherbench2.metrics.SpatialSEEPS(**metric_kwargs) \
                                .compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
        else:
            m_ds = weatherbench2.metrics.SEEPS(**metric_kwargs) \
                                .compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)

    elif metric == 'acc':
        clim_ds = climatology_2020(start_time, end_time, variable, lead, prob_type='deterministic',
                                   grid=grid, mask=mask, region=region)

        fcst = fcst - clim_ds
        obs = obs - clim_ds

        if spatial:
            m_ds = weatherbench2.metrics.SpatialACC().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
        else:
            m_ds = weatherbench2.metrics.ACC().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)

    elif is_contingency(metric):
        bins = get_bins(metric)
        metric = metric.split('-')[0]

        metric_func_names = {
            'pod': 'hit_rate',
            'far': 'false_alarm_rate',
            'ets': 'equit_threat_score',
            'bias_score': 'bias_score',
            'heidke': 'heidke_score',
        }

        metric_func = metric_func_names[metric]

        if spatial:
            dims = ['time']
        else:
            dims = ['time', 'lat', 'lon']

        obs = obs.sel(time=fcst.time)
        contingency_table = xskillscore.Contingency(obs, fcst, bins, bins, dim=dims)
        metric_fn = getattr(contingency_table, metric_func)
        m_ds = metric_fn()

    elif metric == 'pearson':
        fcst = fcst.chunk(time=-1, lat=-1, lon=-1)  # all must be -1 to succeed
        obs = obs.chunk(time=-1, lat=-1, lon=-1)  # all must be -1 to succeed
        if spatial:
            m_ds = xskillscore.pearson_r(a=obs, b=fcst, dim='time', skipna=True)
        else:
            m_ds = xskillscore.pearson_r(a=obs, b=fcst, skipna=True)

    elif metric == 'crps' and enhanced_prob_type == 'ensemble':
        fcst = fcst.chunk(member=-1, time=1, lat=250, lon=250)  # member must be -1 to succeed
        if spatial:
            m_ds = xskillscore.crps_ensemble(observations=obs, forecasts=fcst, mean=avg_time, dim='time')
        else:
            m_ds = xskillscore.crps_ensemble(observations=obs, forecasts=fcst, mean=avg_time)

    elif metric == 'crps' and enhanced_prob_type == 'quantile':
        if spatial:
            m_ds = weatherbench2.metrics.SpatialQuantileCRPS(quantile_dim='member') \
                                .compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
        else:
            m_ds = weatherbench2.metrics.QuantileCRPS(quantile_dim='member') \
                                .compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)

    elif metric == 'mape':
        if spatial:
            m_ds = spatial_mape(fcst, obs, avg_time=avg_time, skipna=True)
        else:
            m_ds = mape(fcst, obs, avg_time=avg_time, skipna=True)

    elif metric == 'smape':
        if spatial:
            m_ds = spatial_smape(fcst, obs, avg_time=avg_time, skipna=True)
        else:
            m_ds = smape(fcst, obs, avg_time=avg_time, skipna=True)

    elif metric == 'mae':
        if spatial:
            m_ds = weatherbench2.metrics.SpatialMAE().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
        else:
            m_ds = weatherbench2.metrics.MAE().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)

    elif metric == 'mse' or metric == 'rmse':
        if spatial:
            m_ds = weatherbench2.metrics.SpatialMSE().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
        else:
            m_ds = weatherbench2.metrics.MSE().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)

    elif metric == 'bias':
        if spatial:
            m_ds = weatherbench2.metrics.SpatialBias().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
        else:
            m_ds = weatherbench2.metrics.Bias().compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)

    else:
        raise ValueError(f"Metric {metric} not implemented")

    m_ds = m_ds.assign_attrs(sparse=sparse)
    m_ds = m_ds.assign_attrs(metric_sparse=metric_sparse)

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
           cache=True,
           validate_cache_timeseries=False)
def global_metric(start_time, end_time, variable, lead, forecast, truth,
                  metric, grid="global1_5", mask='lsm', region='global'):
    """Compute a metric without aggregating in space or time at a specific lead."""
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
    if is_coupled(metric) and time_grouping is not None:
        raise NotImplementedError("Cannot run time grouping for coupled metrics.")

    # Get the completely aggregated metric
    if is_coupled(metric) and not spatial:
        ds = eval_metric(start_time, end_time, variable, lead=lead,
                         forecast=forecast, truth=truth, spatial=False, avg_time=True,
                         metric=metric, grid=grid, mask=mask, region=region)

        if ds is None:
            return None

        for coord in ds.coords:
            if coord not in ['time']:
                ds = ds.reset_coords(coord, drop=True)
        return ds

    # To evaluate RMSE, we call MSE and take the final square root average all averaging in sp/time
    called_metric = 'mse' if metric == 'rmse' else metric

    if is_coupled(called_metric):
        # Get the unaggregated global metric
        ds = aggregated_global_metric(start_time, end_time, variable, lead=lead,
                                      forecast=forecast, truth=truth,
                                      metric=called_metric, grid=grid, mask=mask, region='global')
        if ds is None:
            return None

        sparse = ds.attrs['sparse']
    else:

        # Get the unaggregated global metric
        ds = global_metric(start_time, end_time, variable, lead=lead,
                           forecast=forecast, truth=truth,
                           metric=called_metric, grid=grid, mask=mask, region='global')

        if ds is None:
            return None

        sparse = ds.attrs['sparse']
        metric_sparse = ds.attrs['metric_sparse']

        if time_grouping is not None:
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
        # TODO: we can convert this to a groupby_time call when we're ready
        # ds = groupby_time(ds, grouping=time_grouping, agg_fn=xr.DataArray.mean, dim='time')

    # Clip it to the region
    ds = clip_region(ds, region)

    if metric_sparse:
        print("Metric is sparse, checking if forecast is valid directly")
        if metric in PROB_METRICS:
            prob_type = 'probabilistic'
        else:
            prob_type = 'deterministic'

        check_ds = get_datasource_fn(forecast)(start_time, end_time, variable, lead=lead,
                                               prob_type=prob_type, grid=grid, mask=mask, region=region)
    else:
        check_ds = ds

    # Check if forecast is valid before spatial averaging
    if not sparse and not is_valid(check_ds, variable, mask, region, grid, valid_threshold=0.98):
        print("Metric is not valid for region.")
        return None

    for coord in ds.coords:
        if coord not in ['time', 'lat', 'lon']:
            ds = ds.reset_coords(coord, drop=True)

    # Average in space
    if not spatial:
        ds = _spatial_average(ds, lat_dim='lat', lon_dim='lon', skipna=True)

    # Take the final square root of the MSE, after spatial averaging
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
                          grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        forecasts = ['climatology_2015', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased',  'fuxi']
        if variable == 'rainy_onset_no_drought':
            leads = ['day1', 'day8', 'day15', 'day20']
        else:
            leads = ['day1', 'day8', 'day15', 'day20', 'day29', 'day36']
    else:
        forecasts = ['fuxi', 'salient', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015',
                     'climatology_trend_2015', 'climatology_rolling', 'gencast', 'graphcast']
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


__all__ = ['eval_metric', 'global_metric', 'aggregated_global_metric',
           'grouped_metric', 'skill_metric',
           'summary_metrics_table', 'biweekly_summary_metrics_table']
