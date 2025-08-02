# """Verification metrics for forecasts."""

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

from .metric_library import metric_factory


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
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'statistic', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               },
           },
           cache=True,
           validate_cache_timeseries=False)
def global_statistic(start_time, end_time, variable, lead, forecast, truth,
                     statistic, metric_info, grid="global1_5"):
    """Compute a global metric without aggregated in time or space at a specific lead."""
    prob_type = metric_info.prob_type

    # Get the forecast
    fcst_fn = get_datasource_fn(forecast)

    # Decide if this is a forecast with a lead or direct datasource with just an agg
    sparse = False  # A variable used to indicate whether the truth is creating sparsity
    if 'lead' in signature(fcst_fn).parameters:
        if lead_or_agg(lead) == 'agg':
            raise ValueError("Evaluating the function {forecast} must be called with a lead, not an aggregation")
        fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                       prob_type=prob_type, grid=grid, mask=None, region='global')
        # Check to see the prob type attribute
        enhanced_prob_type = fcst.attrs['prob_type']

    else:
        if lead_or_agg(lead) == 'lead':
            raise "Evaluating the function {forecast} must be called with an aggregation, but not at a lead."
        fcst = fcst_fn(start_time, end_time, variable, agg_days=lead_to_agg_days(lead),
                       grid=grid, mask=None, region='global')
        # Prob type is always deterministic for truth sources
        enhanced_prob_type = "deterministic"

    # Assign sparsity if it exists
    if 'sparse' in fcst.attrs:
        sparse = fcst.attrs['sparse']

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, agg_days=lead_to_agg_days(lead),
                   grid=grid, mask=None, region='global')
    # Assign sparsity if it exists
    if 'sparse' in obs.attrs:
        sparse |= obs.attrs['sparse']

    # Make sure the prob type is consistent
    if enhanced_prob_type == 'deterministic' and prob_type == 'probabilistic':
        raise ValueError("Cannot run probabilistic metric on deterministic forecasts.")
    elif (enhanced_prob_type == 'ensemble' or enhanced_prob_type == 'quantile') and prob_type == 'deterministic':
        raise ValueError("Cannot run deterministic metric on probabilistic forecasts.")

    # Drop all times not in fcst
    valid_times = set(obs.time.values).intersection(set(fcst.time.values))
    valid_times = list(valid_times)
    valid_times.sort()
    obs = obs.sel(time=valid_times)
    fcst = fcst.sel(time=valid_times)
    ############################################################
    # Prepare and preprocess data for the statistics
    ############################################################
    # For the case where obs and forecast are datetime objects, do a special conversion to seconds since epoch
    # TODO: This is a hack to get around the fact that the metrics library doesn't support datetime objects
    if np.issubdtype(obs[variable].dtype, np.datetime64) or (obs[variable].dtype == np.dtype('<M8[ns]')):
        # Forecast must be datetime64
        assert np.issubdtype(fcst[variable].dtype, np.datetime64) or (fcst[variable].dtype == np.dtype('<M8[ns]'))
        obs = obs.astype('int64') / 1e9
        fcst = fcst.astype('int64') / 1e9
        # NaT get's converted to -9.22337204e+09, so filter that to a proper nan
        obs = obs.where(obs > -1e9, np.nan)
        fcst = fcst.where(fcst > -1e9, np.nan)

    # Get the appropriate climatology dataframe for metric calculation
    if statistic in ['anom_covariance', 'squared_pred_anom', 'squared_target_anom']:
        clim_ds = climatology_2020(start_time, end_time, variable, lead, prob_type='deterministic',
                                   grid=grid, mask=None, region='global')
        clim_ds = clim_ds.sel(time=valid_times)

        # If the observations are sparse, ensure that the lengths are the same
        # TODO: This will probably break with sparse forecaster and dense observations
        fcst = fcst.where(obs.notnull(), np.nan)
        clim_ds = clim_ds.where(obs.notnull(), np.nan)

        # Get anomalies
        fcst_anom = fcst - clim_ds
        obs_anom = obs - clim_ds

    # Get the appropriate climatology dataframe for metric calculation
    if statistic == 'seeps':
        wet_threshold = seeps_wet_threshold(first_year=1991, last_year=2020, agg_days=lead_to_agg_days(lead), grid=grid)
        dry_fraction = seeps_dry_fraction(first_year=1991, last_year=2020, agg_days=lead_to_agg_days(lead), grid=grid)
        clim_ds = xr.merge([wet_threshold, dry_fraction])

        statistic_kwargs = {
            'climatology': clim_ds,
            'dry_threshold_mm': 0.25,
            'precip_name': 'precip',
            'min_p1': 0.03,
            'max_p1': 0.93,
        }
    ############################################################
    # Call the statistics with their various libraries
    ############################################################

    # Get the appropriate climatology dataframe for metric calculation
    if statistic == 'seeps':
        m_ds = weatherbench2.metrics.SpatialSEEPS(**statistic_kwargs) \
                            .compute(forecast=fcst, truth=obs, avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif statistic == 'squared_pred_anom':
        m_ds = fcst_anom**2
    elif statistic == 'squared_target_anom':
        m_ds = obs_anom**2
    elif statistic == 'anom_covariance':
        m_ds = fcst_anom * obs_anom
    elif statistic.split('-')[0] in ['false_positive', 'false_negative', 'true_positive', 'true_negative']:
        bins = metric_info.bins
        statistic = statistic.split('-')[0]
        contingency_table = xskillscore.Contingency(obs, fcst, bins, bins, dim=None)
        m_ds = getattr(contingency_table, statistic)()
    elif statistic == 'squared_pred':
        m_ds = fcst**2
    elif statistic == 'squared_target':
        m_ds = obs**2
    elif statistic == 'pred_mean':
        m_ds = fcst
    elif statistic == 'target_mean':
        m_ds = obs
    elif statistic == 'covariance':
        m_ds = fcst * obs
    elif statistic == 'n_valid':
        m_ds = fcst.notnull()  # already masked obs, so this is locs where both forecast and obs are non null
    elif statistic == 'crps' and enhanced_prob_type == 'ensemble':
        fcst = fcst.chunk(member=-1, time=1, lat=250, lon=250)  # member must be -1 to succeed
        m_ds = xskillscore.crps_ensemble(observations=obs, forecasts=fcst, mean=False, dim='time')
    elif statistic == 'crps' and enhanced_prob_type == 'quantile':
        m_ds = weatherbench2.metrics.SpatialQuantileCRPS(quantile_dim='member') \
                            .compute(forecast=fcst, truth=obs, avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif statistic == 'mape':
        m_ds = abs(fcst - obs) / np.maximum(abs(obs), 1e-10)
    elif statistic == 'smape':
        m_ds = abs(fcst - obs) / (abs(fcst) + abs(obs))
    elif statistic == 'mae':
        m_ds = abs(fcst - obs)
    elif statistic == 'mse':
        m_ds = (fcst - obs)**2
    elif statistic == 'bias':
        m_ds = fcst - obs
    else:
        raise ValueError(f"Statistic {statistic} not implemented")

    # Assign attributes in one call
    m_ds = m_ds.assign_attrs(
        sparse=sparse,
        prob_type=prob_type,
        forecast=forecast,
        truth=truth,
        statistic=statistic
    )
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
    # Use the metric registry to get the metric class
    metric_cls = metric_factory(metric)
    metric = metric_cls()

    stats_to_call = metric.statistics
    metric_sparse = metric.sparse
    statistic_values = {}
    for statistic in stats_to_call:
        ds = global_statistic(start_time, end_time, variable, lead=lead,
                              forecast=forecast, truth=truth,
                              statistic=statistic,
                              metric_info=metric, grid=grid)
        if ds is None:
            return None

        truth_sparse = ds.attrs['sparse']

        import pdb
        pdb.set_trace()
        ############################################################
        # Statistic aggregation
        ############################################################
        if time_grouping is not None:
            if time_grouping == 'month_of_year':
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

        if truth_sparse or metric_sparse:
            print("Metric is sparse, checking if forecast is valid directly.")
            fcst_fn = get_datasource_fn(forecast)

            if 'lead' in signature(fcst_fn).parameters:
                check_ds = fcst_fn(start_time, end_time, variable, lead=lead,
                                   prob_type=metric.prob_type, grid=grid, mask=mask, region=region)
            else:
                check_ds = fcst_fn(start_time, end_time, variable, agg_days=lead_to_agg_days(lead),
                                   grid=grid, mask=mask, region=region)
        else:
            check_ds = ds

        # Check if forecast is valid before spatial averaging
        if not is_valid(check_ds, variable, mask, region, grid, valid_threshold=0.98):
            print("Metric is not valid for region.")
            return None

        for coord in ds.coords:
            if coord not in ['time', 'lat', 'lon']:
                ds = ds.reset_coords(coord, drop=True)

        # Average in space
        if not spatial:
            ds = _spatial_average(ds, lat_dim='lat', lon_dim='lon', skipna=True)

        # Assign the final statistic value
        statistic_values[statistic] = ds.copy()

    # Finally, compute the metric based on the aggregated statistic values
    m_ds = metric.compute(statistic_values)
    return m_ds


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
                                    grid=grid, mask=mask, region=region)
            except NotImplementedError:
                ds = None

            if ds:
                ds = ds.rename({variable: lead})
                ds = ds.expand_dims({'forecast': [forecast]}, axis=0)
                results_ds = xr.combine_by_coords([results_ds, ds], combine_attrs='override')

    if not time_grouping:
        results_ds = results_ds.reset_coords('time', drop=True)

    df = results_ds.to_dataframe()

    df = df.reset_index().rename(columns={'index': 'forecast'})

    if 'time' in df.columns:
        order = ['time', 'forecast'] + leads
    else:
        order = ['forecast'] + leads

    # Reorder the columns if necessary
    df = df[order]

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
def seasonal_metrics_table(start_time, end_time, variable,
                           truth, metric, time_grouping=None,
                           grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['salient', 'climatology_2015']
    leads = ["month1", "month2", "month3"]
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
    forecasts = ['era5', 'chirps', 'imerg', 'cbam']
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
