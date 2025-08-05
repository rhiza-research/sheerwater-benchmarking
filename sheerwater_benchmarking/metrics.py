# """Verification metrics for forecasts."""

import numpy as np
from importlib import import_module
from inspect import signature
import xskillscore
import weatherbench2
import matplotlib.pyplot as plt
import xarray as xr

from sheerwater_benchmarking.baselines import climatology_2020, seeps_wet_threshold, seeps_dry_fraction
from sheerwater_benchmarking.utils import (cacheable, dask_remote, clip_region, is_valid,
                                           lead_to_agg_days, lead_or_agg, apply_mask)

from .metric_library import metric_factory


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
           cache_disable_if={'statistic': ['pred', 'target']},
           validate_cache_timeseries=True)
def global_statistic(start_time, end_time, variable, lead, forecast, truth,
                     statistic, metric_info, grid="global1_5"):
    """Compute a global metric without aggregated in time or space at a specific lead."""
    prob_type = metric_info.prob_type
    if metric_info.categorical:
        stat_name = statistic.split('-')[0]
    else:
        stat_name = statistic

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
    if stat_name in ['anom_covariance', 'squared_pred_anom', 'squared_target_anom']:
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
    if stat_name == 'seeps':
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

    if metric_info.categorical:
        # Categorize the forecast and observation into bins
        bins = metric_info.config_dict['bins']

        # `np.digitize(x, bins, right=True)` returns index `i` such that:
        #   `bins[i-1] < x <= bins[i]`
        # Indices range from 0 (for x <= bins[0]) to len(bins) (for x > bins[-1]).
        # `bins` for np.digitize will be `thresholds_np`.
        obs_digitized = xr.apply_ufunc(
            np.digitize,
            obs,
            kwargs={'bins': bins, 'right': True},
            dask='parallelized',
            output_dtypes=[int],
        )
        fcst_digitized = xr.apply_ufunc(
            np.digitize,
            fcst,
            kwargs={'bins': bins, 'right': True},
            dask='parallelized',
            output_dtypes=[int],
        )
        # Restore NaN values
        fcst_digitized = fcst_digitized.where(fcst.notnull(), np.nan)
        obs_digitized = obs_digitized.where(obs.notnull(), np.nan)
    ############################################################
    # Call the statistics with their various libraries
    ############################################################
    # Get the appropriate climatology dataframe for metric calculation
    if stat_name == 'target':
        m_ds = obs
    elif stat_name == 'pred':
        m_ds = fcst
    elif stat_name == 'brier' and enhanced_prob_type == 'ensemble':
        fcst_event_prob = (fcst_digitized == 2).mean(dim='member')
        obs_event_prob = (obs_digitized == 2)
        m_ds = (fcst_event_prob - obs_event_prob)**2
        # TODO implement brier for quantile forecasts
    elif stat_name == 'seeps':
        m_ds = weatherbench2.metrics.SpatialSEEPS(**statistic_kwargs) \
                            .compute(forecast=fcst, truth=obs, avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif stat_name == 'squared_pred_anom':
        m_ds = fcst_anom**2
    elif stat_name == 'squared_target_anom':
        m_ds = obs_anom**2
    elif stat_name == 'anom_covariance':
        m_ds = fcst_anom * obs_anom
    elif stat_name == 'false_positives':
        m_ds = (obs_digitized == 1) & (fcst_digitized == 2)
    elif stat_name == 'false_negatives':
        m_ds = (obs_digitized == 2) & (fcst_digitized == 1)
    elif stat_name == 'true_positives':
        m_ds = (obs_digitized == 2) & (fcst_digitized == 2)
    elif stat_name == 'true_negatives':
        m_ds = (obs_digitized == 1) & (fcst_digitized == 1)
    elif stat_name == 'digitized_obs':
        m_ds = obs_digitized
    elif stat_name == 'digitized_fcst':
        m_ds = fcst_digitized
    elif stat_name == 'squared_pred':
        m_ds = fcst**2
    elif stat_name == 'squared_target':
        m_ds = obs**2
    elif stat_name == 'pred_mean':
        m_ds = fcst
    elif stat_name == 'target_mean':
        m_ds = obs
    elif stat_name == 'covariance':
        m_ds = fcst * obs
    elif stat_name == 'n_valid':
        m_ds = fcst.notnull()  # already masked obs, so this is locs where both forecast and obs are non null
    elif stat_name == 'crps' and enhanced_prob_type == 'ensemble':
        fcst = fcst.chunk(member=-1, time=1, lat=250, lon=250)  # member must be -1 to succeed
        m_ds = xskillscore.crps_ensemble(observations=obs, forecasts=fcst, mean=False, dim='time')
    elif stat_name == 'crps' and enhanced_prob_type == 'quantile':
        m_ds = weatherbench2.metrics.SpatialQuantileCRPS(quantile_dim='member') \
                            .compute(forecast=fcst, truth=obs, avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif stat_name == 'mape':
        m_ds = abs(fcst - obs) / np.maximum(abs(obs), 1e-10)
    elif stat_name == 'smape':
        m_ds = abs(fcst - obs) / (abs(fcst) + abs(obs))
    elif stat_name == 'mae':
        m_ds = abs(fcst - obs)
    elif stat_name == 'mse':
        m_ds = (fcst - obs)**2
    elif stat_name == 'bias':
        m_ds = fcst - obs
    else:
        raise ValueError(f"Statistic {stat_name} not implemented")

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
    # TODO: Delete, keeping around for cachable function atm
    pass


def groupby_time(ds, time_grouping, agg_fn='mean'):
    """Aggregate a statistic over time."""
    if time_grouping is not None:
        if time_grouping == 'month_of_year':
            ds.coords["time"] = ds.time.dt.month
        elif time_grouping == 'year':
            ds.coords["time"] = ds.time.dt.year
        elif time_grouping == 'quarter_of_year':
            ds.coords["time"] = ds.time.dt.quarter
        else:
            raise ValueError("Invalid time grouping")
        if agg_fn == 'mean':
            ds = ds.groupby("time").mean()
        elif agg_fn == 'sum':
            ds = ds.groupby("time").sum()
        else:
            raise ValueError(f"Invalid aggregation function {agg_fn}")
    else:
        # Average in time
        if agg_fn == 'mean':
            ds = ds.mean(dim="time")
        elif agg_fn == 'sum':
            ds = ds.sum(dim="time")
        else:
            raise ValueError(f"Invalid aggregation function {agg_fn}")
    # TODO: we can convert this to a groupby_time call when we're ready
    # ds = groupby_time(ds, grouping=time_grouping, agg_fn=xr.DataArray.mean, dim='time')
    return ds


def latitude_weighted_spatial_average(ds, lat_dim='lat', lon_dim='lon'):
    """
    Compute latitude-weighted spatial average of a dataset.

    This function weights each latitude band by the actual cell area,
    which accounts for the fact that grid cells near the poles are smaller
    in area than those near the equator.
    """
    # Calculate latitude cell bounds
    lat_rad = np.deg2rad(ds[lat_dim].values)
    # Get the centerpoint of each latitude band
    pi_over_2 = np.array([np.pi / 2], dtype=lat_rad.dtype)
    bounds = np.concatenate([-pi_over_2, (lat_rad[:-1] + lat_rad[1:]) / 2, pi_over_2])
    # Calculate the area of each latitude band
    # Calculate cell areas from latitude bounds
    upper = bounds[1:]
    lower = bounds[:-1]
    # normalized cell area: integral from lower to upper of cos(latitude)
    weights = np.sin(upper) - np.sin(lower)

    # Normalize weights
    weights /= np.mean(weights)

    # Create weights array
    weights = ds[lat_dim].copy(data=weights)
    weighted = ds.weighted(weights).mean([lat_dim, lon_dim], skipna=True)
    return weighted


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
def grouped_metric_new(start_time, end_time, variable, lead, forecast, truth,
                       metric, time_grouping=None, spatial=False, grid="global1_5",
                       mask='lsm', region='africa'):
    """Compute a grouped metric for a forecast at a specific lead."""
    # Use the metric registry to get the metric class
    metric_obj = metric_factory(metric)()

    # Check that the variable is valid for the metric
    if metric_obj.valid_variables and variable not in metric_obj.valid_variables:
        raise ValueError(f"Variable {variable} is not valid for metric {metric}")

    stats_to_call = metric_obj.statistics
    metric_sparse = metric_obj.sparse

    # Statistics needed to calculate the metrics, incrementally populated
    statistic_values = {}
    statistic_values['spatial'] = spatial
    if metric_obj.categorical:
        # Get everything after the first '-', for the categorical metrics,
        # e.g., false_positives-10
        bin_str = metric[metric.find('-')+1:]
        statistic_values['bins'] = metric_obj.config_dict['bins']

    for statistic in stats_to_call:
        if metric_obj.categorical:
            statistic_call = f'{statistic}-{bin_str}'
        else:
            statistic_call = statistic

        ds = global_statistic(start_time, end_time, variable, lead=lead,
                              forecast=forecast, truth=truth,
                              statistic=statistic_call,
                              metric_info=metric_obj, grid=grid)
        if ds is None:
            return None

        truth_sparse = ds.attrs['sparse']

        ############################################################
        # Clip, mask and check validity of the statistic
        ############################################################
        # Drop any extra coordinates
        for coord in ds.coords:
            if coord not in ['time', 'lat', 'lon']:
                ds = ds.reset_coords(coord, drop=True)

        # Clip it to the region
        ds = clip_region(ds, region)
        ds = apply_mask(ds, mask)

        # Check validity of the statistic
        if truth_sparse or metric_sparse:
            print("Metric is sparse, checking if forecast is valid directly.")
            fcst_fn = get_datasource_fn(forecast)

            if 'lead' in signature(fcst_fn).parameters:
                check_ds = fcst_fn(start_time, end_time, variable, lead=lead,
                                   prob_type=metric_obj.prob_type, grid=grid, mask=mask, region=region)
            else:
                check_ds = fcst_fn(start_time, end_time, variable, agg_days=lead_to_agg_days(lead),
                                   grid=grid, mask=mask, region=region)
        else:
            check_ds = ds

        # Check if forecast is valid before spatial averaging
        if not is_valid(check_ds, variable, mask, region, grid, valid_threshold=0.98):
            print("Metric is not valid for region.")
            return None

        ############################################################
        # Default n_valid statistic
        ############################################################
        # Add a default n_valid statistic if it doesn't exist that just counts the number
        # of non-null values in the statistics
        if 'n_valid' not in statistic_values:
            statistic_values['n_valid'] = xr.ones_like(ds)
            statistic_values['n_valid'] = groupby_time(
                statistic_values['n_valid'], time_grouping, agg_fn='sum')
            if not spatial:
                # TODO: need to convert this to be a sum, not average
                statistic_values['n_valid'] = statistic_values['n_valid'].notnull().sum()

        ############################################################
        # Statistic aggregation
        ############################################################
        if not metric_obj.coupled:
            # Group by time
            ds = groupby_time(ds, time_grouping, agg_fn='mean')
            # Average in space
            if not spatial:
                ds = latitude_weighted_spatial_average(ds)

        # Assign the final statistic value
        statistic_values[statistic] = ds.copy()

    # Finally, compute the metric based on the aggregated statistic values
    m_ds = metric_obj.compute(statistic_values)
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
