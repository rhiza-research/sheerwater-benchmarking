"""Verification metrics for forecasters and reanalyses."""
import numpy as np
from importlib import import_module
from inspect import signature
import xarray as xr

from sheerwater_benchmarking.baselines import climatology_2020, seeps_wet_threshold, seeps_dry_fraction
from sheerwater_benchmarking.utils import (cacheable, dask_remote, clip_region, is_valid,
                                           lead_to_agg_days, lead_or_agg, apply_mask)
from sheerwater_benchmarking.masks import region_labels

from .metric_library import (metric_factory, compute_statistic, latitude_weighted_spatial_average, groupby_time)


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
           cache=True,
           cache_args=['variable', 'lead', 'forecast', 'truth', 'statistic', 'bins', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               },
           },
           cache_disable_if={'statistic': ['pred', 'target', 'mse', 'mae',
                                           'n_valid', 'n_correct',
                                           'n_fcst_bin_1', 'n_fcst_bin_2', 'n_fcst_bin_3',
                                           'n_fcst_bin_4', 'n_fcst_bin_5',
                                           'n_obs_bin_1', 'n_obs_bin_2', 'n_obs_bin_3',
                                           'n_obs_bin_4', 'n_obs_bin_5']},
           validate_cache_timeseries=True)
def global_statistic(start_time, end_time, variable, lead, forecast, truth,
                     statistic, bins, metric_info, grid="global1_5"):
    """Compute a global metric without aggregated in time or space at a specific lead."""
    prob_type = metric_info.prob_type

    # Get the forecast and check validity
    fcst_fn = get_datasource_fn(forecast)

    # Decide if this is a forecast with a lead or direct datasource with just an agg
    # Enables the same code to be used for both forecasts and truth sources
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

    ###########################################################################
    # Prepare and preprocess the data necessary to compute the statistics
    ###########################################################################
    stat_data = {
        'obs': obs,
        'fcst': fcst,
        'statistic': statistic,
        'prob_type': enhanced_prob_type,
    }
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
        stat_data['fcst_anom'] = fcst_anom
        stat_data['obs_anom'] = obs_anom
    # Get the appropriate climatology dataframe for metric calculation
    elif statistic == 'seeps':
        wet_threshold = seeps_wet_threshold(first_year=1991, last_year=2020, agg_days=lead_to_agg_days(lead), grid=grid)
        dry_fraction = seeps_dry_fraction(first_year=1991, last_year=2020, agg_days=lead_to_agg_days(lead), grid=grid)
        clim_ds = xr.merge([wet_threshold, dry_fraction])
        stat_data['statistic_kwargs'] = {
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
        stat_data['fcst_digitized'] = fcst_digitized
        stat_data['obs_digitized'] = obs_digitized

    ############################################################
    # Call the statistic
    ############################################################
    m_ds = compute_statistic(stat_data)
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
                       mask='lsm', region='countries'):
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
    statistic_values['spatial'] = spatial  # whether the metric is spatially aggregated

    # If metric is categorical, store the bins
    if metric_obj.categorical:
        bins = metric[metric.find('-')+1:]
        statistic_values['bins'] = bins
    else:
        bins = 'none'

    # Iterate through the statistics and compute them
    for statistic in stats_to_call:
        # Statistics can be a tuple of (statistic, agg_fn), or just a statistic with default mean agg
        if isinstance(statistic, tuple):
            statistic, agg_fn = statistic
        else:
            agg_fn = 'mean'

        ds = global_statistic(start_time, end_time, variable, lead=lead,
                              forecast=forecast, truth=truth,
                              statistic=statistic,
                              bins=bins,
                              metric_info=metric_obj, grid=grid)
        if ds is None:
            return None

        truth_sparse = ds.attrs['sparse']

        ############################################################
        # Clip, mask and check validity of the statistic
        ############################################################
        # Drop any extra coordinates
        for coord in ds.coords:
            if coord not in ['time', 'lead_time', 'lat', 'lon']:
                ds = ds.reset_coords(coord, drop=True)

        # Apply masking
        # ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region)

        # Check validity of the statistic
        # TODO: need to figure out how to handle all regions in parallel
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
        # Statistic aggregation
        ############################################################
        if not metric_obj.coupled:
            # Group by time
            ds = groupby_time(ds, time_grouping, agg_fn=agg_fn)
            # region_ds = region_labels(grid=grid, region=region)
            # Average in space
            if not spatial:
                ds = latitude_weighted_spatial_average(ds, agg=agg_fn)

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
                                    grid=grid, mask=mask, region=region,
                                    retry_null_cache=True)
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
           hash_postgres_table_name=True,
           backend='postgres',
           cache=True,
           primary_keys=['time', 'forecast'])
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
           cache=True,
           primary_keys=['time', 'forecast'])
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
           cache=True,
           primary_keys=['time', 'forecast'])
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


__all__ = ['global_statistic', 'grouped_metric', 'skill_metric',
           'summary_metrics_table', 'biweekly_summary_metrics_table']
