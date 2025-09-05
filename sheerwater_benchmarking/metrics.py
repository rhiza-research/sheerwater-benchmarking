"""Verification metrics for forecasters and reanalyses."""
from .metric_factory import metric_factory
from sheerwater_benchmarking.masks import region_labels
from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           get_lead_info,
                                           get_admin_level,
                                           apply_mask,
                                           clip_region,
                                           get_mask,
                                           get_time_level,
                                           get_datasource_fn)
from sheerwater_benchmarking.climatology import climatology_2020, seeps_wet_threshold, seeps_dry_fraction
import xskillscore
from weatherbench2.metrics import _spatial_average
from weatherbench2.metrics import SpatialQuantileCRPS, SpatialSEEPS
import numpy as np
from inspect import signature
import xarray as xr
import pandas as pd

# Disable scientific notation globally
np.set_printoptions(suppress=True)


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
           cache_disable_if={
               # We only need to cache statistics that are very expensive to recompute, b/c memoization will
               # handle most of the rest. We cache statistics that are used multiple times or probabilistic
               # and thus more expensive to compute
               # fcst_anom, obs_anom, fcst_digitized, obs_digitized, crps, seeps, and brier
               'statistic': ['fcst', 'obs',
                             'squared_fcst_anom', 'squared_obs_anom',
                             'anom_covariance', 'false_positives',
                             'false_negatives', 'true_positives',
                             'true_negatives',
                             'squared_fcst', 'squared_obs',
                             'fcst_mean', 'obs_mean', 'covariance',
                             'mape', 'smape', 'mae', 'mse', 'bias'
                             'n_valid', 'n_correct',
                             # some number hard coded for categorical metrics up to 4 bins
                             'n_fcst_bin_1', 'n_fcst_bin_2', 'n_fcst_bin_3',
                             'n_fcst_bin_4', 'n_fcst_bin_5',
                             'n_obs_bin_1', 'n_obs_bin_2', 'n_obs_bin_3',
                             'n_obs_bin_4', 'n_obs_bin_5']},
           validate_cache_timeseries=True)
def global_statistic(start_time, end_time, variable, lead, forecast, truth,
                     statistic, bins, metric_info, grid="global1_5"):
    """Compute a global metric without aggregated in time or space at a specific lead."""
    # save these for easy access later
    cache_kwargs = {'variable': variable, 'lead': lead, 'forecast': forecast, 'truth': truth,
                    'bins': bins, 'grid': grid, 'metric_info': metric_info}

    prob_type = metric_info.prob_type
    # For categorical metrics, get the bins
    if metric_info.categorical:
        bin_thresholds = metric_info.config_dict['bins']

    # Get the forecast and check validity
    fcst_fn = get_datasource_fn(forecast)

    # Decide if this is a forecast with a lead or direct datasource with just an agg
    # Enables the same code to be used for both forecasts and truth sources
    sparse = False  # A variable used to indicate whether the statistic is expected to be sparse
    if 'lead' in signature(fcst_fn).parameters:
        # TODO: this is no longer clearly true, since we can pass in daily, weekly to forecasters
        # TODO: do we still want this check
        # if lead_or_agg(lead) == 'agg':
        #     raise ValueError("Evaluating the function {forecast} must be called with a lead, not an aggregation")
        fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                       prob_type=prob_type, grid=grid, mask=None, region='global')
        # Check to see the prob type attribute
        enhanced_prob_type = fcst.attrs['prob_type']
    else:
        # TODO: do we still want this check?
        # if lead_or_agg(lead) == 'lead':
        #     raise "Evaluating the function {forecast} must be called with an aggregation, but not at a lead."
        fcst = fcst_fn(start_time, end_time, variable, agg_days=get_lead_info(lead)['agg_days'],
                       grid=grid, mask=None, region='global')
        # Prob type is always deterministic for truth sources
        enhanced_prob_type = "deterministic"

    # Make sure the prob type is consistent
    if enhanced_prob_type == 'deterministic' and prob_type == 'probabilistic':
        raise ValueError("Cannot run probabilistic metric on deterministic forecasts.")
    elif (enhanced_prob_type == 'ensemble' or enhanced_prob_type == 'quantile') and prob_type == 'deterministic':
        raise ValueError("Cannot run deterministic metric on probabilistic forecasts.")

    # Assign sparsity if it exists
    if 'sparse' in fcst.attrs:
        sparse = fcst.attrs['sparse']

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, agg_days=get_lead_info(lead)['agg_days'],
                   grid=grid, mask=None, region='global')
    # We need a lead specific obs, so we know which times are valid for the forecast
    lead_labels = get_lead_info(lead)['labels']
    obs = obs.expand_dims({'lead_time': lead_labels})
    # Assign sparsity if it exists
    if 'sparse' in obs.attrs:
        sparse |= obs.attrs['sparse']

    # Drop all times not in fcst
    valid_times = set(obs.time.values).intersection(set(fcst.time.values))
    valid_times = list(valid_times)
    valid_times.sort()
    obs = obs.sel(time=valid_times)
    fcst = fcst.sel(time=valid_times)

    # Ensure a matching null pattern
    # If the observations are sparse, the forecaster and the obs must be the same length
    # for metrics like ACC to work
    no_null = obs.notnull() & fcst.notnull()
    if 'member' in no_null.dims:
        # TODO: This will probably break with sparse forecaster and dense observations
        # Drop all vars except lat, lon, time, and lead_time from no_null
        # Squeeze the member dimension. A note, things like ACC won't work well across members
        no_null = no_null.isel(member=0).drop('member')
        # Drop all other coords except lat, lon, time, and lead_time
        no_null = no_null.drop_vars([var for var in no_null.coords if var not in [
                                    'lat', 'lon', 'time', 'lead_time']], errors='ignore')
    fcst = fcst.where(no_null, np.nan, drop=False)
    obs = obs.where(no_null, np.nan, drop=False)

    if statistic in ['fcst_anom', 'obs_anom']:
        # Get the appropriate climatology dataframe for metric calculation
        clim_ds = climatology_2020(start_time, end_time, variable, lead=lead, prob_type='deterministic',
                                   grid=grid, mask=None, region='global')
        clim_ds = clim_ds.sel(time=valid_times)
        clim_ds = clim_ds.where(no_null, np.nan, drop=False)

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
    ############################################################
    # Call the statistic
    ############################################################
    # Get the appropriate climatology dataframe for metric calculation
    if statistic == 'obs':
        m_ds = obs
    elif statistic == 'fcst':
        m_ds = fcst
    elif statistic == 'brier' and enhanced_prob_type == 'ensemble':
        fcst_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_digitized')
        obs_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_digitized')
        sparse = fcst_digitized.attrs['sparse'] | obs_digitized.attrs['sparse']
        fcst_event_prob = (fcst_digitized == 2).mean(dim='member')
        obs_event_prob = (obs_digitized == 2)
        m_ds = (fcst_event_prob - obs_event_prob)**2
        # TODO implement brier for quantile forecasts
    elif statistic == 'seeps':
        wet_threshold = seeps_wet_threshold(first_year=1991, last_year=2020,
                                            agg_days=get_lead_info(lead)['agg_days'], grid=grid)
        dry_fraction = seeps_dry_fraction(first_year=1991, last_year=2020,
                                          agg_days=get_lead_info(lead)['agg_days'], grid=grid)
        clim_ds = xr.merge([wet_threshold, dry_fraction])
        m_ds = SpatialSEEPS(climatology=clim_ds,
                            dry_threshold_mm=0.25,
                            precip_name='precip',
                            min_p1=0.03,
                            max_p1=0.93) \
            .compute(forecast=fcst, truth=obs,
                     avg_time=False, skipna=True)
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    elif statistic == 'fcst_anom':
        m_ds = fcst - clim_ds
    elif statistic == 'obs_anom':
        m_ds = obs - clim_ds
    elif statistic == 'squared_fcst_anom':
        fcst_anom = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_anom')
        m_ds = fcst_anom**2
    elif statistic == 'squared_obs_anom':
        obs_anom = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_anom')
        m_ds = obs_anom**2
    elif statistic == 'anom_covariance':
        fcst_anom = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_anom')
        obs_anom = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_anom')
        m_ds = fcst_anom * obs_anom
    elif statistic == 'false_positives':
        obs_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_digitized')
        fcst_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_digitized')
        m_ds = (obs_digitized == 1) & (fcst_digitized == 2)
    elif statistic == 'false_negatives':
        obs_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_digitized')
        fcst_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_digitized')
        m_ds = (obs_digitized == 2) & (fcst_digitized == 1)
    elif statistic == 'true_positives':
        obs_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_digitized')
        fcst_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_digitized')
        m_ds = (obs_digitized == 2) & (fcst_digitized == 2)
    elif statistic == 'true_negatives':
        obs_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_digitized')
        fcst_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_digitized')
        m_ds = (obs_digitized == 1) & (fcst_digitized == 1)
    elif statistic == 'n_correct':
        obs_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_digitized')
        fcst_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_digitized')
        m_ds = (obs_digitized == fcst_digitized)
    elif 'n_obs_bin' in statistic:
        category = int(statistic.split('_')[3])
        obs_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='obs_digitized')
        m_ds = (obs_digitized == category)
    elif 'n_fcst_bin' in statistic:
        category = int(statistic.split('_')[3])
        fcst_digitized = global_statistic(start_time, end_time, **cache_kwargs, statistic='fcst_digitized')
        m_ds = (fcst_digitized == category)
    elif statistic == 'obs_digitized':
        # `np.digitize(x, bins, right=True)` returns index `i` such that:
        #   `bins[i-1] < x <= bins[i]`
        # Indices range from 0 (for x <= bins[0]) to len(bins) (for x > bins[-1]).
        # `bins` for np.digitize will be `thresholds_np`.
        obs_digitized = xr.apply_ufunc(
            np.digitize,
            obs,
            kwargs={'bins': bin_thresholds, 'right': True},
            dask='parallelized',
            output_dtypes=[int],
        )
        # Restore NaN values
        m_ds = obs_digitized.where(obs.notnull(), np.nan)
    elif statistic == 'fcst_digitized':
        fcst_digitized = xr.apply_ufunc(
            np.digitize,
            fcst,
            kwargs={'bins': bin_thresholds, 'right': True},
            dask='parallelized',
            output_dtypes=[int],
        )
        # Restore NaN values
        m_ds = fcst_digitized.where(fcst.notnull(), np.nan)
    elif statistic == 'squared_fcst':
        m_ds = fcst**2
    elif statistic == 'squared_obs':
        m_ds = obs**2
    elif statistic == 'covariance':
        m_ds = fcst * obs
    elif statistic == 'crps' and enhanced_prob_type == 'ensemble':
        fcst = fcst.chunk(member=-1, time=1, lead_time=1, lat=250, lon=250)  # member must be -1 to succeed
        m_ds = xskillscore.crps_ensemble(observations=obs, forecasts=fcst, mean=False, dim=['time', 'lead_time'])
    elif statistic == 'crps' and enhanced_prob_type == 'quantile':
        m_ds = SpatialQuantileCRPS(quantile_dim='member').compute(forecast=fcst, truth=obs, avg_time=False, skipna=True)
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
    elif statistic == 'n_valid':
        m_ds = xr.ones_like(fcst)
        m_ds = m_ds.where(fcst.notnull(), 0.0, drop=False).astype(float)
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
    # TODO: Delete, keeping around for cachable function atm
    pass


@dask_remote
@cacheable(data_type='array',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast', 'truth',
                       'metric', 'time_grouping', 'spatial', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 30, 'region': 300, 'lead_time': -1},
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

    # Get admin level for grouping
    admin_level, is_admin = get_admin_level(region)
    if is_admin and spatial:
        raise ValueError(f"Cannot compute spatial metrics for admin level '{region}'. " +
                         "Pass in a specific region instead.")

    # Get time level for grouping
    time_level = get_time_level(time_grouping)

    # Set up sparsity for the metric
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
    for statistic in metric_obj.statistics:
        # Statistics can be a tuple of (statistic, agg_fn), or just a statistic with default mean agg
        if isinstance(statistic, tuple):
            statistic, agg_fn = statistic
        else:
            agg_fn = 'mean'

        ds = global_statistic(start_time, end_time, variable, lead=lead,
                              forecast=forecast, truth=truth,
                              statistic=statistic,
                              bins=bins, metric_info=metric_obj, grid=grid)
        if ds is None:
            return None
        data_sparse = ds.attrs['sparse']  # Whether the input data to the statistic is expected to be sparse

        ############################################################
        # Aggregate and and check validity of the statistic
        ############################################################
        # Drop any extra random coordinates that shouldn't be there
        for coord in ds.coords:
            if coord not in ['time', 'lead_time', 'lat', 'lon']:
                ds = ds.reset_coords(coord, drop=True)

        # Prepare the check_ds for validity checking, considering sparsity
        if data_sparse or metric_sparse:
            print("Metric is sparse, need to check the underlying forecast validity directly.")
            fcst_fn = get_datasource_fn(forecast)

            if 'lead' in signature(fcst_fn).parameters:
                check_ds = fcst_fn(start_time, end_time, variable, lead=lead,
                                   prob_type=metric_obj.prob_type, grid=grid, mask='lsm', region='global')
            else:
                check_ds = fcst_fn(start_time, end_time, variable, agg_days=get_lead_info(lead)['agg_days'],
                                   grid=grid, mask='lsm', region='global')
        else:
            check_ds = ds.copy()
        check_ds = check_ds.notnull().astype(float)
        # Create a non_null indicator
        ds['non_null'] = check_ds[variable]
        ############################################################
        # Statistic aggregation
        ############################################################
        # Group by time
        ds = groupby_time(ds, time_level, agg_fn=agg_fn)

        # For any lat / lon / lead where there is at least one non-null value, reset to one for space validation
        ds['non_null'] = (ds['non_null'] > 0.0).astype(float)
        # Create an indicator variable that is 1 for all dimensions
        ds['indicator'] = xr.ones_like(ds['non_null'])

        # Add the region coordinate to the statistic
        region_ds = region_labels(grid=grid, admin_level=admin_level)
        ds = ds.assign_coords(region=region_ds.region)

        # Aggregate in space
        mask_ds = get_mask(mask, grid)
        if not spatial:
            if agg_fn == 'mean':
                # Group by region and average in space, while applying weighting for mask
                # and latitudes
                weights = latitude_weights(ds, lat_dim='lat', lon_dim='lon')
                ds['weights'] = weights * mask_ds.mask
            else:
                ds['weights'] = mask_ds.mask

            ds[variable] = ds[variable] * ds['weights']
            ds['non_null'] = ds['non_null'] * mask_ds.mask
            ds['indicator'] = ds['indicator'] * mask_ds.mask

            ds = ds.groupby('region').apply(mean_or_sum, agg_fn='sum', dims='stacked_lat_lon')
            if agg_fn == 'mean':
                # Normalize by weights
                ds[variable] = ds[variable] / ds['weights']
            ds = ds.drop_vars(['weights'])
        else:
            # Mask and drop the region coordinate
            ds = ds.where(mask_ds.mask, np.nan, drop=False)
            ds = ds.where((ds.region == region).compute(), drop=True)
            ds = ds.drop_vars('region')

        # Check if the statistic is valid per grouping
        is_valid = (ds['non_null'] / ds['indicator'] > 0.98)
        ds = ds.where(is_valid, np.nan, drop=False)
        ds = ds.drop_vars(['indicator', 'non_null'])

        # Assign the final statistic value
        statistic_values[statistic] = ds.copy()

    # Finally, compute the metric based on the aggregated statistic values
    m_ds = metric_obj.compute(statistic_values)
    return m_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'metric', 'baseline',
                       'time_grouping', 'spatial', 'grid', 'region'],
           cache=False)
def skill_metric(start_time, end_time, variable, lead, forecast, truth,
                 metric, baseline, time_grouping=None, spatial=False, grid="global1_5",
                 region='global'):
    """Compute skill either spatially or as a region summary."""
    try:
        m_ds = grouped_metric(start_time, end_time, variable, lead, forecast,
                              truth, metric, time_grouping, spatial=spatial,
                              grid=grid, region=region)
    except NotImplementedError:
        return None
    if not m_ds:
        return None

    # Get the baseline if it exists and run its metric
    base_ds = grouped_metric(start_time, end_time, variable, lead, baseline,
                             truth, metric, time_grouping, spatial=spatial, grid=grid, region=region)
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
                           grid='global1_5', region='global'):
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
                                    grid=grid, region=region,
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
                       'time_grouping', 'grid', 'region'],
           hash_postgres_table_name=True,
           backend='postgres',
           cache=True,
           primary_keys=['time', 'forecast'])
def summary_metrics_table(start_time, end_time, variable,
                          truth, metric, time_grouping=None,
                          grid='global1_5', region='global'):
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
                                grid=grid, region=region)

    print(df)
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric',
                       'time_grouping', 'grid', 'region'],
           cache=True,
           primary_keys=['time', 'forecast'])
def seasonal_metrics_table(start_time, end_time, variable,
                           truth, metric, time_grouping=None,
                           grid='global1_5', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['salient', 'climatology_2015']
    leads = ["month1", "month2", "month3"]
    df = _summary_metrics_table(start_time, end_time, variable, truth, metric, leads, forecasts,
                                time_grouping=time_grouping,
                                grid=grid, region=region)

    print(df)
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric',
                       'time_grouping', 'grid', 'region'],
           cache=True,
           primary_keys=['time', 'forecast'])
def station_metrics_table(start_time, end_time, variable,
                          truth, metric, time_grouping=None,
                          grid='global1_5', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['era5', 'chirps', 'imerg', 'cbam']
    leads = ["daily", "weekly", "biweekly", "monthly"]
    df = _summary_metrics_table(start_time, end_time, variable, truth, metric, leads, forecasts,
                                time_grouping=time_grouping,
                                grid=grid, region=region)

    print(df)
    return df


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric',
                       'time_grouping', 'grid', 'region'],
           cache=True)
def biweekly_summary_metrics_table(start_time, end_time, variable,
                                   truth, metric, time_grouping=None,
                                   grid='global1_5', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['perpp', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015',
                 'climatology_trend_2015', 'climatology_rolling']
    leads = ["weeks34", "weeks56"]
    df = _summary_metrics_table(start_time, end_time, variable, truth, metric, leads, forecasts,
                                time_grouping=time_grouping,
                                grid=grid, region=region)

    print(df)
    return df


def mean_or_sum(ds, agg_fn, dims=['lat', 'lon']):
    # Note, for some reason:
    # ds.groupby('region').mean(['lat', 'lon'], skipna=True).compute()
    # raises:
    # *** AttributeError: 'bool' object has no attribute 'blockwise'
    # or
    # *** TypeError: reindex_intermediates() missing 1 required positional argument: 'array_type'
    # So we have to do it via apply
    if agg_fn == 'mean':
        return ds.mean(dims, skipna=True)
    else:
        return ds.sum(dims, skipna=True)


def groupby_time(ds, time_grouping, agg_fn='mean'):
    """Aggregate a statistic over time."""
    if time_grouping is not None:
        if time_grouping == 'month_of_year':
            coords = [f'M{x:02d}' for x in ds.time.dt.month.values]
        elif time_grouping == 'year':
            coords = [f'Y{x:04d}' for x in ds.time.dt.year.values]
        elif time_grouping == 'quarter_of_year':
            coords = [f'Q{x:02d}' for x in ds.time.dt.quarter.values]
        elif time_grouping == 'day_of_year':
            coords = [f'D{x:03d}' for x in ds.time.dt.dayofyear.values]
        elif time_grouping == 'month':
            coords = [f'{pd.to_datetime(x).year:04d}-{pd.to_datetime(x).month:02d}-01' for x in ds.time.values]
        else:
            raise ValueError("Invalid time grouping")
        ds = ds.assign_coords(group=("time", coords))
        ds = ds.groupby("group").apply(mean_or_sum, agg_fn=agg_fn, dims='time')
        # Rename the group coordinate to time
        ds = ds.rename({"group": "time"})
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


def latitude_weights(ds, lat_dim='lat', lon_dim='lon'):
    """Return latitude weights as an xarray DataArray.

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
    # Return an xarray DataArray with dimensions lat
    weights = xr.DataArray(weights, coords=[ds[lat_dim]], dims=[lat_dim])
    weights = weights.expand_dims({lon_dim: ds[lon_dim]})
    return weights


def latitude_weighted_spatial_average(ds, lat_dim='lat', lon_dim='lon', agg_fn='mean'):
    """Compute latitude-weighted spatial average of a dataset.

    This function weights each latitude band by the actual cell area,
    which accounts for the fact that grid cells near the poles are smaller
    in area than those near the equator.
    """
    weights = latitude_weights(ds, lat_dim)
    # Create weights array
    weights = ds[lat_dim].copy(data=weights)
    if f'stacked_{lat_dim}_{lon_dim}' in ds.coords:
        agg_dims = [f'stacked_{lat_dim}_{lon_dim}']
    else:
        agg_dims = [lat_dim, lon_dim]
    if agg_fn == 'mean':
        weighted = ds.weighted(weights).mean(agg_dims, skipna=True)
    else:
        weighted = ds.weighted(weights).sum(agg_dims, skipna=True)
    return weighted


__all__ = ['global_statistic', 'grouped_metric', 'skill_metric',
           'summary_metrics_table', 'biweekly_summary_metrics_table']
