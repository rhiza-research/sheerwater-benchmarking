"""Verification metrics for forecasts."""
from importlib import import_module

import xarray as xr

from sheerwater_benchmarking.baselines import climatology_agg_raw
from sheerwater_benchmarking.utils import cacheable, dask_remote, clip_region, is_valid
from weatherbench2.metrics import _spatial_average

PROB_METRICS = ['crps']  # a list of probabilistic metrics
CLIM_METRICS = ['acc', 'seeps']  # a list of probabilistic metrics


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


def get_metric_fn(prob_type, metric, clim_ds=None, spatial=True):
    """Import the correct metrics function from weatherbench."""
    # Make sure things are consistent
    if prob_type == 'deterministic' and metric in PROB_METRICS:
        raise ValueError("Cannot run CRPS on deterministic forecasts.")
    elif (prob_type == 'ensemble' or prob_type == 'quantile') and metric not in PROB_METRICS:
        raise ValueError("Cannot run MAE on probabilistic forecasts.")

    wb_metrics = {
        'crps': ('xskillscore', 'crps_ensemble', {}),
        'crps-q': ('weatherbench2.metrics', 'QuantileCRPS', {'quantile_dim': 'member'}),
        'spatial-crps': ('xskillscore', 'crps_ensemble', {'dim': 'time'}),
        'spatial-crps-q': ('weatherbench2.metrics', 'SpatialQuantileCRPS', {'quantile_dim': 'member'}),
        'mae': ('weatherbench2.metrics', 'MAE', {}),
        'spatial-mae': ('weatherbench2.metrics', 'SpatialMAE', {}),
        'acc': ('weatherbench2.metrics', 'ACC', {'climatology': clim_ds}),
        'spatial-acc': ('weatherbench2.metrics', 'SpatialACC', {'climatology': clim_ds}),
        'mse': ('weatherbench2.metrics', 'MSE', {}),
        'spatial-mse': ('weatherbench2.metrics', 'SpatialMSE', {}),
        'rmse': ('weatherbench2.metrics', 'RMSESqrtBeforeTimeAvg', {}),
        'spatial-rmse': ('weatherbench2.metrics', 'SpatialRMSE', {}),
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

    # Get the forecast
    fcst_fn = get_datasource_fn(forecast)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                   prob_type=prob_type, grid=grid, mask=mask, region=region)

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    # Check to see the prob type attribute
    enhanced_prob_type = fcst.attrs['prob_type']

    if metric in CLIM_METRICS:
        if 'weeks' in lead:
            agg = 14  # biweekly
        elif 'week' in lead:
            agg = 7  # weekly
        else:
            raise ValueError("Cannot compute climatology for this metric.")
        # Use NOAA definition of climatology for evaluation
        clim_ds = climatology_agg_raw(variable, first_year=1991, last_year=2020, agg=agg,
                                      grid=grid, mask=mask, region=region)
        # Reset day of year column to an integer
        clim_ds['dayofyear'] = clim_ds['dayofyear'].dt.dayofyear
    else:
        clim_ds = None
    metric_fn, metric_kwargs, metric_lib = get_metric_fn(
        enhanced_prob_type, metric, clim_ds=clim_ds, spatial=spatial)

    # Run the metric without aggregating in time or space
    if metric_lib == 'xskillscore':
        assert metric == 'crps'
        fcst = fcst.chunk(member=-1, time=1, lat=100, lon=100)
        m_ds = metric_fn(observations=obs, forecasts=fcst, mean=avg_time, **metric_kwargs)
    else:
        m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, avg_time=avg_time, skipna=True)
        if spatial:
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    return m_ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
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
    """Compute a metric without aggregated in time or space at a specific lead."""
    if region != 'global':
        raise ValueError('Global metric must be run with region global.')
    m_ds = eval_metric(start_time, end_time, variable, lead, forecast, truth,
                       metric, spatial=True, avg_time=False,
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
    # Get the unaggregated metric
    ds = global_metric(start_time, end_time, variable, lead, forecast, truth,
                       metric, grid, mask, region='global')

    # Check to make sure it supports this region/time
    if not is_valid(ds, variable, mask, region, grid, valid_threshold=0.95):
        return None

    # Clip it to the region
    ds = clip_region(ds, region)

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

    for coord in ds.coords:
        if coord not in ['time', 'lat', 'lon']:
            ds = ds.reset_coords(coord, drop=True)

    # Average in space
    if spatial:
        return ds
    else:
        return _spatial_average(ds, lat_dim='lat', lon_dim='lon', skipna=True)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'metric', 'baseline',
                       'time_grouping', 'spatial', 'grid', 'mask', 'region'],
           cache=False)
def skill_metric(start_time, end_time, variable, lead, forecast, truth,
                 metric, baseline, time_grouping=None, spatial=False, grid="global1_5",
                 mask='lsm', region='global'):
    """Compute skill either spatially or as a region summary."""
    m_ds = grouped_metric(start_time, end_time, variable, lead, forecast,
                          truth, metric, time_grouping, spatial=spatial, grid=grid, mask=mask, region=region)

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
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric', 'baseline',
                       'time_grouping', 'grid', 'mask', 'region'],
           cache=True)
def summary_metrics_table(start_time, end_time, variable,
                          truth, metric, baseline=None, time_grouping=None,
                          grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""
    forecasts = ['salient', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015',
                 'climatology_trend_2015', 'climatology_rolling']
    leads = ["week1", "week2", "week3", "week4", "week5"]

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
            ds = grouped_metric(start_time, end_time, variable, lead, forecast, truth,
                                metric, time_grouping, False, grid, mask, region)

            if ds:
                ds = ds.rename({variable: lead})
                ds = ds.expand_dims({'forecast': [forecast]}, axis=0)
                results_ds = xr.combine_by_coords([results_ds, ds])

            # IF there is a baseline get the skill
            if baseline:
                try:
                    skill_ds = skill_metric(start_time, end_time, variable, lead, forecast, truth,
                                            metric, baseline, time_grouping, False, grid, mask, region)
                except NotImplementedError:
                    # IF we raise then return early - we can't do this baseline
                    return None

                if skill_ds:
                    skill_ds = skill_ds.rename({variable: leads_skill[i]})
                    skill_ds = skill_ds.expand_dims({'forecast': [forecast]}, axis=0)
                    results_ds = xr.combine_by_coords([results_ds, skill_ds])

    if not time_grouping:
        results_ds = results_ds.reset_coords('time', drop=True)

    df = results_ds.to_dataframe()

    # Rename the index
    df = df.reset_index().rename(columns={'index': 'forecast'})

    print(df)
    return df


__all__ = ['eval_metric', 'global_metric', 'grouped_metric', 'skill_metric', 'summary_metrics_table']
