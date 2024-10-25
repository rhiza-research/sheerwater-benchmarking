"""Verification metrics for forecasts."""
from importlib import import_module
import itertools

import pandas as pd
import dask

from sheerwater_benchmarking.utils import cacheable, dask_remote


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
    if prob_type == 'deterministic' and metric == 'crps':
        raise ValueError("Cannot run CRPS on deterministic forecasts.")
    elif (prob_type == 'ensemble' or prob_type == 'quantile') and metric == 'mae':
        raise ValueError("Cannot run MAE on probabilistic forecasts.")

    wb_metrics = {
        'crps': ('xskillscore', 'crps_ensemble', {}),
        'crps-q': ('weatherbench2.metrics', 'QuantileCRPS', {'quantile_dim': 'member'}),
        'spatial-crps': ('xskillscore', 'crps_ensemble', {'dim': 'time'}),
        'spatial-crps-q': ('weatherbench2.metrics', 'SpatialQuantileCRPS', {'quantile_dim': 'member'}),
        'mae': ('weatherbench2.metrics', 'MAE', {}),
        'spatial-mae': ('weatherbench2.metrics', 'SpatialMAE', {}),
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
@cacheable(data_type='basic',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast', 'truth', 'metric', 'grid', 'mask', 'region'],
           cache=True)
def single_metric(start_time, end_time, variable, lead, forecast, truth,
                  metric, grid="global1_5", mask='lsm', region='africa'):
    """Compute a metric for a forecast at a specific lead."""
    if metric == "crps":
        prob_type = "probabilistic"
    elif metric == "mae":
        prob_type = "deterministic"
    else:
        raise ValueError("Unsupported metric")

    # Get the forecast
    fcst_fn = get_datasource_fn(forecast)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                   prob_type=prob_type, grid=grid, mask=mask, region=region)

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    # Check to see the prob type attribute
    enhanced_prob_type = fcst.attrs['prob_type']

    metric_fn, metric_kwargs, metric_lib = get_metric_fn(enhanced_prob_type, metric, spatial=False)
    if metric_lib == 'xskillscore':
        assert prob_type == 'probabilistic'
        fcst = fcst.chunk(member=-1, time=1, lat=100, lon=100)
        m_ds = metric_fn(observations=obs, forecasts=fcst, **metric_kwargs)
    else:
        m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, skipna=True)

    return m_ds[variable].values.max()

@dask_remote
@cacheable(data_type='array',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast', 'truth', 'metric', 'grid', 'mask', 'region'],
           cache=True)
def single_spatial_metric(start_time, end_time, variable, lead, forecast, truth,
                  metric, grid="global1_5", mask='lsm', region='africa'):
    """Compute a metric for a forecast at a specific lead."""
    if metric == "crps":
        prob_type = "probabilistic"
    elif metric == "mae":
        prob_type = "deterministic"
    else:
        raise ValueError("Unsupported metric")

    # Get the forecast
    fcst_fn = get_datasource_fn(forecast)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead,
                   prob_type=prob_type, grid=grid, mask=mask, region=region)

    # Get the truth to compare against
    truth_fn = get_datasource_fn(truth)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    # Check to see the prob type attribute
    enhanced_prob_type = fcst.attrs['prob_type']

    metric_fn, metric_kwargs, metric_lib = get_metric_fn(enhanced_prob_type, metric, spatial=True)
    if metric_lib == 'xskillscore':
        assert prob_type == 'probabilistic'
        fcst = fcst.chunk(member=-1, time=1, lat=100, lon=100)
        m_ds = metric_fn(observations=obs, forecasts=fcst, **metric_kwargs)
    else:
        m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, skipna=True)

    return m_ds


@dask_remote
def _metric(start_time, end_time, variable, lead, forecast, truth,
            metric, baseline=None, grid="global1_5", mask='lsm', region='africa', spatial=True):
    """Compute a metric for a forecast at a specific lead."""

    if spatial:
        m_ds = single_spatial_metric(start_time, end_time, variable, lead, forecast, truth, metric, grid=grid, mask=mask, region=region)
    else:
        print("Calling metric!")
        m_ds = single_metric(start_time, end_time, variable, lead, forecast, truth, metric, grid=grid, mask=mask, region=region)
        print("Returned metric!")

    # Get the baseline if it exists and run its metric
    if baseline:
        if spatial:
            base_ds = single_spatial_metric(start_time, end_time, variable, lead, baseline, truth, metric, grid=grid, mask=mask, region=region)
        else:
            base_ds = single_metric(start_time, end_time, variable, lead, baseline, truth, metric, grid=grid, mask=mask, region=region)

        print("Got metrics. Computing skill")
        # Compute the skill
        m_ds = (1 - (m_ds/base_ds))

    return m_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'metric', 'baseline', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           chunk_by_arg={
               'grid': {
                   'global0_25': {"lat": 721, "lon": 1440, 'time': 30}
               }
           },
           cache=False)
def spatial_metric(start_time, end_time, variable, lead, forecast, truth,
                   metric, baseline=None, grid="global1_5", mask='lsm', region='global'):
    """Runs and caches a geospatial metric."""
    m_ds = _metric(start_time, end_time, variable, lead, forecast, truth,
                   metric, baseline, grid, mask, region, spatial=True)

    # Convert to standard naming
    m_ds = m_ds.rename_vars({variable: f'{variable}_{metric}'})
    if 'latitude' in m_ds.dims:
        m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    return m_ds


@dask_remote
@cacheable(data_type='basic',
           cache_args=['start_time', 'end_time', 'variable', 'lead', 'forecast', 'truth', 'metric', 'baseline', 'grid', 'mask', 'region'],
           cache=False)
def summary_metric(start_time, end_time, variable, lead, forecast, truth,
                   metric, baseline=None, grid="global1_5", mask='lsm', region='global'):
    """Runs and caches a summary metric."""
    try:
        m_ds = _metric(start_time, end_time, variable, lead, forecast, truth,
                       metric, baseline, grid, mask, region, spatial=False)
    except NotImplementedError:
        return None

    return m_ds


@dask_remote
@cacheable(data_type='tabular',
           cache_args=['start_time', 'end_time', 'variable', 'truth', 'metric', 'baseline', 'grid', 'mask', 'region'],
           cache=True)
def summary_metrics_table(start_time, end_time, variable, truth, metric, baseline=None, grid='global1_5', mask='lsm', region='global'):
    """Runs summary metric repeatedly for all forecasts and creates a pandas table out of them."""

    forecasts = ['salient', 'ecmwf_ifs_er', 'ecmwf_ifs_er_debiased', 'climatology_2015']
    leads = ["week1", "week2", "week3", "week4", "week5"]

    # Create a dict to insert our data
    results = {forecast:[] for forecast in forecasts}

    combos = itertools.product(forecasts, leads)
    for forecast in forecasts:
        lead_vals = []
        for lead in leads:
            # Try running as dask delayed
            print(f"Running for {forecast} and {lead} with variable {variable}, metric {metric}, grid {grid}, and region {region}")
            # First get the value without the baseline
            val = dask.delayed(summary_metric)(start_time, end_time, variable, lead, forecast, truth, metric, None, grid, mask, region)
            lead_vals.append(val)

            # IF there is a baseline get the skill
            if baseline:
                val = dask.delayed(summary_metric)(start_time, end_time, variable, lead, forecast, truth, metric, baseline, grid, mask, region)
                lead_vals.append(val)

        l = dask.compute(*lead_vals)
        results[forecast] = l

    # Turn the dict into a pandas dataframe with appropriate columns
    leads_skill = [lead + '_skill' for lead in leads]

    # interleave
    cols = leads + leads_skill
    cols[::2] = leads
    cols[1::2] = leads_skill

    print(results)

    # create the dataframe
    df = pd.DataFrame.from_dict(results, orient='index', columns=cols)

    # Rename the index
    df = df.reset_index().rename(columns={'index':'forecast'})

    print(df)
    return df

