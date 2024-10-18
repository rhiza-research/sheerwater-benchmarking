"""Verification metrics for forecasts."""
from importlib import import_module


from sheerwater_benchmarking.utils import cacheable, dask_remote


def get_forecast_fn(forecast):
    """Import the forecast and truth functions."""
    # Import truth
    try:
        mod = import_module("sheerwater_benchmarking.reanalysis")
        fn = getattr(mod, forecast)
    except (ImportError, AttributeError):
        try:
            mod = import_module("sheerwater_benchmarking.forecasts")
            fn = getattr(mod, forecast)
        except (ImportError, AttributeError):
            try:
                mod = import_module("sheerwater_benchmarking.baselines")
                fn = getattr(mod, forecast)
            except (ImportError, AttributeError):
                raise ImportError(f"Could not find truth {truth}.")

    return fn


def get_metric_fn(prob_type, metric, spatial=True):

    # Make sure things are conssitent
    if prob_type == 'deterministic' and metric == 'crps':
        raise ValueError("Cannot run CRPS on deterministic forecasts.")
    elif (prob_type == 'ensemble' or prob_type == 'quantile') and metric == 'mae':
        raise ValueError("Cannot run MAE on probababilistic forecasts.")



    """Import the metric function."""
    wb_metrics = {
        'crps': ('CRPS', {'ensemble_dim': 'member'}),
        'crps-q': ('QuantileCRPS', {'quantile_dim': 'member'}),
        'spatial-crps': ('SpatialCRPS', {'ensemble_dim': 'member'}),
        'spatial-crps-q': ('SpatialQuantileCRPS', {'quantile_dim': 'member'}),
        'mae': ('MAE', {}),
        'spatial-mae': ('SpatialMAE', {})
    }

    if spatial:
        metric = 'spatial-' + metric

    if prob_type == 'quantile':
        metric = metric + '-q'

    try:
        metric_mod, metric_kwargs = wb_metrics[metric]
        mod = import_module("weatherbench2.metrics")
        metric_fn = getattr(mod, metric_mod)
        return metric_fn, metric_kwargs
    except:
        raise ImportError("Did not find implementation for metric {metric}")


@dask_remote
def combined_metric(start_time, end_time, variable, lead, forecast, truth,
                    metric, baseline=None, grid="global1_5", mask='lsm', region='africa', spatial=True):
    """Compute a metric for a forecast at a specific lead."""

    if metric == "crps":
        prob_type = "probabalistic"
    elif metric == "mae":
        prob_type = "deterministic"
    else:
        raise ValueError("Unsupported metric")

    # Get the forecast
    fcst_fn = get_forecast_fn(forecast)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead, prob_type=prob_type, grid=grid, mask=mask, region=region)

    # Get the truth to compare against
    truth_fn = get_forecast_fn(truth)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    # Check to see the prob type attribute
    enhanced_prob_type = fcst.attrs['prob_type']

    metric_fn, metric_kwargs = get_metric_fn(enhanced_prob_type, metric, spatial=spatial)
    m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, skipna=True)

    # Get the baseline if it exists and run its metric
    if baseline:
        baseline_fn = get_forecast_fn(baseline)
        baseline_output = baseline_fn(start_time, end_time, variable, lead=lead, prob_type=prob_type, grid=grid, mask=mask, region=region)

        # Check to see the prob type attribute
        enhanced_prob_type = baseline_output.attrs['prob_type']

        metric_fn, metric_kwargs = get_metric_fn(enhanced_prob_type, metric, spatial=spatial)
        base_ds = metric_fn(**metric_kwargs).compute(forecast=baseline_output, truth=obs, skipna=True)

        # Compute the skill
        m_ds = (1 - (m_ds/base_ds))

    return m_ds

@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'metric', 'baseline', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           cache=False)
def spatial_metric(start_time, end_time, variable, lead, forecast, truth,
                   metric, baseline=None, grid="global1_5", mask='lsm', region='africa'):

    m_ds =  combined_metric(start_time, end_time, variable, lead, forecast, truth,
                            metric, baseline, grid, mask, region, spatial=True)

    # Convert to standard naming
    m_ds = m_ds.rename_vars({variable: f'{variable}_{metric}'})
    m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    return m_ds

@dask_remote
@cacheable(data_type='tabular',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'metric', 'baseline', 'grid', 'mask', 'region'],
           cache=False)
def summary_metric(start_time, end_time, variable, lead, forecast, truth,
                   metric, baseline=None, grid="global1_5", mask='lsm', region='africa'):

    m_ds = combined_metric(start_time, end_time, variable, lead, forecast, truth,
                           metric, baseline, grid, mask, region, spatial=False)

    m_ds = m_ds.rename_vars({variable: f'{variable}_{metric}'})

    return m_ds

#@dask_remote
#@cacheable(data_type='tabular',
#           cache_args=['variable', 'lead', 'forecast', 'truth', 'prob_type', 'metric', 'grid', 'mask', 'region'],
#           cache=True)
#def summary_metrics_combined(start_time, end_time, variable, lead, forecast, truth,
#                   prob_type, metric, grid="global1_5", mask='lsm', region='africa'):
