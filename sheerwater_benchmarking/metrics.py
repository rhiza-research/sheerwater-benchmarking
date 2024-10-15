"""Verification metrics for forecasts."""
from importlib import import_module


from sheerwater_benchmarking.utils import cacheable, dask_remote


def get_forecast_and_truth_fn(forecast, truth):
    """Import the forecast and truth functions."""
    # Import forecast
    try:
        mod = import_module("sheerwater_benchmarking.forecasts")
        fcst_fn = getattr(mod, forecast)
    except ImportError:
        raise ImportError(f"Could not find forecast {forecast}")

    # Import truth
    try:
        mod = import_module("sheerwater_benchmarking.reanalysis")
        truth_fn = getattr(mod, truth)
    except ImportError:
        try:
            mod = import_module("sheerwater_benchmarking.forecasts")
            truth_fn = getattr(mod, truth)
        except ImportError:
            raise ImportError(f"Could not find truth {truth}.")

    return fcst_fn, truth_fn


def get_metric_fn(prob_type, metric):
    """Import the metric function."""
    wb_metrics = {
        'crps': ('CRPS', {'ensemble_dim': 'member'}),
        'crps-q': ('QuantileCRPS', {'quantile_dim': 'member'}),
        'spatial-crps': ('SpatialCRPS', {'ensemble_dim': 'member'}),
        'spatial-crps-q': ('SpatialQuantileCRPS', {'quantile_dim': 'member'}),
        'mae': ('MAE', {}),
        'spatial-mae': ('SpatialMAE', {})
    }
    metric_mod, metric_kwargs = wb_metrics[metric + '-q' if prob_type == 'quantile' else metric]
    mod = import_module("weatherbench2.metrics")
    metric_fn = getattr(mod, metric_mod)
    return metric_fn, metric_kwargs


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'prob_type', 'metric', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           cache=False)
def spatial_metric(start_time, end_time, variable, lead, forecast, truth,
                   prob_type, metric, grid="global1_5", mask='lsm', region='africa'):
    """Compute a metric for a forecast at a specific lead."""
    fcst_fn, truth_fn = get_forecast_and_truth_fn(forecast, truth)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead, prob_type=prob_type, grid=grid, mask=mask, region=region)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    metric_fn, metric_kwargs = get_metric_fn(prob_type, metric)
    m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, skipna=True)
    m_ds = m_ds.rename_vars({variable: f'{variable}_{metric}'})
    m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    # Convert to standard naming
    return m_ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'lead', 'forecast', 'truth', 'prob_type', 'metric', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           cache=False)
def summary_metric(start_time, end_time, variable, lead, forecast, truth,
                   prob_type, metric, grid="global1_5", mask='lsm', region='africa'):
    """Compute a metric for a forecast at a specific lead."""
    fcst_fn, truth_fn = get_forecast_and_truth_fn(forecast, truth)
    fcst = fcst_fn(start_time, end_time, variable, lead=lead, prob_type=prob_type, grid=grid, mask=mask, region=region)
    obs = truth_fn(start_time, end_time, variable, lead=lead, grid=grid, mask=mask, region=region)

    metric_fn, metric_kwargs = get_metric_fn(prob_type, metric)
    m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, skipna=True)
    m_ds = m_ds.rename_vars({variable: f'{variable}_{metric}'})

    # Convert to standard naming
    return m_ds
