"""Verification metrics for forecasts."""
from importlib import import_module

import xarray as xr
import dask

from weatherbench2 import metrics

from sheerwater_benchmarking.utils import cacheable, dask_remote
from sheerwater_benchmarking.reanalysis import era5


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid', 'lead', 'forecast', 'prob_type', 'metric', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           cache=False,
           auto_rechunk=False)
def verification(start_time, end_time, variable, lead, forecast, prob_type,
                 metric, grid="global1_5", mask="lsm"):
    """Compute a metric for a forecast at a specific lead."""
    try:
        mod = import_module("sheerwater_benchmarking.forecasts")
        fcst_fn = getattr(mod, forecast)
    except ImportError:
        raise ImportError(f"Could not find forecast {forecast}")

    fcst = fcst_fn(start_time, end_time, variable, lead=lead, prob_type=prob_type, grid=grid, mask=mask)
    obs = era5(start_time, end_time, variable, lead=lead)

    if type(metric) is not list:
        metric = [metric]

    datasets = []
    for m in metric:
        metric_mod, metric_kwargs = {
            'crps': ('CRPS', {'ensemble_dim': 'member'}),
            'crps-q': ('QuantileCRPS', {'quantile_dim': 'member'}),
            'spatial-crps': ('SpatialCRPS', {'ensemble_dim': 'member'}),
            'spatial-crps-q': ('SpatialQuantileCRPS', {'quantile_dim': 'member'}),
            'mae': ('MAE', {}),
            'spatial-mae': ('SpatialMAE', {})
        }[m + '-q' if prob_type == 'quantile' else m]
        mod = import_module("weatherbench2.metrics")
        metric_fn = getattr(mod, metric_mod)

        m_ds = metric_fn(**metric_kwargs).compute(forecast=fcst, truth=obs, skipna=True)
        m_ds = m_ds.rename_vars({variable: f'{variable}_{m}'})
        if 'spatial' in m:
            m_ds = m_ds.rename({'latitude': 'lat', 'longitude': 'lon'})

        # m_ds = dask.delayed(fn)(forecast=fcst, truth=obs, skipna=True)
        datasets.append(m_ds)

    datasets = dask.compute(*datasets)
    val_ds = xr.merge(datasets)
    # Convert to standard naming
    return val_ds
