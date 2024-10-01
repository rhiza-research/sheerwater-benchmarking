"""Verification metrics for forecasts."""
from importlib import import_module

from weatherbench2 import metrics

from sheerwater_benchmarking.utils import cacheable, dask_remote
from sheerwater_benchmarking.reanalysis import era5


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid', 'forecast'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           cache=False,
           auto_rechunk=False)
def spatial_mae(start_time, end_time, variable, forecast, lead, grid="global1_5", mask="lsm"):
    """Compute the MAE for a forecast at a specific lead."""
    try:
        mod = import_module("sheerwater_benchmarking.forecasts")
        fcst = getattr(mod, forecast)
    except ImportError:
        raise ImportError(f"Could not find forecast {forecast}")

    ds = fcst(start_time, end_time, variable, lead=lead, dorp='d', grid=grid, mask=mask)
    obs = era5(start_time, end_time, variable, lead=lead)

    val_ds = metrics.SpatialMAE().compute(forecast=ds, truth=obs, skipna=True)

    # Convert to standard naming
    val_ds = val_ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    val_ds = val_ds.rename_vars({variable: f'{variable}_mae'})
    return val_ds
