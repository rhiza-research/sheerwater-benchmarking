"""Utility functions for benchmarking."""
from .caching_utils import cacheable
from .remote import dask_remote
from .secrets import cdsapi_secret, ecmwf_secret, salient_secret, salient_auth
from .data_utils import apply_mask, roll_and_agg, regrid
from .general_utils import (load_netcdf, write_zarr, load_zarr,
                            is_valid_forecast_date,
                            get_dates, get_variable, get_grid)


# Use __all__ to define what is part of the public API.
__all__ = [
    cacheable,
    dask_remote,
    cdsapi_secret,
    ecmwf_secret,
    salient_secret,
    salient_auth,
    apply_mask,
    roll_and_agg,
    regrid,
    load_netcdf,
    write_zarr,
    load_zarr,
    is_valid_forecast_date,
    get_dates,
    get_variable,
    get_grid,
]
