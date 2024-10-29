"""Utility functions for benchmarking."""
from .caching import cacheable
from .remote import dask_remote
from .secrets import cdsapi_secret, ecmwf_secret, salient_secret, salient_auth
from .data_utils import (apply_mask, roll_and_agg, regrid, clip_region,
                         lon_base_change, get_globe_slice, get_anomalies, is_valid)
from .general_utils import (load_netcdf, write_zarr, load_zarr, load_object)

from .space_utils import (get_grid, get_grid_ds, is_wrapped,
                          base360_to_base180, base180_to_base360, check_bases)

from .time_utils import is_valid_forecast_date, generate_dates_in_between, get_dates

from .weather_utils import get_variable


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
    load_object,
    is_valid_forecast_date,
    generate_dates_in_between,
    get_dates,
    get_variable,
    get_grid,
    get_grid_ds,
    clip_region,
    is_wrapped,
    base360_to_base180,
    base180_to_base360,
    lon_base_change,
    get_globe_slice,
    check_bases,
    get_anomalies,
    is_valid,
]
