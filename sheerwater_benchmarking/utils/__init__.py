"""Utility functions for benchmarking."""
from .caching import cacheable
from .remote import dask_remote
from .secrets import cdsapi_secret, ecmwf_secret, salient_secret, salient_auth
from .data_utils import (apply_mask, roll_and_agg, regrid,
                         lon_base_change, get_globe_slice, plot_map)
from .general_utils import (load_netcdf, write_zarr, load_zarr,
                            is_valid_forecast_date,
                            get_dates, get_variable, get_grid, get_global_grid,
                            is_wrapped, base360_to_base180, base180_to_base360,
                            check_bases)


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
    get_global_grid,
    is_wrapped,
    base360_to_base180,
    base180_to_base360,
    lon_base_change,
    get_globe_slice,
    plot_map,
    check_bases
]
