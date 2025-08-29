"""Utility functions for benchmarking."""
from .caching import cacheable
from .remote import dask_remote, start_remote
from .secrets import cdsapi_secret, ecmwf_secret, salient_secret, salient_auth, tahmo_secret, gap_secret
from .data_utils import (roll_and_agg, get_anomalies, is_valid)
from .general_utils import (load_netcdf, write_zarr, load_zarr, load_object, plot_ds, plot_ds_map, run_in_parallel, get_datasource_fn)

from .space_utils import (get_grid, get_grid_ds, is_wrapped, get_region,
                          base360_to_base180, base180_to_base360, check_bases, clip_region,
                          lon_base_change, get_globe_slice, apply_mask, regrid,  get_admin_level,
                          get_region_labels)

from .time_utils import (is_valid_forecast_date, generate_dates_in_between, get_dates,
                         pad_with_leapdays, add_dayofyear,
                         groupby_time, assign_grouping_coordinates,
                         convert_group_to_time, date_mean, doy_mean, get_time_level)

from .weather_utils import (get_variable, get_lead_info, forecast,
                            forecast_date_to_target_date, target_date_to_forecast_date,
                            shift_forecast_date_to_target_date, convert_lead_to_valid_time,
                            get_lead_group)

from .task_utils import first_satisfied_date

# Use __all__ to define what is part of the public API.
__all__ = [
    "cacheable",
    "dask_remote",
    "cdsapi_secret",
    "ecmwf_secret",
    "salient_secret",
    "tahmo_secret",
    "gap_secret",
    "salient_auth",
    "apply_mask",
    "roll_and_agg",
    "regrid",
    "load_netcdf",
    "write_zarr",
    "load_zarr",
    "load_object",
    "is_valid_forecast_date",
    "generate_dates_in_between",
    "get_dates",
    "get_variable",
    "get_grid",
    "get_grid_ds",
    "clip_region",
    "is_wrapped",
    "get_region",
    "base360_to_base180",
    "base180_to_base360",
    "lon_base_change",
    "get_globe_slice",
    "check_bases",
    "get_anomalies",
    "is_valid",
    "pad_with_leapdays",
    "add_dayofyear",
    "forecast_date_to_target_date",
    "target_date_to_forecast_date",
    "groupby_time",
    "shift_forecast_date_to_target_date",
    "start_remote",
    "plot_ds",
    "plot_ds_map",
    "assign_grouping_coordinates",
    "convert_group_to_time",
    "shift_forecast_date_to_target_date",
    "first_satisfied_date",
    "date_mean",
    "doy_mean",
    "run_in_parallel",
    "forecast",
    "get_leads",
    "get_admin_level",
    "get_time_level",
    "convert_lead_to_valid_time",
    "get_lead_info",
    "get_region_labels",
    "get_lead_group",
]
