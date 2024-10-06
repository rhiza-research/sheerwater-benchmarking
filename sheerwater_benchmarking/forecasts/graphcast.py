"""Pulls Salient Predictions S2S forecasts from the Salient API."""


import numpy as np
import xarray as xr

from sheerwater_benchmarking.masks import land_sea_mask
from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           get_variable,
                                           apply_mask,
                                           lon_base_change,
                                           regrid)


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache=False)
def ecmwf_hres_raw(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ECMWF function that returns data from Google Weather Bench."""
    if grid != "global0_25":
        raise NotImplementedError("Only ERA5 native 0.25 degree grid is implemented.")

    # Pull the google dataset
    ds = xr.open_zarr('gs://weatherbench2/datasets/hres/2016-2022-12h-6h-0p25deg-chunk-1.zarr',
                      chunks={'time': 1, 'latitude': 721, 'longitude': 1440,
                              'prediction_timedelta': 41, 'level': 1})

    var = get_variable(variable, 'ecmwf_hres')
    ds = ds[var].to_dataset()
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time')
def ecmwf_hres_daily(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ECMWF function that returns data from Google Weather Bench."""
    if grid != "global0_25":
        raise NotImplementedError("Only ERA5 native 0.25 degree grid is implemented.")

    ds = ecmwf_hres_raw(start_time, end_time, variable, grid=grid)
    var = get_variable(variable, 'ecmwf_hres')
    ds = ds.rename_vars(name_dict={var: variable})

    # Convert hourly data to daily data and then aggregate
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    # Convert local dataset naming and units
    if variable == 'tmp2m':
        if ds[variable].units == 'K':
            ds[variable] = ds[variable] - 273.15
        ds = ds.resample(prediction_timedelta='D').mean(dim='prediction_timedelta')
    elif variable == 'precip':
        if ds[variable].units == 'm':
            ds[variable] = ds[variable] * 1000.0
        ds = ds.resample(prediction_timedelta='D').sum(dim='prediction_timedelta')
        ds = np.maximum(ds, 0)

    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")

    if '0_25' not in grid:
        ds = regrid(ds, grid)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'grid', 'mask'])
def ecmwf_hres_proc(start_time, end_time, variable, grid="africa0_25", mask='lsm'):
    """Processed ECMWF HRES forecast files."""
    ds = ecmwf_hres_daily(start_time, end_time, variable, grid=grid)

    if mask == "lsm":
        # Select variables and apply mask
        mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = None
    else:
        raise NotImplementedError("Only land-sea or None mask is implemented.")

    ds = apply_mask(ds, mask_ds, variable)
    return ds


# @dask_remote
# @cacheable(data_type='array',
#            timeseries='time',
#            cache=False,
#            cache_args=['variable', 'lead', 'dorp', 'grid', 'mask'])
# def ecmwf_hres(start_time, end_time, variable, lead, dorp='d',
#                grid='africa0_25', mask='lsm'):
#     """Standard format forecast data for Salient."""
#     lead_params = {
#         "week1": ("sub-seasonal", 1),
#         "week2": ("sub-seasonal", 2),
#         "week3": ("sub-seasonal", 3),
#         "week4": ("sub-seasonal", 4),
#         "week5": ("sub-seasonal", 5),
#         "month1": ("seasonal", 1),
#         "month2": ("seasonal", 2),
#         "month3": ("seasonal", 3),
#         "quarter1": ("sub-seasonal", 1),
#         "quarter2": ("sub-seasonal", 2),
#         "quarter3": ("sub-seasonal", 3),
#         "quarter4": ("sub-seasonal", 4),
#     }
#     timescale, lead_id = lead_params.get(lead, (None, None))
#     if timescale is None:
#         raise NotImplementedError(f"Lead {lead} not implemented for Salient.")

#     ds = salient_blend_proc(start_time, end_time, variable, grid=grid,
#                             timescale=timescale, mask=mask)
#     ds = ds.sel(lead=lead_id)
#     if dorp == 'd':
#         # Get the median forecast
#         ds = ds.sel(quantiles=0.5)
#         ds['quantiles'] = -1
#     ds = ds.rename({'quantiles': 'member'})
#     ds = ds.rename({'forecast_date': 'time'})

#     return ds
