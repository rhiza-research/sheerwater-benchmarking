"""Pulls Salient Predictions S2S forecasts from the Salient API."""
import os
import dask
from urllib.error import HTTPError


import numpy as np
import xarray as xr
import salientsdk as sk

from sheerwater_benchmarking.masks import land_sea_mask
from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           generate_dates_in_between,
                                           salient_auth,
                                           get_grid, get_dates,
                                           roll_and_agg, apply_mask)


@salient_auth
def get_salient_loc(grid):
    """Get and upload the location object for the Salient API."""
    if grid != "salient_africa0_25":
        raise NotImplementedError("Only the Salient African 0.25 grid is supported.")

    # Upload location shapefile to Salient backend
    lons, lats, _ = get_grid(grid)
    coords = [(lons[0], lats[0]), (lons[-1], lats[0]), (lons[-1], lats[-1]), (lons[0], lats[-1])]
    loc = sk.Location(shapefile=sk.upload_shapefile(
        coords=coords,
        geoname="all_africa",  # the full African continent
        force=True))

    return loc


@salient_auth
@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 292, "lon": 396, "time": 300},
           auto_rechunk=True)
def salient_era5_raw(start_time, end_time, variable, grid="salient_africa0_25", verbose=False):
    """Fetches ground truth data from Salient's SDK.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - salient_africa0_25: 0.25 degree African grid from Salient
        verbose (bool): Whether to print verbose output.
    """
    if grid != "salient_africa0_25":
        # TODO: implement regridding for other grids
        raise NotImplementedError("Only the Salient African 0.25 grid is supported.")
    # Fetch the data from Salient

    loc = get_salient_loc(grid)
    var_name = {'tmp2m': 'temp', 'precip': 'precip'}[variable]

    # Fetch and load the data
    data = sk.data_timeseries(
        loc=loc,
        variable=var_name,
        field="vals",
        start=np.datetime64(start_time),
        end=np.datetime64(end_time),
        frequency="daily",
        verbose=verbose,
        force=True,
    )
    ds = xr.load_dataset(data)
    ds = ds.rename_vars(name_dict={'vals': variable})
    ds = ds.compute()
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid', 'agg', 'mask'],
           chunking={"lat": 292, "lon": 396, "time": 500},
           auto_rechunk=False)
def salient_era5(start_time, end_time, variable, grid="salient_africa0_25",
                 agg=14, mask=None, verbose=False):
    """Fetches ground truth data from Salient's SDK and applies aggregation and masking .

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - salient_africa0_25: 0.25 degree African grid from Salient
        agg (str): The aggregation period to use, in days
        mask (str): The mask to apply to the data. One of:
            - lsm: Land-sea mask
            - None: No mask
    """
    # Get raw salient data
    ds = salient_era5_raw(start_time, end_time, variable, grid=grid, verbose=verbose)

    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="time", agg_fn=agg_fn)

    if mask == "lsm":
        # Select variables and apply mask
        raise ValueError("Land-sea mask not implemented for Salient data.")
        # mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = None
    else:
        raise NotImplementedError("Only land-sea or None mask is implemented.")

    ds = apply_mask(ds, mask_ds, variable)
    return ds


@dask_remote
@salient_auth
@cacheable(data_type='array',
           timeseries='forecast_date_weekly',
           cache_args=['variable', 'grid', 'timescale'],
           chunking={"lat": 292, "lon": 396, "time": 500},
           auto_rechunk=False)
def salient_blend_raw(start_time, end_time, variable, grid="salient_africa0_25",
                      timescale="sub-seasonal", verbose=False):
    """Fetches ground truth data from Salient's SDK and applies aggregation and masking .

    Args:
        time (str): The date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - salient_africa0_25: 0.25 degree African grid from Salient
        timescale (str): The timescale of the forecast. One of:
            - sub-seasonal
            - seasonal
            - long-term
            - all
    """
    # Fetch the data from Salient
    loc = get_salient_loc(grid)
    var_name = {'tmp2m': 'temp', 'precip': 'precip'}[variable]

    # stride = {"sub-seasonal": "week", "seasonal": "month", "long-term": "year"}[timescale]
    # target_dates = generate_dates_in_between(start_time, end_time, "Wednesday", return_string=True)
    target_dates = get_dates(start_time, end_time, stride="day", return_string=True)
    # Fetch and load the data
    fcst = sk.forecast_timeseries(
        loc=loc,
        variable=var_name,
        field="vals",
        date=target_dates,
        timescale=timescale,
        model="blend",
        # reference_clim="30_yr",  # this is the climatology used by data_timeseries
        version="v8",
        verbose=verbose,
        force=False,  # use local data if already downloaded
        strict=False,  # There is missing data in 2020.  work around it.
    )

    filenames = fcst["file_name"].tolist()
    ds = xr.open_mfdataset(filenames,
                           concat_dim='forecast_date_weekly',
                           combine="nested",
                           parallel=True,
                           chunks={'lat': 292, 'lon': 316, 'lead_weekly': 5,
                                   'quantiles': 23, 'forecast_date_weekly': 3})

    # Drop down to the subset of dates that have unique forecasts
    ds = ds.drop_duplicates(dim='forecast_date_weekly', keep='first')

    # Rename and clean variables
    var_name = {'sub-seasonal': 'vals_weekly', 'seasonal': 'vals_monthly', 'long-term': 'vals_yearly'}[timescale]
    ds = ds.rename_vars(name_dict={var_name: variable})

    # ds = ds.compute()
    # os.remove(fcst)
    return ds


# @dask_remote
# @salient_auth
# @cacheable(data_type='array',
#            timeseries='time',
#            cache_args=['variable', 'grid', 'timescale'],
#            chunking={"lat": 292, "lon": 396, "time": 500},
#            auto_rechunk=False)
# def salient_blend_raw(start_time, end_time, variable, grid="salient_africa0_25",
#                       timescale="sub-seasonal", verbose=False):
#     """Fetches ground truth data from Salient's SDK and applies aggregation and masking .

#     Args:
#         start_time (str): The start date to fetch data for.
#         end_time (str): The end date to fetch.
#         variable (str): The weather variable to fetch.
#         grid (str): The grid resolution to fetch the data at. One of:
#             - salient_africa0_25: 0.25 degree African grid from Salient
#         timescale (str): The timescale of the forecast. One of:
#             - sub-seasonal
#             - seasonal
#             - long-term
#             - all
#     """
#     # Read and combine all the data into an array
#     target_dates = get_dates(start_time, end_time,
#                              stride="day", return_string=True)

#     # Get correct single function
#     datasets = []
#     for date in target_dates:
#         ds = dask.delayed(single_salient_blend_raw)(
#             date, variable, grid, timescale, recompute=True, force_overwrite=True,
#             verbose=verbose, filepath_only=True)
#         # ds = single_salient_blend_raw(
#         # #     date, variable, grid, timescale, recompute=True, force_overwrite=True,
#         #     verbose=verbose)
#         datasets.append(ds)
#     datasets = dask.compute(*datasets)
#     data = [d for d in datasets if d is not None]
#     if len(data) == 0:
#         return None

#     x = xr.open_mfdataset(data,
#                           concat_dim='forecast_date_weekly',
#                           combine="nested",
#                           parallel=True,
#                           chunks={'lat': 292, 'lon': 316, 'lead_weekly': 5,
#                                   'quantiles': 23, 'forecast_date_weekly': 3})

#     return x
