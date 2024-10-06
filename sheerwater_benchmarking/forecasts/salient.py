"""Pulls Salient Predictions S2S forecasts from the Salient API."""
import dask
import pandas as pd
import dateparser


import numpy as np
import xarray as xr
import salientsdk as sk

from sheerwater_benchmarking.masks import land_sea_mask
from sheerwater_benchmarking.utils import (cacheable, dask_remote,
                                           salient_auth,
                                           get_grid, get_dates,
                                           roll_and_agg, apply_mask,
                                           get_variable,
                                           regrid)


@salient_auth
def get_salient_loc(grid):
    """Get and upload the location object for the Salient API."""
    if grid != "salient_africa0_25":
        raise NotImplementedError("Only the Salient African 0.25 grid is supported.")

    # Upload location shapefile to Salient backend
    lons, lats, _, _ = get_grid(grid)
    coords = [(lons[0], lats[0]), (lons[-1], lats[0]), (lons[-1], lats[-1]), (lons[0], lats[-1])]
    loc = sk.Location(shapefile=sk.upload_shapefile(
        coords=coords,
        geoname="all_africa",  # the full African continent
        force=False))
    return loc


@salient_auth
@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 292, "lon": 396, "time": 300},
           auto_rechunk=False)
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
        verbose (bool): Whether to print verbose output.
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


@salient_auth
@cacheable(data_type='array',
           cache_args=['year', 'variable', 'grid', 'timescale'],
           chunking={"lat": 292, "lon": 396, "forecast_date": 3, 'lead': 5, 'quantiles': 23},
           auto_rechunk=False)
def year_salient_blend_raw(year, variable, grid="salient_africa0_25",
                           timescale="sub-seasonal", verbose=True):
    """Fetch a year of Salient data and cache.

    Args:
        year (int, str): The year to fetch data for.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - salient_africa0_25: 0.25 degree African grid from Salient
        timescale (str): The timescale of the forecast. One of:
            - sub-seasonal
            - seasonal
            - long-range
        verbose (bool): Whether to print verbose output.
    """
    # Fetch data from Salient API
    start_time = f"{year}-01-01"
    end_time = f"{year}-12-31"
    target_dates = get_dates(start_time, end_time, stride="day", return_string=True)

    loc = get_salient_loc(grid)
    var_name = {'tmp2m': 'temp', 'precip': 'precip'}[variable]

    fcst = sk.forecast_timeseries(
        loc=loc,
        variable=var_name,
        field="vals",
        date=target_dates,
        timescale=timescale,
        model="blend",
        verbose=verbose,
        force=False,  # use local data if already downloaded
        strict=False,
    )

    # Get locally downloaded file and load into xarray
    filenames = fcst["file_name"].tolist()
    filenames = [f for f in filenames if not pd.isnull(f)]

    # If no non-null files were downloaded, return None
    if len(filenames) == 0:
        return None

    # Get variable renaming for Salient timescales
    fcst_date, fcst_lead, \
        fcst_vals, _ = {"sub-seasonal": ("forecast_date_weekly", "lead_weekly", "vals_weekly", "week"),
                        "seasonal": ("forecast_date_monthly", "lead_monthly", "vals_monthly", "month"),
                        "long-range": ("forecast_date_yearly", "lead_yearly", "vals_yearly", "year")
                        }[timescale]

    # Open locally downloaded netcdf files
    ds = xr.open_mfdataset(filenames,
                           concat_dim=fcst_date,
                           engine='netcdf4',
                           combine="nested",
                           parallel=True,
                           chunks={'lat': 292, 'lon': 316, fcst_lead: 5,
                                   'quantiles': 23, fcst_date: 3})

    # Remove duplicated downloads from API
    ds = ds.sel(forecast_date_weekly=~ds['forecast_date_weekly'].to_index().duplicated())

    # Rename and clean variables
    ds = ds.rename_vars(name_dict={fcst_vals: variable})
    ds = ds.rename({fcst_date: "forecast_date", fcst_lead: "lead"})

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'grid', 'timescale'],
           cache=False)
def salient_blend_raw_sdk(start_time, end_time, variable, grid="salient_africa0_25",
                          timescale="sub-seasonal", verbose=True):
    """Fetches ground truth data from Salient's SDK and applies aggregation and masking .

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - salient_africa0_25: 0.25 degree African grid from Salient
        timescale (str): The timescale of the forecast. One of:
            - sub-seasonal
            - seasonal
            - long-range
            - all
        verbose (bool): Whether to print verbose output.
    """
    start_year = dateparser.parse(start_time).year
    end_year = dateparser.parse(end_time).year

    datasets = []
    for year in range(start_year, end_year + 1):
        # Can't use dask delayed here because it doesn't work with the Salient API
        ds = year_salient_blend_raw(year, variable, grid, timescale,
                                    verbose=verbose, filepath_only=True)
        datasets.append(ds)
    datasets = dask.compute(*datasets)
    data = [d for d in datasets if d is not None]
    if len(data) == 0:
        return None

    x = xr.open_mfdataset(data,
                          concat_dim='forecast_date',
                          combine="nested",
                          engine='zarr',
                          parallel=True,
                          chunks={'lat': 292, 'lon': 316, 'lead': 5,
                                  'quantiles': 23, 'forecast_date': 3})

    return x


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'grid', 'timescale'],
           cache=False)
def salient_blend_raw(start_time, end_time, variable, grid="salient_africa0_25",  # noqa: F841
                      timescale="sub-seasonal", verbose=True):
    """Salient function that returns data from GCP mirror.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - salient_africa0_25: 0.25 degree African grid from Salient
        timescale (str): The timescale of the forecast. One of:
            - sub-seasonal
            - seasonal
            - long-range
        verbose (bool): Whether to print verbose output

    """
    if grid != "salient_africa0_25":
        raise NotImplementedError("Only Salient common 0.25 degree grid is implemented.")

    # Pull the google dataset
    var = get_variable(variable, 'salient')
    filename = f'gs://sheerwater-datalake/salient-data/v9/africa/{var}_{timescale}/blend'
    ds = xr.open_zarr(filename,
                      chunks={'forecast_date': 3, 'lat': 300, 'lon': 316,
                              'lead': 10, 'quantile': 23, 'model': 5})
    ds = ds['vals'].to_dataset()
    ds = ds.rename(vals=variable)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='forecast_date',
           cache_args=['variable', 'grid', 'timescale', 'mask'],
           chunking={"lat": 300, "lon": 400, "forecast_date": 300, 'lead': 1, 'quantiles': 1},
           auto_rechunk=False)
def salient_blend_proc(start_time, end_time, variable, grid="africa0_25",
                       timescale="sub-seasonal", mask='lsm'):
    """Processed Salient forecast files."""
    if grid == 'salient_africa0_25' and mask is not None:
        raise NotImplementedError('Masking not implemented for Salient native grid.')

    if 'africa' not in grid:
        raise NotImplementedError('Only Africa grids are implemented for Salient.')

    ds = salient_blend_raw(start_time, end_time, variable, 'salient_africa0_25', timescale)
    ds = ds.dropna('forecast_date', how='all')

    if grid != "salient_africa0_25":
        ds = regrid(ds, grid)

    if mask == "lsm":
        # Select variables and apply mask
        mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = None
    else:
        raise NotImplementedError("Only land-sea or None mask is implemented.")

    ds = apply_mask(ds, mask_ds, variable)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask'])
def salient_blend(start_time, end_time, variable, lead, prob_type='deterministic',
                  grid='africa0_25', mask='lsm'):
    """Standard format forecast data for Salient."""
    lead_params = {
        "week1": ("sub-seasonal", 1),
        "week2": ("sub-seasonal", 2),
        "week3": ("sub-seasonal", 3),
        "week4": ("sub-seasonal", 4),
        "week5": ("sub-seasonal", 5),
        "month1": ("seasonal", 1),
        "month2": ("seasonal", 2),
        "month3": ("seasonal", 3),
        "quarter1": ("long-range", 1),
        "quarter2": ("long-range", 2),
        "quarter3": ("long-range", 3),
        "quarter4": ("long-range", 4),
    }
    timescale, lead_id = lead_params.get(lead, (None, None))
    if timescale is None:
        raise NotImplementedError(f"Lead {lead} not implemented for Salient.")

    ds = salient_blend_proc(start_time, end_time, variable, grid=grid,
                            timescale=timescale, mask=mask)
    ds = ds.sel(lead=lead_id)
    if prob_type == 'd':
        # Get the median forecast
        ds = ds.sel(quantiles=0.5)
        ds['quantiles'] = -1
    ds = ds.rename({'quantiles': 'member'})
    ds = ds.rename({'forecast_date': 'time'})

    return ds
