"""Fetches ERA5 data from the CDS API."""
import dask
import cdsapi
import os
import xarray as xr
import dateparser
import numpy as np
from datetime import datetime, timedelta

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           cdsapi_secret,
                                           get_grid, clip_region, get_variable,
                                           apply_mask, roll_and_agg, lon_base_change,
                                           regrid, get_anomalies)
from sheerwater_benchmarking.masks import land_sea_mask


@cacheable(data_type='array', cache_args=['year', 'variable', 'grid'])
def single_era5(year, variable, grid="global1_5"):
    """Fetches a single year of hourly ERA5 data.

    Args:
        year (str): The year to fetch data for.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree
    """
    variable = get_variable(variable, 'era5')

    times = ['00:00', '01:00', '02:00',
             '03:00', '04:00', '05:00',
             '06:00', '07:00', '08:00',
             '09:00', '10:00', '11:00',
             '12:00', '13:00', '14:00',
             '15:00', '16:00', '17:00',
             '18:00', '19:00', '20:00',
             '21:00', '22:00', '23:00']
    days = [str(i) for i in range(1, 32)]
    months = ["01", "02", "03", "04", "05", "06",
              "07", "08", "09", "10", "11", "12"]

    _, _, grid_size = get_grid(grid)

    url, key = cdsapi_secret()
    c = cdsapi.Client(url=url, key=key)

    os.makedirs('./temp', exist_ok=True)
    path = f"./temp/{variable}.{year}.{grid}.nc"

    print(f"Fetching data {variable} data for year {year}, months {
          months}, days {days}, and times {times} at grid {grid}.")

    c.retrieve('reanalysis-era5-single-levels',
               {
                   'product_type': 'reanalysis',
                   'variable': variable,
                   'year': year,
                   'month': months,
                   'day': days,
                   'time': times,
                   'format': 'netcdf',
                   'grid': [str(grid_size), str(grid_size)],
               },
               path)

    # Read the data and return individual datasets
    ds = xr.open_dataset(path)
    ds = ds.compute()
    os.remove(path)
    return ds


@cacheable(data_type='array',
           cache_args=['year', 'variable', 'grid'],
           chunking={"lat": 121, "lon": 240, "time": 1000})
def single_era5_cleaned(year, variable, grid="global1_5"):
    """Fetches a single year of ERA5 data and cleans it up.

    Handles the two different formats of ERA5 data.
    Has the same interface as single_era5.
    """
    ds = single_era5(year, variable, grid)

    # Handle second data format
    if "valid_time" in ds.coords:
        ds = ds.rename({'valid_time': 'time'})
    if "expver" in ds.coords:
        ds = ds.drop_vars("expver")
    if "number" in ds.coords:
        ds = ds.drop_vars("number")

    return ds


@dask_remote
def era5_cds(start_time, end_time, variable, grid="global1_5"):
    """Aggregate yearly ERA5 data into a single dataset."""
    # Read and combine all the data into an array
    first_year = dateparser.parse(start_time).year
    last_year = dateparser.parse(end_time).year
    years = [str(y) for y in range(first_year, last_year+1)]

    datasets = []
    for year in years:
        ds = dask.delayed(single_era5_cleaned)(
            year, variable, grid, filepath_only=True)
        datasets.append(ds)

    ds = dask.compute(*datasets)
    ds = xr.open_mfdataset(ds,
                           engine='zarr',
                           parallel=True,
                           chunks={'lat': 121, 'lon': 240, 'time': 1000}
                           )
    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache=False)
def era5_raw(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ERA5 function that returns data from Google ARCO."""
    if grid != 'global0_25':
        raise NotImplementedError("Only ERA5 native 0.25 degree grid is implemented.")

    # Pull the google dataset
    ds = xr.open_zarr('gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
                      chunks={'time': 50, 'latitude': 721, 'longitude': 1440})

    # Select the right variable
    var = get_variable(variable, 'era5')
    ds = ds[var].to_dataset()

    # Convert local dataset naming and units
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})
    ds = ds.rename_vars(name_dict={var: variable})

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid'],
           chunking={"lat": 721, "lon": 1440, "time": 30},
           auto_rechunk=False)
def era5_daily(start_time, end_time, variable, grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
    """
    # Read and combine all the data into an array
    ds = era5_raw(start_time, end_time, variable, grid='global0_25')

    # Convert to base180 longitude
    ds = lon_base_change(ds, to_base="base180")

    if variable == 'tmp2m':
        ds[variable] = ds[variable] - 273.15
        ds[variable].units = 'C'
        ds = ds.resample(time='D').mean(dim='time')
    elif variable == 'precip':
        ds[variable] = ds[variable] * 1000.0
        ds[variable].units = 'mm'
        ds = ds.resample(time='D').sum(dim='time')
        ds = np.maximum(ds, 0)

    if grid != 'global0_25':
        # Regrid the data to the desired grid, on base360 longitudes
        ds = regrid(ds, grid, base="base180")
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg', 'anom', 'clim_params', 'grid'],
           chunking={"lat": 721, "lon": 1441, "time": 30})
def era5_rolled(start_time, end_time, variable, agg=14,
                anom=False, clim_params=None,
                grid="global1_5"):
    """Aggregates the hourly ERA5 data into daily data and rolls.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
        agg (str): The aggregation period to use, in days
        anom (bool): Whether to return the climatological anomaly
        clim_params (dict): Parameters for the climatology anomaly calculation, 
            passed directly to climatology function. If anom is False,
            these parameters are ignored.
    """
    # Read and combine all the data into an array
    ds = era5_daily(start_time, end_time, variable, grid=grid)

    if anom:
        # Import here to avoid circular dependency
        from sheerwater_benchmarking.climatology import climatology_raw
        # Get the climatology on the same grid
        clim_params = {} if clim_params is None else clim_params
        try:
            clim = climatology_raw(variable, **clim_params, grid=grid)
        except:
            import pdb
            pdb.set_trace()
        ds = get_anomalies(ds, clim, var=variable)

    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="time", agg_fn=agg_fn)

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'agg', 'anom', 'clim_params', 'grid', 'mask', 'region'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           auto_rechunk=False)
def era5_agg(start_time, end_time, variable, agg=14, anom=False, clim_params=None,
             grid="global1_5", mask="lsm", region='global'):
    """Fetches ground truth data from ERA5 and applies aggregation and masking.

    Specialized function for the ABC model.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        agg (str): The aggregation period to use, in days
        mask (str): The mask to apply to the data. One of:
            - lsm: Land-sea mask
            - None: No mask
    """
    # Get ERA5 on the corresponding global grid
    ds = era5_rolled(start_time, end_time, variable, grid=grid, agg=agg,
                     anom=anom, clim_params=clim_params)

    # Apply mask
    if mask != 'lsm' and mask is not None:
        raise NotImplementedError("Only land-sea or no mask is implemented.")
    mask_ds = land_sea_mask(grid=grid).compute() if mask == "lsm" else None
    ds = apply_mask(ds, mask_ds, variable)

    # Clip to region
    ds = clip_region(ds, region)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'grid', 'mask', 'region'])
def era5(start_time, end_time, variable, lead, grid='global0_25', mask='lsm', region='africa'):
    """Standard format task data for ERA5 Reanalysis.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        lead (str): The lead time of the forecast.
        grid (str): The grid resolution to fetch the data at.
        mask (str): The mask to apply to the data.
        region (str): The region to clip the data to.
    """
    leads_param = {
        "week1": (7, 0),
        "week2": (7, 7),
        "week3": (7, 14),
        "week4": (7, 21),
        "week5": (7, 28),
        "week6": (7, 36),
        "weeks12": (14, 0),
        "weeks23": (14, 7),
        "weeks34": (14, 14),
        "weeks45": (14, 21),
        "weeks56": (14, 28),
    }

    agg, time_shift = leads_param.get(lead)

    # Get daily data
    new_start = datetime.strftime(dateparser.parse(start_time)+timedelta(days=time_shift), "%Y-%m-%d")
    new_end = datetime.strftime(dateparser.parse(end_time)+timedelta(days=time_shift), "%Y-%m-%d")
    ds = era5_rolled(new_start, new_end, variable, agg=agg, anom=False, grid=grid)
    ds = ds.assign_coords(time=ds['time']-np.timedelta64(time_shift, 'D'))

    # Apply mask
    if mask != 'lsm' and mask is not None:
        raise NotImplementedError("Only land-sea or no mask is implemented.")
    mask_ds = land_sea_mask(grid=grid).compute() if mask == "lsm" else None
    ds = apply_mask(ds, mask_ds, variable)

    # Clip to region
    ds = clip_region(ds, region)
    return ds
