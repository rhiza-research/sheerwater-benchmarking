"""Fetches ERA5 data from the CDS API."""
import dask
import cdsapi
import os
import xarray as xr
import dateparser

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           cdsapi_secret,
                                           get_grid, get_variable,
                                           apply_mask, roll_and_agg, regrid)
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
    x = xr.open_mfdataset(ds, engine='zarr')
    return x


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache_disable_if={'grid': 'global0_25'})
def era5(start_time, end_time, variable, grid="global0_25"):  # noqa ARG001
    """ERA5 function that returns data from Google ARCO."""
    # Pull the google dataset
    ds = xr.open_zarr('gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
                      chunks={'time': 50, 'latitude': 721, 'longitude': 1440})

    # Select the right variable
    variable = get_variable(variable, 'era5')
    ds = ds[variable].to_dataset()

    # Rename variable into our variable space
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    # Regrid if necessary
    if grid != 'global0_25':
        print(f"Regridding Google ARCO ERA5 from global0_25 to {grid}")
        ds = regrid(ds, grid)

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid', 'agg'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           auto_rechunk=False)
def era5_rolled(start_time, end_time, variable, grid="global1_5", agg=14):
    """Aggregates the hourly ERA5 data into daily data and rolls.

    Args:
        start_time (str): The start date to fetch data for.
        end_time (str): The end date to fetch.
        variable (str): The weather variable to fetch.
        grid (str): The grid resolution to fetch the data at. One of:
            - global1_5: 1.5 degree global grid
        agg (str): The aggregation period to use, in days
    """
    # Read and combine all the data into an array
    ds = era5(start_time, end_time, variable, grid=grid)
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon'})

    # Convert hourly data to daily data and then aggregate
    if variable == 'tmp2m':
        ds = ds.rename_vars(name_dict={'t2m': 'tmp2m'})
        # Convert from Kelvin to Celsius
        if ds[variable].units == 'K':
            ds[variable] = ds[variable] - 273.15
        ds = ds.resample(time='D').mean(dim='time')
    elif variable == 'precip':
        ds = ds.rename_vars(name_dict={'tp': 'precip'})
        # Convert from m to mm
        if ds[variable].units == 'm':
            ds[variable] = ds[variable] * 1000.0
        ds = ds.resample(time='D').sum(dim='time')

    agg_fn = "sum" if variable == "precip" else "mean"
    ds = roll_and_agg(ds, agg=agg, agg_col="time", agg_fn=agg_fn)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'grid', 'agg', 'mask'],
           chunking={"lat": 121, "lon": 240, "time": 1000},
           auto_rechunk=False)
def era5_agg(start_time, end_time, variable, grid="global1_5", agg=14, mask="lsm"):
    """Fetches ground truth data from ERA5 and applies aggregation and masking .

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
    ds = era5_rolled(start_time, end_time, variable, grid=grid, agg=agg)

    if mask == "lsm":
        # Select variables and apply mask
        mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = None
    else:
        raise NotImplementedError("Only land-sea or None mask is implemented.")

    ds = apply_mask(ds, mask_ds, variable, val=0.0, rename_dict={"mask": variable})

    return ds
