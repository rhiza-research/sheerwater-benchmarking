"""Fetches ERA5 data from the CDS API."""
import dask
import cdsapi
import os
import xarray as xr
import dateparser

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           cdsapi_secret,
                                           get_grid, get_variable)


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
