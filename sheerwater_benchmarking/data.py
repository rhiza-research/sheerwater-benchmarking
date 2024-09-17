import dask
import cdsapi
import os
import xarray as xr
import dateparser
from sheerwater_benchmarking.utils import cdsapi_secret, dask_remote, cacheable, get_grid, get_variable, regrid


@cacheable(data_type='array', cache_args=['grid'])
def land_sea_mask(grid="global1_5"):
    times = ['00:00']
    days = ['01']
    months = ['01']
    _, _, grid_size = get_grid(grid)

    # Make sure the temp folder exists
    os.makedirs('./temp', exist_ok=True)
    path = "./temp/lsm.nc"

    # Create a file path in the temp folder
    url, key = cdsapi_secret()
    c = cdsapi.Client(url=url, key=key)
    c.retrieve('reanalysis-era5-single-levels',
               {
                   'product_type': 'reanalysis',
                   'variable': "land_sea_mask",
                   'year': 2022,
                   'month': months,
                   'day': days,
                   'time': times,
                   'format': 'netcdf',
                   'grid': [str(grid_size), str(grid_size)],
               },
               path)

    # Some transformations
    ds = xr.open_dataset(path)
    ds = ds.rename({'latitude': 'lat', 'longitude': 'lon', 'lsm': 'mask'})
    ds = ds.sel(time=ds['time'].values[0])
    ds = ds.drop('time')

    os.remove(path)
    return ds


@cacheable(data_type='array', cache_args=['year', 'variable', 'grid'])
def single_era5(year, variable, grid="global1_5"):
    """Fetches a single variable from the ERA5 dataset."""

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

    # Materialize the dataset into memory to ensure caching works
    ds = ds.compute()


@dask_remote
def era5_cds(start_time, end_time, variable, grid="global1_5"):
    """Read the raw ERA5 data from the CDS API."""
    # Read and combine all the data into an array
    first_year = dateparser.parse(start_time).year
    last_year = dateparser.parse(end_time).year
    years = [str(y) for y in range(first_year, last_year+1)]

    datasets = []
    for year in years:
        ds = dask.delayed(single_era5)(year, variable, grid, filepath_only=True)
        datasets.append(ds)

    ds = dask.compute(*datasets)
    x = xr.open_mfdataset(ds, engine='zarr')
    return x


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid'],
           timeseries='time',
           cache_disable_if={'grid': 'global0_25'})
def era5(start_time, end_time, variable, grid="global0_25"):
    """ERA5 function that returns data from Google ARCO"""
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
@cacheable(data_type='array', cache_args=['variable', 'grid'], timeseries='time')
def era5_daily(start_time, end_time, variable, grid='global1_5'):
    era = era5(start_time, end_time, variable, grid)
    era = era.resample(time='1D').mean()

    return era
