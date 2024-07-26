
import dask
import cdsapi
import argparse
import json
import pathlib
import sys
import time
import os
import subprocess
import xarray as xr
import dateparser
from datetime import datetime
from sheerwater_benchmarking.utils.secrets import cdsapi_secret
from sheerwater_benchmarking.utils.remote import dask_remote
from sheerwater_benchmarking.utils.caching import cacheable

@cacheable(data_type='array', immutable_args=['grid'])
def land_sea_mask(grid=1.5):

    times = ['00:00']
    days = ['01']
    months = ['01']

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
                    'grid':[str(grid), str(grid)],
                },
            path)

    # Some transformations
    ds = xr.open_dataset(path)
    ds = ds.rename({'latitude': 'lat','longitude': 'lon', 'lsm':'mask'})
    ds = ds.sel(time=ds['time'].values[0])
    ds = ds.drop('time')

    os.remove(path)

    return ds

@cacheable(data_type='array', immutable_args=['year', 'variable', 'grid'])
def single_era5(year, variable, grid=1.5):
    weather_variables = {
        # Static variables (2):
        "z": "geopotential", # geopotential at surface
        "lsm": "land_sea_mask",
        # Surface variables (6):
        "tmp2m": "2m_temperature",
        "precip": "total_precipitation",
        "vwind10m": "10m_v_component_of_wind",
        "uwind10m": "10m_u_component_of_wind",
        "msl": "mean_sea_level_pressure",
        "tisr": "toa_incident_solar_radiation",
        # Atmospheric variables (6):
        "tmp": "temperature",
        "uwind": "u_component_of_wind",
        "vwind": "v_component_of_wind",
        "hgt": "geopotential",
        "q": "specific_humidity",
        "w": "vertical_velocity",
    }

    # Enable variable shortcuts
    if variable in weather_variables:
        variable = weather_variables[variable]


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

    url, key = cdsapi_secret()
    c = cdsapi.Client(url=url, key=key)

    os.makedirs('./temp', exist_ok=True)
    path = f"./temp/{variable}.{year}.{grid}.nc"

    print(f"Fetching data {variable} data for year {year}, months {months}, days {days}, and times {times} at grid {grid}.")

    c.retrieve('reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': variable,
                    'year': year,
                    'month': months,
                    'day': days,
                    'time': times,
                    'format': 'netcdf',
                    'grid':[str(grid), str(grid)],
                },
                path)

    # Read the data and return individual datasets
    return xr.open_dataset(path)

@dask_remote
@cacheable(data_type='array', immutable_args=['variable', 'grid'], cache=False, timeseries=True)
def era5(start_time, end_time, variable, grid=1.5):
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
@cacheable(data_type='array', immutable_args=['variable', 'grid'], timeseries=True)
def era5_daily(start_time, end_time, variable, grid=1.5):
    era = era5(start_time, end_time, variable, grid)
    era = era.resample(time='1D').mean()

    return era
