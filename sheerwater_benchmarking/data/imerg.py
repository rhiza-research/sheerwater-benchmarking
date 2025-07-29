"""Imerg data product."""
import os
import sys
import pandas as pd
import numpy as np
import re
import xarray as xr
import gcsfs
from dateutil import parser
from functools import partial
import requests
import ssl
from urllib3 import poolmanager
from requests.auth import HTTPBasicAuth
from scipy.interpolate import griddata
# import xesmf as xe
# import ESMF

from sheerwater_benchmarking.utils import cacheable, dask_remote, regrid, roll_and_agg, apply_mask, clip_region, nasa_earthdata_secret, get_grid_ds, get_grid
from sheerwater_benchmarking.tasks import spw_rainy_onset, spw_precip_preprocess

import xarray as xr
import numpy as np

import pandas as pd
import numpy as np


import numpy as np
import pandas as pd
import xarray as xr


def snap_and_keep_first_all(ds, lat_name='lat', lon_name='lon', resolution=0.05):
    """
    Snap lat/lon to the nearest grid and keep the first occurrence per cell,
    preserving all variables.
    """
    # Convert all variables to a single DataFrame (flatten)
    df = ds.to_dataframe().reset_index()

    # Snap lat/lon
    df[lat_name] = np.round(df[lat_name] / resolution) * resolution
    df[lon_name] = np.round(df[lon_name] / resolution) * resolution

    # Drop duplicates, keeping first occurrence
    df_first = df.drop_duplicates(subset=[lat_name, lon_name], keep='first')

    # Convert back to xarray dataset
    ds_first = df_first.set_index([lat_name, lon_name]).to_xarray()

    return ds_first


@dask_remote
@cacheable(data_type='array',
           cache_args=['time'],
           chunking={'time': 500000, 'nray': 50})
def single_gpm_2bcombined(time, verbose=True):
    """Fetches GPM 2B-CMB data from the NASA Earthdata API.

    See all options here: https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L2/GPM_2BCMB.07/2023/001/2B.GPM.DPRGMI.CORRA2022.20230101-S000208-E013440.050238.V07A.HDF5.dmr.html 

    Args:
        time (str): The date to fetch data for (by day).
    """
    username, password = nasa_earthdata_secret()
    # 📂 Remote NASA GES DISC directory
    url_base = 'https://gpm1.gesdisc.eosdis.nasa.gov/opendap/GPM_L2/GPM_2BCMB.07'
    date = parser.parse(time)
    year = date.year
    day_of_year = date.strftime("%j")
    url_base = f"{url_base}/{year}/{day_of_year}"
    # Get directory listing (HTML)
    r = requests.get(url_base)
    html = r.text

    # Find all HDF5 files in the directory by extracting the HDF5 filename from the href string
    hdf5_links = re.findall(r'href="([^"]*\.HDF5)(?!\.)"', html)
    hdf5_files = []
    for link in hdf5_links:
        # Extract the HDF5 filename by splitting on '/' and taking the last part
        hdf5_file = link.split('/')[-1]
        hdf5_files.append(hdf5_file)

    # Start session
    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    os.makedirs('./temp', exist_ok=True)
    datasets = []
    for i, hdf5_file in enumerate(hdf5_files):
        url = f"{url_base}/{hdf5_file}.dap.nc4"
        url = (f"{url}?dap4.ce=/"
               "KuGMI_Longitude;/"
               "KuGMI_Latitude;/"
               "KuGMI_surfaceAirTemperature;/"
               "KuGMI_nearSurfPrecipTotRate;/"
               "KuGMI_estimSurfPrecipTotRate;/"
               "KuGMI_pia;/"
               "KuGMI_estimSurfPrecipTotRateSigma;/"
               "KuGMI_ScanTime_Year;/"
               "KuGMI_ScanTime_DayOfYear;/"
               "KuGMI_ScanTime_SecondOfDay"
               )
        # 📁 Local download destination
        file = f"./temp/{hdf5_file}.nc"
        r = session.get(url, stream=True)
        if r.status_code == 200 and r.headers["Content-Type"] == "application/x-netcdf;ver=4":
            if verbose:
                print(f"Downloading: {time}, Scan {i+1} / {len(hdf5_files)}")
            with open(file, "wb") as f:
                f.write(r.content)
            if verbose:
                print(f"-done (downloaded {sys.getsizeof(r.content) / 1024**2:.2f} MB).\n")
        elif r.status_code == 404:
            print(f"Data for {time} is not available for GPM 2B-CMB.\n")
            return None
        else:
            raise ValueError(f"Failed to download data for {time} for GPM 2B-CMB.")

        try:
            # Read the data and return individual datasets
            ds = xr.open_dataset(file, engine="netcdf4")
        except OSError:
            print(f"Failed to load data for: {time}.")
            return None

        # Try to convert all at once using pandas to_datetime, without using timedelta
        year = int(ds['KuGMI_ScanTime_Year'].values[0])  # they are all the same year and doy
        doy_delta = ds['KuGMI_ScanTime_DayOfYear'].values[0] - pd.to_timedelta(1, unit='D')
        second_of_day = ds['KuGMI_ScanTime_SecondOfDay'].values

        # Convert base date: Jan 1 of each year
        base = pd.to_datetime(f"{year}-01-01")

        # Add day-of-year offset + second-of-day offset
        scan_time = base + doy_delta + pd.to_timedelta(second_of_day, unit='s')
        ds = ds.assign_coords(time=("nscan", scan_time))

        rename_dict = {
            "KuGMI_Longitude": "lon",
            "KuGMI_Latitude": "lat",
            "KuGMI_nearSurfPrecipTotRate": "precip",
            "KuGMI_surfaceAirTemperature": "tmp2m",
            "KuGMI_pia": "pia",
            "KuGMI_estimSurfPrecipTotRate": "precip_est",
            "KuGMI_estimSurfPrecipTotRateSigma": "precip_est_sigma",
        }
        ds = ds.rename(rename_dict)
        ds = ds.drop_vars(['KuGMI_ScanTime_Year', 'KuGMI_ScanTime_DayOfYear', 'KuGMI_ScanTime_SecondOfDay'])

        # Swap time and nscan coords
        ds = ds.swap_dims({'nscan': 'time'})
        ds = ds.compute()
        datasets.append(ds)
        os.remove(file)

    ds_full = xr.concat(datasets, dim="time")
    return ds_full


@dask_remote
@cacheable(data_type='array',
           cache_args=['year'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def imerg_raw(year):
    """Concatted imerge netcdf files by year."""
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    gsf = [fs.open(x) for x in fs.glob(f'gs://sheerwater-datalake/imerg/{year}*.nc')]

    ds = xr.open_mfdataset(gsf, engine='h5netcdf')

    return ds


@dask_remote
@cacheable(data_type='array',
           cache_args=['grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def imerg_gridded(start_time, end_time, grid):
    """Regridded version of whole imerg dataset."""
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

    datasets = []
    for year in years:
        ds = imerg_raw(year, filepath_only=True)
        datasets.append(ds)

    ds = xr.open_mfdataset(datasets,
                           engine='zarr',
                           parallel=True,
                           chunks={'lat': 300, 'lon': 300, 'time': 365})

    ds = ds['precipitation'].to_dataset()
    ds = ds.rename({'precipitation': 'precip'})

    # Regrid if not on the native grid
    if grid != 'imerg':
        ds = regrid(ds, grid, base='base180', method='conservative')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['grid', 'agg_days'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def imerg_rolled(start_time, end_time, agg_days, grid):
    """Imerg rolled and aggregated."""
    ds = imerg_gridded(start_time, end_time, grid)
    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean')
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries=['time'],
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region'],
           cache=False)
def imerg(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global'):
    """Final imerg product."""
    if variable not in ['precip', 'rainy_onset', 'rainy_onset_no_drought']:
        raise NotImplementedError("Only precip and derived variables provided by IMERG.")

    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = variable == 'rainy_onset_no_drought'
        fn = partial(imerg_rolled, start_time, end_time, grid=grid)
        roll_days = [8, 11] if not drought_condition else [8, 11, 11]
        shift_days = [0, 0] if not drought_condition else [0, 0, 11]
        data = spw_precip_preprocess(fn, agg_days=roll_days, shift_days=shift_days,
                                     mask=mask, region=region, grid=grid)
        ds = spw_rainy_onset(data,
                             onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                             time_dim='time', prob_type='deterministic',
                             drought_condition=drought_condition,
                             mask=mask, region=region, grid=grid)
        # Rainy onset is sparse, so we need to set the sparse attribute
        ds = ds.assign_attrs(sparse=True)
    else:
        ds = imerg_rolled(start_time, end_time, agg_days=agg_days, grid=grid)
        ds = apply_mask(ds, mask, grid=grid)
        ds = clip_region(ds, region=region)

    return ds
