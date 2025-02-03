from huggingface_hub import hf_hub_download
from huggingface_hub.utils import EntryNotFoundError
from sheerwater_benchmarking.utils.secrets import huggingface_read_token
from sheerwater_benchmarking.utils import cacheable, dask_remote
import py7zr
import glob
import pandas as pd
import dask
import xarray as xr
import os
import shutil
import numpy as np

from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           regrid, get_variable,
                                           target_date_to_forecast_date,
                                           shift_forecast_date_to_target_date, lead_to_agg_days, roll_and_agg)


@dask_remote
@cacheable(data_type='array', cache_args=['date'])
def fuxi_single_forecast(date):
    token = huggingface_read_token()

    date = str(date)
    date = date.replace('-', '')
    fname = date + '.7z'
    target_path = f"./{fname.replace('.7z', '')}"

    # Download the file
    try:
        path = hf_hub_download(repo_id="FudanFuXi/FuXi-S2S", filename=fname, repo_type="dataset", token=token)
    except EntryNotFoundError:
        print("Skipping invalid forecast date")
        return None

    # unzip the file
    with py7zr.SevenZipFile(path, mode='r') as zip:
        print(f"Extracting {path} to {target_path}")
        zip.extractall(path=target_path)

    files = glob.glob(f"{target_path}/**/*.nc", recursive=True)

    def pre(ds):
        ff = ds.encoding["source"]
        member = ff.split('/')[-2]
        ds = ds.assign_coords(member=member)
        ds = ds.expand_dims(dim='member')
        return ds

    # Transform and drop dataa
    print("Opening dataset")
    ds = xr.open_mfdataset(files, engine='netcdf4', preprocess=pre)
    return ds

    ds = ds['__xarray_dataarray_variable__'].to_dataset(dim='channel')

    variables = [
        'tp',
        't2m',
        'd2m',
        'sst',
        'ttr',
        '10u',
        '10v',
        'msl',
        'tcwv'
    ]

    print(ds)

    ds = ds[variables]
    ds = ds.compute()

    # Delete the files
    link = os.path.realpath(path)
    os.remove(path)
    os.remove(link)
    shutil.rmtree(target_path)

    return ds

@dask_remote
@cacheable(data_type='array', cache_args=[], timeseries='time',
           chunking={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})
def fuxi_raw(start_time, end_time, delayed=False):
    dates = pd.date_range(start_time, end_time)

    datasets = []
    for date in dates:
        date = date.date()
        if delayed:
            ds = dask.delayed(fuxi_single_forecast)(date, filepath_only=True)
        else:
            ds = fuxi_single_forecast(date, filepath_only=True)

        datasets.append(ds)

    if delayed:
        datasets = dask.compute(*datasets)

    print(datasets)

    data = [d for d in datasets if d is not None]
    if len(data) == 0:
        print("No data found.")
        return None

    ds = xr.open_mfdataset(data,
                          engine='zarr',
                          combine="by_coords",
                          parallel=True,
                          chunks={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})

    ds = ds.rename({'tp': 'precip', 't2m': 'tmp2m'})

    return ds

#@dask_remote
#@cacheable(data_type='array',
#           timeseries='time',
#           cache_args=['grid'],
#           cache_disable_if={'grid':'global1_5'},
#           chunking={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})
#def fuxi_gridded(start_time, end_time, grid='global1_5'):
#
#    ds = fuxi_raw(start_time, end_time)
#
#    if grid != 'global1_5':
#        ds = regrid(ds, grid, base='base180', method='conservative',
#                    output_chunks={"lat": 721, "lon": 1440})
#
#    return ds

@dask_remote
@cacheable(data_type='array',
           cache_args=['agg_days', 'prob_type'],
           chunking={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})
def fuxi_rolled(start_time, end_time, agg_days=7, prob_type='probabilistic'):
    ds = fuxi_raw(start_time, end_time)

    # If deterministic average across the members
    if prob_type == 'deterministic':
        ds = ds.mean(dim='member')
        ds = ds.assign_attrs(prob_type="deterministic")
    else:
        ds = ds.assign_attrs(prob_type="ensemble")

    ds = roll_and_agg(ds, agg=agg_days, agg_col="lead_time", agg_fn="mean")

    return ds

@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def fuxi(start_time, end_time, variable, lead, prob_type='deterministic',
         grid='global1_5', mask='lsm', region="global"):

    if grid != 'global1_5':
        raise NotImplementedError("Only 1.5 grid implemented for FuXi.")

    """Standard format forecast data for daily forecasts."""
    lead_params = {
        "week1": ('weekly', 0),
        "week2": ('weekly', 7),
        "week3": ('weekly', 14),
        "week4": ('weekly', 21),
        "week5": ('weekly', 28),
        "week6": ('weekly', 35),
        "weeks12": ('biweekly', 0),
        "weeks23": ('biweekly', 7),
        "weeks34": ('biweekly', 14),
        "weeks45": ('biweekly', 21),
        "weeks56": ('biweekly', 28),
    }
    time_group, lead_offset_days = lead_params.get(lead, (None, None))
    if time_group is None:
        raise NotImplementedError(f"Lead {lead} not implemented for ECMWF debiased forecasts.")

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    # Get the data with the right days
    agg_days = lead_to_agg_days(lead)
    ds = fuxi_rolled(forecast_start, forecast_end, prob_type=prob_type, agg_days=agg_days)
    ds = lon_base_change(ds)


    # Get the right variable
    attrs = ds.attrs
    ds = ds[variable].to_dataset()
    ds = ds.assign_attrs(attrs)

    # flip the lat dim
    ds = ds.reindex(lat=list(reversed(ds.lat)))

    # convert to mm
    #if variable == 'precip':
    #    ds['precip'] = ds['precip'] * 1000

    # Convert from kelvin
    if variable == 'tmp2m':
        ds['tmp2m'] = ds['tmp2m'] - 273.15

    # Get specific lead
    lead_shift = np.timedelta64(lead_offset_days + 1, 'D')
    ds = ds.sel(lead_time=lead_shift)

    # Time shift - we want target date, instead of forecast date
    ds = shift_forecast_date_to_target_date(ds, 'time', lead)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds


