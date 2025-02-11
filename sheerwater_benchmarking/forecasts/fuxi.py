"""Interface for FuXi forecasts."""

import os
import glob
import shutil
import dask
import xarray as xr
import numpy as np
import pandas as pd
import py7zr
from functools import partial

from huggingface_hub import hf_hub_download
from huggingface_hub.utils import EntryNotFoundError

from sheerwater_benchmarking.utils.secrets import huggingface_read_token
from sheerwater_benchmarking.utils import (dask_remote, cacheable,
                                           apply_mask, clip_region,
                                           lon_base_change,
                                           target_date_to_forecast_date,
                                           shift_forecast_date_to_target_date, lead_to_agg_days, roll_and_agg)
from sheerwater_benchmarking.tasks import spw_precip_preprocess, spw_rainy_onset


@dask_remote
@cacheable(data_type='array', cache_args=['date'])
def fuxi_single_forecast(date):
    """Download a single forecast from the FuXi dataset."""
    token = huggingface_read_token()

    date = str(date).replace('-', '')
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

    def preprocess(ds):
        """Preprocess the dataset to add the member dimension."""
        ff = ds.encoding["source"]
        member = ff.split('/')[-2]
        ds = ds.assign_coords(member=member)
        ds = ds.expand_dims(dim='member')
        return ds

    # Transform and drop dataa
    print("Opening dataset")
    ds = xr.open_mfdataset(files, engine='netcdf4', preprocess=preprocess)
    # return ds

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
           validate_cache_timeseries=False,
           chunking={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})
def fuxi_raw(start_time, end_time, delayed=False):
    """Combine a range of forecasts with or without dask delayed. Returns daily, unagged fuxi timeseries."""
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

# Decided not to regrid fuxi data, but leaving this as a comment
# @dask_remote
# @cacheable(data_type='array',
#           timeseries='time',
#           cache_args=['grid'],
#           cache_disable_if={'grid':'global1_5'},
#           chunking={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})
# def fuxi_gridded(start_time, end_time, grid='global1_5'):
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
           cache_args=['variable', 'agg_days', 'prob_type'],
           chunking={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})
def fuxi_rolled(start_time, end_time, variable, agg_days=7, prob_type='probabilistic'):
    """Roll and aggregate the FuXi data."""
    ds = fuxi_raw(start_time, end_time)

    # sort the lat dim and change the lon dim
    ds = lon_base_change(ds)
    ds = ds.sortby(ds.lat)

    # Get the right variable
    ds = ds[[variable]]

    # convert based on a linear conversion factor of the average forecast to the era5 average
    if variable == 'precip':
        ds['precip'] = ds['precip'] * 27.4

    # Convert from kelvin
    if variable == 'tmp2m':
        ds['tmp2m'] = ds['tmp2m'] - 273.15

    # Convert the lead time into a time delta that is 0 indexed
    ds['lead_time'] = ds['lead_time'] - 1
    ds['lead_time'] = ds['lead_time'] * np.timedelta64(1, 'D')

    # If deterministic average across the members
    if prob_type == 'deterministic':
        ds = ds.mean(dim='member')
        ds = ds.assign_attrs(prob_type="deterministic")
    else:
        ds = ds.assign_attrs(prob_type="ensemble")
    ds = roll_and_agg(ds, agg=agg_days, agg_col="lead_time", agg_fn="mean")

    return ds


def _process_lead(variable, lead):
    """Helper function for interpreting lead for FuXI forecasts."""
    lead_params = {}
    if variable == 'rainy_onset':  # rainy onset only has daily leads out to day 36
        lead_params = {f"day{i+1}": i for i in range(33)}
    elif variable == 'rainy_onset_no_drought':
        # need to add 11 days to the lead to handle drought condition
        lead_params = {f"day{i+1}": i for i in range(20)}
    else:
        for i in range(46):
            lead_params[f"day{i+1}"] = i
        for i in [0, 7, 14, 21, 28, 35]:
            lead_params[f"week{i//7+1}"] = i
        for i in [0, 7, 14, 21, 28]:
            lead_params[f"weeks{(i//7)+1}{(i//7)+2}"] = i
    lead_offset_days = lead_params.get(lead, None)
    if lead_offset_days is None:
        raise NotImplementedError(f"Lead {lead} not implemented for FuXi {variable} forecasts.")

    agg_days = lead_to_agg_days(lead)
    return agg_days, lead_offset_days


@dask_remote
def fuxi_spw(start_time, end_time, lead,
             prob_type='probabilistic', prob_threshold=0.6,
             onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
             drought_condition=False,
             grid='global1_5', mask='lsm', region="global"):
    """The FuXi SPW forecasts."""
    # Get rainy season onset forecast
    prob_label = prob_type if prob_type == 'deterministic' else 'ensemble'

    # Set up aggregation and shift functions for SPW
    agg_fn = partial(fuxi_rolled, start_time, end_time, variable='precip', prob_type=prob_type)

    def shift_fn(ds, shift_by_days):
        """Helper function for selecting and shifting lead for FuXi forecasts."""
        # Select the appropriate lead
        lead_offset_days = _process_lead('precip', lead)[1]
        lead_sel = {'lead_time': np.timedelta64(lead_offset_days + shift_by_days, 'D')}
        ds = ds.sel(**lead_sel)
        # Time shift - we want target date, instead of forecast date
        ds = shift_forecast_date_to_target_date(ds, 'time', lead)
        return ds

    roll_days = [8, 11] if not drought_condition else [8, 11, 11]
    shift_days = [0, 0] if not drought_condition else [0, 0, 11]
    data = spw_precip_preprocess(agg_fn, shift_fn, agg_days=roll_days, shift_days=shift_days,
                                 mask=mask, region=region, grid=grid)

    (prob_dim, prob_threshold) = ('member', prob_threshold) if prob_type == 'probabilistic' else (None, None)
    ds = spw_rainy_onset(data,
                         onset_group=onset_group, aggregate_group=aggregate_group,
                         time_dim='time',
                         prob_type=prob_label, prob_dim=prob_dim, prob_threshold=prob_threshold,
                         drought_condition=drought_condition,
                         mask=mask, region=region, grid=grid)
    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'prob_type', 'grid', 'mask', 'region'])
def fuxi(start_time, end_time, variable, lead, prob_type='deterministic',
         grid='global1_5', mask='lsm', region="global"):
    """Final FuXi forecast interface."""
    if grid != 'global1_5':
        raise NotImplementedError("Only 1.5 grid implemented for FuXi.")

    agg_days, lead_offset_days = _process_lead(variable, lead)

    # Convert start and end time to forecast start and end based on lead time
    forecast_start = target_date_to_forecast_date(start_time, lead)
    forecast_end = target_date_to_forecast_date(end_time, lead)

    prob_label = prob_type if prob_type == 'deterministic' else 'ensemble'
    if variable == 'rainy_onset' or variable == 'rainy_onset_no_drought':
        drought_condition = variable == 'rainy_onset_no_drought'
        ds = fuxi_spw(forecast_start, forecast_end, lead,
                      prob_type=prob_type, prob_threshold=0.6,
                      onset_group=['ea_rainy_season', 'year'], aggregate_group=None,
                      drought_condition=drought_condition,
                      grid=grid, mask=mask, region=region)
        # Rainy onset is sparse, so we need to set the sparse attribute
        ds = ds.assign_attrs(sparse=True)
    else:
        ds = fuxi_rolled(forecast_start, forecast_end, variable=variable, prob_type=prob_type, agg_days=agg_days)
        # Select the appropriate lead
        lead_sel = {'lead_time': np.timedelta64(lead_offset_days, 'D')}
        ds = ds.sel(**lead_sel)
        ds = shift_forecast_date_to_target_date(ds, 'time', lead)

        # Apply masking and clip to region
        ds = apply_mask(ds, mask, var=variable, grid=grid)
        ds = clip_region(ds, region=region)

    # Assign probability label
    ds = ds.assign_attrs(prob_type=prob_label)
    return ds
