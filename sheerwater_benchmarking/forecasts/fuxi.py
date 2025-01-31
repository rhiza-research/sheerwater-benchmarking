from huggingface_hub import hf_hub_download
from huggingface_hub.utils import EntryNotFoundError
from sheerwater_benchmarking.utils.secrets import huggingface_read_token
from sheerwater_benchmarking.utils import cacheable, dask_remote
import py7zr
import glob
import pandas as pd
import dask
import xarray as xr

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

    return ds

@dask_remote
@cacheable(data_type='array', cache_args=[], timeseries='time', 
           chunking={'lat': 121, 'lon': 240, 'lead_time': 14, 'time': 2, 'member': 51})
def fuxi_raw(start_time, end_time):
    dates = pd.date_range(start_time, end_time)

    datasets = []
    for date in dates:
        date = date.date()
        ds = fuxi_single_forecast(date, filepath_only=True)
        datasets.append(ds)

    #datasets = dask.compute(*datasets)
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