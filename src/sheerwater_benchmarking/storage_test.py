from sheerwater_benchmarking.utils.remote import dask_remote
import gcsfs
import numpy as np
import xarray as xr
import os
import pandas as pd

@dask_remote
def load_netcdf(filepath):
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    netc = fs.open(filepath)

    ds = xr.open_dataset(netc, engine="h5netcdf")

    # write it back out to ZARR
    gcsmap = fs.get_mapper(os.path.splitext(filepath)[0]+".zarr")
    ds.to_zarr(store=gcsmap, mode='w')

@dask_remote
def load_zarr(filepath):
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    gcsmap = fs.get_mapper(filepath)

    ds = xr.open_dataset(gcsmap, engine="zarr")
    ds.precip.encoding['missing_value'] = np.nan

    # NOTE This is not possible
    # recommended solution is to write it to local disk then copy it to the cloud
    netc = fs.open(os.path.splitext(filepath)[0]+".nc", mode='w')
    ds.to_netcdf(netc, engine='h5netcdf')


filepath = "gs://sheerwater-datalake/iri-ecmwf-precip-all-global1_5-ef-reforecast.h5"
# filepath = "gs://sheerwater-datalake/precip.1999.zarr"
df = load_netcdf(filepath)

# filepath = "gs://sheerwater-datalake/iri-ecmwf-precip-all-global1_5-ef-reforecast.h5"
filepath = "gs://sheerwater-datalake/iri-ecmwf-tmp2m-all-global1_5-ef-forecast.h5"
# filepath = "gs://sheerwater-datalake/precip.2021.nc"
fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
netc = fs.open(filepath)
ds = xr.open_dataset(netc, engine="h5netcdf", group="data", phony_dims="sort")
# gcsmap = fs.get_mapper(os.path.splitext(filepath)[0]+".zarr")
# ds.to_zarr(store=gcsmap, mode='w')

filepath = "/Users/avocet/content/forecast_rodeo_ii/data/dataframes/iri-ecmwf-tmp2m-all-global1_5-ef-forecast.h5"
df = pd.read_hdf(filepath, "data")

df

df["start_date"].sort_values()

import dask.dataframe as dd

filepath = "/Users/avocet/content/forecast_rodeo_ii/data/dataframes/iri-ecmwf-tmp2m-all-global1_5-ef-forecast.h5"
df = dd.read_hdf(filepath, "data")  


