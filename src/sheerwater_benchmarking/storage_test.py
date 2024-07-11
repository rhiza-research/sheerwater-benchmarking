from sheerwater_benchmarking.utils.remote import dask_remote
import gcsfs
import numpy as np
import xarray as xr

@dask_remote
def load_netcdf():
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    netc = fs.open("gs://sheerwater-datalake/precip.1999.nc")

    ds = xr.open_dataset(netc, engine="h5netcdf")

    # write it back out to ZARR
    gcsmap = fs.get_mapper("gs://sheerwater-datalake/precip.1999.zarr")
    ds.to_zarr(store=gcsmap, mode='w')

@dask_remote
def load_zarr():
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    gcsmap = fs.get_mapper("gs://sheerwater-datalake/precip.1999.zarr")

    ds = xr.open_dataset(gcsmap, engine="zarr")
    ds.precip.encoding['missing_value'] = np.nan

    # NOTE This is not possible
    # recommended solution is to write it to local disk then copy it to the cloud
    netc = fs.open("gs://sheerwater-datalake/precip2.1999.nc", mode='w')
    ds.to_netcdf(netc, engine='h5netcdf')

