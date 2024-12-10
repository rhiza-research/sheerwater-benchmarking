"""General utility functions for all parts of the data pipeline."""
import matplotlib.pyplot as plt
import gcsfs
import xarray as xr


def load_object(filepath):
    """Load a file from cloud bucket."""
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    return fs.open(filepath)


def load_netcdf(filepath):
    """Load a NetCDF dataset from cloud bucket."""
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    netc = fs.open(filepath)
    ds = xr.open_dataset(netc, engine="h5netcdf")
    return ds


def write_zarr(ds, filepath):
    """Write an xarray to a Zarr file in cloud bucket."""
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    _ = fs.open(filepath)
    # write it back out to ZARR
    gcsmap = fs.get_mapper(filepath)
    ds.to_zarr(store=gcsmap, mode='w')


def load_zarr(filename):
    """Load a Zarr dataset from cloud bucket."""
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    cache_map = fs.get_mapper(filename)
    ds = xr.open_dataset(cache_map, engine='zarr')
    return ds


def plot_ds(ds, sel=None, variable=None):
    """Plot the first variable in a dataset."""
    # Select the first variable
    if variable is None and isinstance(ds, xr.Dataset):
        variable = list(ds.data_vars)[0]

    # Select the first dim value in all dims except lat and lon
    if sel is None:
        sel = {dim: ds[dim][0].values for dim in ds.dims if dim not in ['lat', 'lon']}

    # Plot the data
    if isinstance(ds, xr.Dataset):
        ds[variable].sel(sel).plot(x='lon')
    else:  # Assume it is a DataArray
        ds.sel(sel).plot()
    plt.show()
