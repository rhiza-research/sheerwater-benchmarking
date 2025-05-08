"""General utility functions for all parts of the data pipeline."""
import matplotlib.pyplot as plt
import gcsfs
import numpy as np
import xarray as xr
import itertools
import multiprocessing
import tqdm
import dask

import plotly.graph_objects as go


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
    is_time = np.issubdtype(ds[variable].dtype, np.datetime64) or (ds[variable].dtype == np.dtype('<M8[ns]'))
    is_timedelta = np.issubdtype(ds[variable].dtype, np.timedelta64)
    if isinstance(ds, xr.Dataset):
        if is_time:
            ds[variable].dt.dayofyear.sel(sel).plot(x='lon')
        elif is_timedelta:
            ds[variable].dt.days.sel(sel).plot(x='lon')
        else:
            ds[variable].sel(sel).plot(x='lon')
    else:  # Assume it is a DataArray
        if is_time:
            ds.dt.dayofyear.sel(sel).plot(x='lon')
        elif is_timedelta:
            ds.dt.days.sel(sel).plot(x='lon')
        else:
            ds.sel(sel).plot(x='lon')
    plt.show()


def plot_ds_map(ds, sel=None, variable=None, zoom=3, center_lat=None, center_lon=None):
    """Plot a variable from an xarray dataset on a Plotly map."""
    # Select the first variable
    if variable is None and isinstance(ds, xr.Dataset):
        variable = list(ds.data_vars)[0]

    # Select the first dim value in all dims except lat and lon
    if sel is None:
        sel = {dim: ds[dim][0].values for dim in ds.dims if dim not in ['lat', 'lon']}

    # Plot the data
    if isinstance(ds, xr.Dataset):
        data = ds[variable].sel(sel)
    else:  # Assume it is a DataArray
        data = ds.sel(sel)

    # Convert to pandas DataFrame for easier manipulation with Plotly
    df = data.to_dataframe().reset_index()

    # Set center coordinates (optional, if not provided)
    if center_lat is None:
        center_lat = df['lat'].mean()
    if center_lon is None:
        center_lon = df['lon'].mean()

    # Create a Plotly map figure
    fig = go.Figure(go.Scattermapbox(
        lat=df['lat'],
        lon=df['lon'],
        mode='markers',
        marker=dict(
            size=8,
            color=df[variable],  # Color based on the variable
            colorscale='Viridis',  # Use a predefined colorscale
            colorbar=dict(title=variable)
        ),
        text=df[variable],  # Show values when hovering over points
        hoverinfo='text'
    ))

    # Update layout for the map
    fig.update_layout(
        title=f'{variable} on Map',
        mapbox_style="open-street-map",
        mapbox_zoom=zoom,
        mapbox_center={"lat": center_lat, "lon": center_lon},
        margin={"r": 0, "t": 30, "l": 0, "b": 0},
    )

    # Show the map
    fig.show()


def run_in_parallel(func, iterable, parallelism, local_multiproc=False):
    """Run a function in parallel with dask delayed.

    Args:
        func(callable): A function to call. Must take one of iterable as an argument.
        iterable (iterable): Any iterable object to pass to func.
        parallelism (int): Number of func(iterables) to run in parallel at a time.
        local_multiproc (bool): If true run using multiprocessing pool instead of dask delayed batches.
    """
    iterable, copy = itertools.tee(iterable)
    length = len(list(copy))
    counter = 0
    success_count = 0
    failed = []
    if parallelism <= 1:
        for i, it in enumerate(iterable):
            print(f"Running {i+1}/{length}")
            out = func(it)
            if out is not None:
                success_count += 1
            else:
                failed.append(it)
    else:
        if local_multiproc:
            with multiprocessing.Pool(parallelism) as p:
                results = list(tqdm.tqdm(p.imap_unordered(func, iterable), total=length))
                outputs = [result[0] for result in results]
                for out in outputs:
                    if out is not None:
                        success_count += 1
        else:
            for it in itertools.batched(iterable, parallelism):
                output = []
                print(f"Running {counter+1}...{counter+parallelism}/{length}")
                for i in it:
                    out = dask.delayed(func)(i)
                    if out is not None:
                        success_count += 1
                    else:
                        failed.append(i)

                    output.append(out)

                dask.compute(output)
                counter = counter + parallelism

    print(f"{success_count}/{length} returned non-null values. Runs that failed: {failed}")
