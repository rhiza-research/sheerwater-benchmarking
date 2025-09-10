"""Data utility functions for all parts of the data pipeline.

These utility functions take as input an xarray dataset and return a modified
dataset.
"""
import numpy as np
import dask
import xarray_regrid  # noqa: F401, import needed for regridding


from .space_utils import get_grid_ds
from .time_utils import add_dayofyear


def roll_and_agg(ds, agg, agg_col, agg_fn="mean", agg_thresh=None):
    """Rolling aggregation of the dataset.

    Applies rolling and then corrects rolling window labels to be left aligned.

    Args:
        ds (xr.Dataset): Dataset to aggregate.
        variable (str): Variable to aggregate.
        agg (int): Aggregation period in days.
        agg_col (str): Column to aggregate over.
        agg_fn (str): Aggregation function. One of mean or sum.
        agg_thresh(int): number of data required to agg.
    """
    agg_kwargs = {
        f"{agg_col}": agg,
        "min_periods": agg_thresh,
        "center": False
    }
    # Apply n-day rolling aggregation
    if agg_fn == "mean":
        ds_agg = ds.rolling(**agg_kwargs).mean()
    elif agg_fn == "sum":
        ds_agg = ds.rolling(**agg_kwargs).sum()
    else:
        raise NotImplementedError(f"Aggregation function {agg_fn} not implemented.")

    # Check to see if coord is a time value
    assert np.issubdtype(ds[agg_col].dtype, np.timedelta64) or np.issubdtype(ds[agg_col].dtype, np.datetime64)

    # Drop the nan values added by the rolling aggregation at the end
    ds_agg = ds_agg.dropna(agg_col, how="all")

    # Correct coords to left-align the aggregated forecast window
    # (default is right aligned)
    ds_agg = ds_agg.assign_coords(**{f"{agg_col}": ds_agg[agg_col]-np.timedelta64(agg-1, 'D')})

    return ds_agg


def regrid(ds, output_grid, method='conservative', base="base180", output_chunks=None):
    """Regrid a dataset to a new grid.

    Args:
        ds (xr.Dataset): Dataset to regrid.
        output_grid (str): The output grid resolution. One of valid named grids.
        method (str): The regridding method. One of:
            'linear', 'nearest', 'cubic', 'conservative', 'most_common'.
        base (str): The base of the longitudes. One of 'base180', 'base360'.
        output_chunks (dict): Chunks for the output dataset (optional).
            Only used for conservative regridding.
    """
    # Interpret the grid
    ds_out = get_grid_ds(output_grid, base=base)
    # Output chunks only for conservative regridding
    kwargs = {'output_chunks': output_chunks} if method == 'conservative' else {}
    regridder = getattr(ds.regrid, method)
    ds = regridder(ds_out, **kwargs)
    return ds


def get_anomalies(ds, clim, var, time_dim='time'):
    """Calculate the anomalies of a dataset.

    The input dataset should have a time dimension of the type datetime64[ns].
    The climatology dataset should have a dayofyear dimension. The datasets
    should have the same spatial dimensions and coordinates.

    Args:
        ds (xr.Dataset): Dataset to calculate anomalies for.
        clim (xr.Dataset): Climatology dataset to calculate anomalies from.
        var (str): Variable to calculate anomalies for.
        time_dim (str): The name of the time dimension.
    """
    # Create a day of year timeseries
    ds = add_dayofyear(ds, time_dim=time_dim)
    with dask.config.set(**{'array.slicing.split_large_chunks': True}):
        clim_ds = clim.sel(dayofyear=ds.dayofyear)
        clim_ds = clim_ds.drop('dayofyear')

    # Drop day of year coordinates
    ds = ds.drop('dayofyear')

    # Ensure that the climatology and dataset have the same dimensions
    if not all([dim in ds.dims for dim in clim_ds.dims]):
        raise ValueError("Climatology and dataset must have the same dimensions.")

    # Calculate the anomalies
    anom = ds[var] - clim_ds[var]
    anom = anom.to_dataset()
    return anom
