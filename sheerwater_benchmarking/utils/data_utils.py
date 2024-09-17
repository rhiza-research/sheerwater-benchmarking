"""Data utility functions for all parts of the data pipeline."""
import numpy as np
import xarray as xr

from .general_utils import get_grid


def apply_mask(ds, mask, var, val, rename_dict={}):
    """Apply a mask to a dataset.

    Args:
        ds (xr.Dataset): Dataset to apply mask to.
        mask (xr.Dataset): Mask to apply.
        var (str): Variable to mask.
        val (int): Value to mask.
        rename_dict (dict): Dictionary to rename variables.
    """
    # Apply mask
    if mask is not None:
        # This will mask and include any location where there is any land
        ds = ds[var].where(mask > val, drop=False)
        ds = ds.rename(rename_dict)
    return ds


def roll_and_agg(ds, agg, agg_col, agg_fn="mean"):
    """Rolling aggregation of the dataset.

    Args:
        ds (xr.Dataset): Dataset to aggregate.
        variable (str): Variable to aggregate.
        agg (int): Aggregation period in days.
        agg_col (str): Column to aggregate over.
        agg_fn (str): Aggregation function. One of:
            - mean
            - sum
    """
    agg_kwargs = {
        f"{agg_col}": agg,
        "min_periods": agg,
        "center": False
    }
    # Apply n-day rolling aggregation
    if agg_fn == "mean":
        ds_agg = ds.rolling(**agg_kwargs).mean()
    elif agg_fn == "sum":
        ds_agg = ds.rolling(**agg_kwargs).sum()
    else:
        raise NotImplementedError(f"Aggregation function {agg_fn} not implemented.")

    # Drop the nan values added by the rolling aggregration at the end
    ds_agg = ds_agg.dropna(agg_col, how="all")

    # Correct coords to left-align the aggregated forecast window
    # (default is right aligned)
    ds_agg = ds_agg.assign_coords(**{f"{agg_col}": ds_agg[agg_col]-np.timedelta64(agg-1, 'D')})
    return ds_agg


def regrid(ds, output_grid, method='bilinear', lat_col='lat', lon_col='lon'):
    """Regrid a dataset to a new grid.

    Args:
        ds (xr.Dataset): Dataset to regrid.
        output_grid (str): The output grid resolution. One of valid named grids.
        method (str): The regridding method. One of:
            - bilinear
            - conservative
            - nearest_s2d
            - nearest_d2s
            - patch
            - regrid
        lat_col (str): The name of the latitude column.
        lon_col (str): The name of the longitude column.
    """
    # Attempt to import xesmf and throw an error if it doesn't exist
    try:
        import xesmf as xe
    except:
        raise RuntimeError(
            "Failed to import XESMF. Try running in coiled instead: 'rye run coiled-run ...")

    # Interpret the grid
    lons, lats, _ = get_grid(output_grid)

    ds_out = xr.Dataset(
        {
            "lat": (["lat"], lats, {"units": "degrees_north"}),
            "lon": (["lon"], lons, {"units": "degrees_east"}),
        })

    # Rename the columns if necessary
    if lat_col != 'lat' or lon_col != 'lon':
        ds = ds.rename({lat_col: 'lat', lon_col: 'lon'})

    # Reorder the data columns data if necessary - extra dimensions must be on the left
    # TODO: requires that all vars have the same dims; should be fixed
    coords = None
    for var in ds.data_vars:
        coords = ds.data_vars[var].dims
        break

    if coords[-2] != 'lat' and coords[-1] != 'lon':
        # Get coords that are not lat and lon
        other_coords = [x for x in coords if x != 'lat' and x != 'lon']
        ds = ds.transpose(*other_coords, 'lat', 'lon')

    # Do the regridding
    regridder = xe.Regridder(ds, ds_out, method)
    ds = regridder(ds)

    # Change the column names back
    if lat_col != 'lat' or lon_col != 'lon':
        ds = ds.rename({'lat': lat_col, 'lon': lon_col})

    return ds
