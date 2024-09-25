"""Data utility functions for all parts of the data pipeline."""
import numpy as np
import xarray as xr

from .general_utils import get_grid, base360_to_base180, base180_to_base360, is_wrapped, check_bases


def apply_mask(ds, mask, var, val=0.0):
    """Apply a mask to a dataset.

    Args:
        ds (xr.Dataset): Dataset to apply mask to.
        mask (xr.Dataset): Mask to apply.
        var (str): Variable to mask.
        val (int): Value to mask above (any value that is
            strictly greater than this value will be masked).
    """
    if check_bases(ds, mask) == -1:
        raise ValueError("Datasets have different bases. Cannot mask.")
    # Apply mask
    if mask is not None:
        # This will mask and include any location where there is any land
        ds = ds[var].where(mask > val, drop=False)
        ds = ds.rename({"mask": var})
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

    # Drop the nan values added by the rolling aggregation at the end
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
    except ImportError:
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


def get_globe_slice(ds, lon_slice, lat_slice, lon_col='lon', lat_col='lat', base="base360"):
    """Get a slice of the globe from the dataset.

    Handle the wrapping of the globe when slicing.  

    Args:
        ds (xr.Dataset): Dataset to slice.
        lon_slice (np.ndarray): The longitude slice.
        lat_slice (np.ndarray): The latitude slice.
    """
    if base == "base360" and (lon_slice < 0.0).any():
        raise ValueError("Longitude slice not in base 360 format.")
    if base == "base180" and (lon_slice > 180.0).any():
        raise ValueError("Longitude slice not in base 180 format.")

    wrapped = is_wrapped(lon_slice)
    if not wrapped:
        return ds.sel(**{lon_col: slice(lon_slice[0], lon_slice[-1]),
                         lat_col: slice(lat_slice[0], lat_slice[-1])})
    # A single wrapping discontinuity
    if base == "base360":
        slices = [[lon_slice[0], 360.0], [0.0, lon_slice[-1]]]
    else:
        slices = [[lon_slice[0], 180.0], [-180.0, lon_slice[-1]]]
    ds_subs = []
    for s in slices:
        ds_subs.append(ds.sel(**{
            lon_col: slice(s[0], s[-1]),
            lat_col: slice(lat_slice[0], lat_slice[-1])
        }))
    return xr.concat(ds_subs, dim=lon_col)


def lon_base_change(ds, to_base="base180", lon_col='lon'):
    """Change the base of the dataset from base 360 to base 180 or vice versa.

    Args:
        ds (xr.Dataset): Dataset to change.
        to_base (str): The base to change to. One of:
            - base180
            - base360
    """
    if to_base == "base180":
        if (ds[lon_col] < 0.0).any():
            raise ValueError("Longitude slice must be in base 360 format.")
        lons = base360_to_base180(ds[lon_col].values)
    elif to_base == "base360":
        if (ds[lon_col] > 180.0).any():
            raise ValueError("Longitude slice must be in base 180 format.")
        lons = base180_to_base360(ds[lon_col].values)
    else:
        raise ValueError(f"Invalid base {to_base}.")

    ds = ds.assign_coords({lon_col: lons})
    return ds


def plot_map(ds, var, lon_col='lon'):
    """Plot a map of the dataset, handling longitude wrapping.

    Args:
        ds (xr.Dataset): Dataset to change.
        lon_col (str): The longitude column name.
    """
    if is_wrapped(ds[lon_col].values):
        print("Warning: Wrapped data cannot be plotted. Converting bases for visualization")
        if ds[lon_col].max() > 180.0:
            plot_ds = lon_base_change(ds, to_base="base180")
        else:
            plot_ds = lon_base_change(ds, to_base="base360")
    else:
        plot_ds = ds
    plot_ds[var].plot(x=lon_col)
