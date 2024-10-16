"""Data utility functions for all parts of the data pipeline.

These utility functions take as input an xarray dataset and return a modified
dataset.
"""
import numpy as np
import xarray as xr
import dask
import xarray_regrid  # noqa: F401, import needed for regridding


from .space_utils import (get_grid_ds,
                          base360_to_base180, base180_to_base360,
                          is_wrapped, check_bases,
                          get_region)


def apply_mask(ds, mask, var, val=0.0):
    """Apply a mask to a dataset.

    Args:
        ds (xr.Dataset): Dataset to apply mask to.
        mask (xr.Dataset): Mask to apply.
        var (str): Variable to mask.
        val (int): Value to mask above (any value that is
            strictly greater than this value will be masked).
    """
    # Apply mask
    if mask is not None:
        if check_bases(ds, mask) == -1:
            raise ValueError("Datasets have different bases. Cannot mask.")
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


def regrid(ds, output_grid, method='conservative', base="base180"):
    """Regrid a dataset to a new grid.

    Args:
        ds (xr.Dataset): Dataset to regrid.
        output_grid (str): The output grid resolution. One of valid named grids.
        method (str): The regridding method. One of:
            - linear
            - nearest
            - cubic
            - conservative
            - most_common
        base (str): The base of the longitudes. One of:
            - base180
            - base360
    """
    # Interpret the grid
    ds_out = get_grid_ds(output_grid, base=base)
    regridder = getattr(ds.regrid, method)
    ds = regridder(ds_out)
    return ds


def get_globe_slice(ds, lon_slice, lat_slice, lon_dim='lon', lat_dim='lat', base="base180"):
    """Get a slice of the globe from the dataset.

    Handle the wrapping of the globe when slicing.

    Args:
        ds (xr.Dataset): Dataset to slice.
        lon_slice (np.ndarray): The longitude slice.
        lat_slice (np.ndarray): The latitude slice.
        lon_dim (str): The longitude column name.
        lat_dim (str): The latitude column name.
        base (str): The base of the longitudes. One of:
            - base180, base360
    """
    if base == "base360" and (lon_slice < 0.0).any():
        raise ValueError("Longitude slice not in base 360 format.")
    if base == "base180" and (lon_slice > 180.0).any():
        raise ValueError("Longitude slice not in base 180 format.")

    # Ensure that latitude is sorted before slicing
    ds = ds.sortby(lat_dim)

    wrapped = is_wrapped(lon_slice)
    if not wrapped:
        return ds.sel(**{lon_dim: slice(lon_slice[0], lon_slice[-1]),
                         lat_dim: slice(lat_slice[0], lat_slice[-1])})
    # A single wrapping discontinuity
    if base == "base360":
        slices = [[lon_slice[0], 360.0], [0.0, lon_slice[-1]]]
    else:
        slices = [[lon_slice[0], 180.0], [-180.0, lon_slice[-1]]]
    ds_subs = []
    for s in slices:
        ds_subs.append(ds.sel(**{
            lon_dim: slice(s[0], s[-1]),
            lat_dim: slice(lat_slice[0], lat_slice[-1])
        }))
    return xr.concat(ds_subs, dim=lon_dim)


def lon_base_change(ds, to_base="base180", lon_dim='lon'):
    """Change the base of the dataset from base 360 to base 180 or vice versa.

    Args:
        ds (xr.Dataset): Dataset to change.
        to_base (str): The base to change to. One of:
            - base180
            - base360
        lon_dim (str): The longitude column name.
    """
    if to_base == "base180":
        if (ds[lon_dim] < 0.0).any():
            raise ValueError("Longitude slice must be in base 360 format.")
        lons = base360_to_base180(ds[lon_dim].values)
    elif to_base == "base360":
        if (ds[lon_dim] > 180.0).any():
            raise ValueError("Longitude slice must be in base 180 format.")
        lons = base180_to_base360(ds[lon_dim].values)
    else:
        raise ValueError(f"Invalid base {to_base}.")

    # Check if original data is wrapped
    wrapped = is_wrapped(ds.lon.values)
    ds = ds.assign_coords({lon_dim: lons})

    # Sort the lons after conversion, unless the slice
    # you're considering wraps around the meridian
    # in the resultant base.
    if not wrapped:
        ds = ds.sortby('lon')
    return ds


def clip_region(ds, region, lon_dim='lon', lat_dim='lat'):
    """Clip a dataset to a region.

    Args:
        ds (xr.Dataset): The dataset to clip to a specific region.
        region (str): The region to clip to. One of:
            - africa, conus, global
        lon_dim (str): The name of the longitude dimension.
        lat_dim (str): The name of the latitude dimension.
    """
    # No clipping needed
    if region == 'global':
        return ds

    region_data = get_region(region)
    if len(region_data) == 2:
        lon_slice, lat_slice = region_data
    else:
        # Set up dataframe for clipping
        lon_slice, lat_slice, gdf = region_data
        ds = ds.rio.write_crs("EPSG:4326")
        ds = ds.rio.set_spatial_dims(lon_dim, lat_dim)

        # Clip the grid to the boundary of Shapefile
        ds = ds.rio.clip(gdf.geometry, gdf.crs, drop=False)

    # Slice the globe
    ds = get_globe_slice(ds, lon_slice, lat_slice, lon_dim=lon_dim, lat_dim=lat_dim, base='base180')
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
    """
    # Create a day of year timeseries
    ds = ds.assign_coords(dayofyear=ds[time_dim].dt.dayofyear)
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
