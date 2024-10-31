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
from .time_utils import add_dayofyear


def roll_and_agg(ds, agg, agg_col, agg_fn="mean"):
    """Rolling aggregation of the dataset.

    Applies rolling and then corrects rolling window labels to be left aligned.

    Args:
        ds (xr.Dataset): Dataset to aggregate.
        variable (str): Variable to aggregate.
        agg (int): Aggregation period in days.
        agg_col (str): Column to aggregate over.
        agg_fn (str): Aggregation function. One of mean or sum.
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


def regrid(ds, output_grid, method='conservative', base="base180", grid_chunks=None):
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
        grid_chunks (dict): The chunking of the output grid.
            If None, no chunking is applied.
    """
    # Interpret the grid
    ds_out = get_grid_ds(output_grid, base=base)
    if grid_chunks is not None:
        ds_out = ds_out.chunk(grid_chunks)
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
            print("Longitude already in base 180 format.")
            return ds
        lons = base360_to_base180(ds[lon_dim].values)
    elif to_base == "base360":
        if (ds[lon_dim] > 180.0).any():
            print("Longitude already in base 360 format.")
            return ds
        lons = base180_to_base360(ds[lon_dim].values)
    else:
        raise ValueError(f"Invalid base {to_base}.")

    # Check if original data is wrapped
    wrapped = is_wrapped(ds.lon.values)

    # Then assign new coordinates
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


def apply_mask(ds, mask, var=None, val=0.0, grid='global1_5'):
    """Apply a mask to a dataset.

    Args:
        ds (xr.Dataset): Dataset to apply mask to.
        mask (str): The mask to apply. One of: 'lsm', None
        var (str): Variable to mask. If None, applies to apply to all variables.
        val (int): Value to mask above (any value that is
            strictly greater than this value will be masked).
        grid (str): The grid resolution of the dataset.
    """
    # No masking needed
    if mask is None:
        return ds

    if mask == 'lsm':
        # Import here to avoid circular imports
        from sheerwater_benchmarking.masks import land_sea_mask
        mask_ds = land_sea_mask(grid=grid).compute()
    else:
        raise NotImplementedError("Only land-sea mask is implemented.")

    # Check that the mask and dataset have the same dimensions
    if not all([dim in ds.dims for dim in mask_ds.dims]):
        raise ValueError("Mask and dataset must have the same dimensions.")

    if check_bases(ds, mask_ds) == -1:
        raise ValueError("Datasets have different longitude bases. Cannot mask.")

    att = ds.attrs
    ds = ds[var].where(mask_ds > val, drop=False)

    # Preserve name and attributes
    ds = ds.rename({"mask": var})
    ds.attrs = att

    return ds


def is_valid(ds, var, mask, region, grid, valid_threshold=0.5):
    """Check if the dataset is valid in the given region and mask."""
    if mask == 'lsm':
        # Import here to avoid circular imports
        from sheerwater_benchmarking.masks import land_sea_mask
        mask_ds = land_sea_mask(grid=grid).compute()
    elif mask is None:
        mask_ds = get_grid_ds(grid_id=grid).compute()
    else:
        raise ValueError("Only land-sea mask is implemented.")

    mask_ds = clip_region(mask_ds, region)

    data_count = (~ds[var].where(mask_ds > 0.0).isnull()).sum(dim=['lat', 'lon']).min()['mask'].compute().values
    mask_count = int((mask_ds['mask'] > 0.0).sum().compute().values)

    if data_count < mask_count * valid_threshold:
        return False
    return True


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
