"""Space and geography utility functions for all parts of the data pipeline."""
import numpy as np
import xarray as xr
import geopandas as gpd
import rioxarray  # noqa: F401 - needed to enable .rio attribute
from importlib import import_module

from .general_utils import load_object, plot_ds, plot_ds_map

import matplotlib.pyplot as plt


def clean_country_name(country):
    """Clean a country name to match the geojson file."""
    return country.lower().replace(' ', '_')


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


def clip_region(ds, region, lon_dim='lon', lat_dim='lat', drop=False, keep_shape=False):
    """Clip a dataset to a region.

    Args:
        ds (xr.Dataset): The dataset to clip to a specific region.
        region (str): The region to clip to. One of:
            - africa, conus, global
        lon_dim (str): The name of the longitude dimension.
        lat_dim (str): The name of the latitude dimension.
        drop (bool): Whether to drop the original coordinates that are NaN'd by clipping.
        keep_shape (bool): Whether to keep the shape of the region.
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
        ds = ds.rio.clip(gdf.geometry, gdf.crs, drop=drop)

    # Slice the globe
    if not keep_shape:
        ds = get_globe_slice(ds, lon_slice, lat_slice, lon_dim=lon_dim, lat_dim=lat_dim, base='base180')
    return ds


def apply_mask(ds, mask, var=None, val=0.0, grid='global1_5'):
    """Apply a mask to a dataset.

    Args:
        ds (xr.Dataset): Dataset to apply mask to.
        mask (str): The mask to apply. One of: 'lsm', None
        var (str): Variable to mask. If None, applies to apply to all variables.
        val (int): Value to mask below (any value that is
            strictly less than this value will be masked).
        grid (str): The grid resolution of the dataset.
    """
    # No masking needed
    if mask is None:
        return ds

    if mask == 'lsm':
        # Import here to avoid circular imports
        from sheerwater_benchmarking.masks import land_sea_mask
        if grid == 'global1_5' or grid == 'global0_25':
            mask_ds = land_sea_mask(grid=grid).compute()
        else:
            # TODO: Should implement a more resolved land-sea mask for the other grids
            mask_ds = land_sea_mask(grid='global0_25')
            mask_ds = regrid(mask_ds, grid, method='nearest').compute()
    else:
        raise NotImplementedError("Only land-sea mask is implemented.")

    # Check that the mask and dataset have the same dimensions
    if not all([dim in ds.dims for dim in mask_ds.dims]):
        raise ValueError("Mask and dataset must have the same dimensions.")

    if check_bases(ds, mask_ds) == -1:
        raise ValueError("Datasets have different longitude bases. Cannot mask.")

    # Ensure that the mask and the dataset don't have different precision
    # This MUST be np.float32 as of 4/28/25...unsure why?
    # Otherwise the mask doesn't match and lat/lons get dropped
    mask_ds['lon'] = np.round(mask_ds.lon, 5).astype(np.float32)
    mask_ds['lat'] = np.round(mask_ds.lat, 5).astype(np.float32)
    ds['lon'] = np.round(ds.lon, 5).astype(np.float32)
    ds['lat'] = np.round(ds.lat, 5).astype(np.float32)

    if isinstance(var, str):
        # Mask a single variable
        ds[var] = ds[var].where(mask_ds['mask'] > val, drop=False)
    else:
        # Mask multiple variables
        ds = ds.where(mask_ds['mask'] > val, drop=False)
    return ds


def get_grid_ds(grid_id, base="base180"):
    """Get a dataset equal to ones for a given region."""
    lons, lats, _ = get_grid(grid_id, base=base)
    data = np.ones((len(lons), len(lats)))
    ds = xr.Dataset(
        {"mask": (['lon', 'lat'], data)},
        coords={"lon": lons, "lat": lats}
    )
    return ds


def get_grid(grid, base="base180"):
    """Get the longitudes, latitudes and grid size for a given global grid.

    Args:
        grid (str): The resolution to get the grid for. One of:
            - global1_5: 1.5 degree global grid
            - global0_25: 0.25 degree global grid
            - salient0_25: 0.25 degree Salient global grid
        base (str): The base grid to use. One of:
            - base360: 360 degree base longitude grid
            - base180: 180 degree base longitude grid
    """
    if grid == "global1_5":
        grid_size = 1.5
        offset = 0.0
    elif grid == "chirps":
        grid_size = 0.05
        offset = 0.025
    elif grid == "imerg":
        grid_size = 0.1
        offset = 0.05
    elif grid == "global0_25":
        grid_size = 0.25
        offset = 0.0
    elif grid == "salient0_25":
        grid_size = 0.25
        offset = 0.125
    else:
        raise NotImplementedError(
            f"Grid {grid} has not been implemented.")

    # Instantiate the grid
    lons = np.arange(-180.0+offset, 180.0, grid_size)
    eps = 1e-6  # add a small epsilon to the end of the grid to enable poles for lat
    lats = np.arange(-90.0+offset, 90.0+eps, grid_size)
    if base == "base360":
        lons = base180_to_base360(lons)
        lons = np.sort(lons)

    # Round the longitudes and latitudes to the nearest 1e-5 to avoid floating point precision issues
    lons = np.round(lons, 5).astype(np.float32)
    lats = np.round(lats, 5).astype(np.float32)
    return lons, lats, grid_size


def get_region(region):
    """Get the longitudes, latitudes boundaries and/or shapefile for a given region.

    Note: assumes longitude in base180 format.

    Args:
        region (str): The region to get the data for. Can be a country, region, continent, or global.

    Returns:
        data: The longitudes and latitudes of the region as a tuple,
            or the shapefile defining the region.
    """
    # Get which admin level the region is at (e.g., countries, continents, etc.)
    admin_level = get_admin_level(region)[0]
    valid_labels = get_region_labels(admin_level)
    if region not in valid_labels:
        raise NotImplementedError(f"Region {region} has not been implemented.")

    # If the region does not specify countries, then just return the lats and lons bounding box
    from .region_defs import valid_regions
    if admin_level != 'countries' and len(valid_regions[admin_level][region]['countries']) == 0:
        return (valid_regions[admin_level][region]['lons'], valid_regions[admin_level][region]['lats'])

    # Get the geojson of world countries
    tol = 0.01
    filepath = 'gs://sheerwater-datalake/regions/world_countries.geojson'
    gdf = gpd.read_file(load_object(filepath))
    # Get the countries, lats and lons for the region
    if admin_level == 'countries':
        gdf = gdf[gdf['name'].apply(clean_country_name) == clean_country_name(region)]
        lons = np.array([gdf['geometry'].bounds['minx'].values[0]-tol, gdf['geometry'].bounds['maxx'].values[0] + tol])
        lats = np.array([gdf['geometry'].bounds['miny'].values[0]-tol, gdf['geometry'].bounds['maxy'].values[0] + tol])
        return (lons, lats, gdf)

    # Read and merge multiple countries from the regions to get a unified regional gdf
    countries = valid_regions[admin_level][region]['countries']
    lats = valid_regions[admin_level][region]['lats']
    lons = valid_regions[admin_level][region]['lons']

    countries = [clean_country_name(country) for country in countries]
    region_gdf = gdf[gdf['name'].apply(clean_country_name).isin(countries)]
    geometry = region_gdf.geometry.unary_union
    region_gdf = gpd.GeoDataFrame(geometry=[geometry], crs=region_gdf.crs, columns=['name'])
    region_gdf['name'] = region

    # Get the bounding box of the region
    if lons is None:
        lons = np.array([region_gdf['geometry'].bounds['minx'].values[0]-tol,
                        region_gdf['geometry'].bounds['maxx'].values[0] + tol])
    if lats is None:
        lats = np.array([region_gdf['geometry'].bounds['miny'].values[0]-tol,
                        region_gdf['geometry'].bounds['maxy'].values[0] + tol])
    return (lons, lats, region_gdf)


def get_admin_level(region):
    """Get the admin level of a region.

    Returns the admin level and a boolean indicating if the region is
    already at the admin level.

    Args:
        region (str): The region to get the admin level of.
    """
    if region == 'countries':
        return 'countries', 1
    if region in get_region_labels('countries'):
        return 'countries', 0

    from .region_defs import valid_regions
    for admin_level, data in valid_regions.items():
        if region == admin_level:
            return admin_level, 1
        if region in data.keys():
            return admin_level, 0

    raise NotImplementedError(
        f"Region {region} has not been implemented.")


def get_region_labels(admin_level='countries'):
    """Get default names for administrative levels.

    Args:
        admin_level (str): The administrative level to get labels for.
            One of: 'countries', 'african_regions', 'continents', 'meterological_zones', 'hemispheres', 'global'

    Returns:
        list: List of region names for the specified admin level

    Raises:
        NotImplementedError: If the admin level is not supported
    """
    if admin_level == 'countries':
        from .region_defs import countries
        return [clean_country_name(country) for country in countries]

    from .region_defs import valid_regions
    try:
        return list(valid_regions[admin_level].keys())
    except KeyError:
        # Check if the region is a subregion of a valid region
        new_admin_level = get_admin_level(admin_level)
        return list(valid_regions[new_admin_level].keys())


def base360_to_base180(lons):
    """Converts a list of longitudes from base 360 to base 180.

    Args:
        lons (list, float): A list of longitudes, or a single longitude
    """
    if not isinstance(lons, np.ndarray) and not isinstance(lons, list):
        lons = [lons]
    val = [x - 360.0 if x >= 180.0 else x for x in lons]
    if len(val) == 1:
        return val[0]
    return np.array(val)


def base180_to_base360(lons):
    """Converts a list of longitudes from base 180 to base 360.

    Args:
        lons (list, float): A list of longitudes, or a single longitude
    """
    if not isinstance(lons, np.ndarray) and not isinstance(lons, list):
        lons = [lons]
    val = [x + 360.0 if x < 0.0 else x for x in lons]
    if len(val) == 1:
        return val[0]
    return np.array(val)


def is_wrapped(lons):
    """Check if the longitudes are wrapped.

    Works for both base180 and base360 longitudes. Requires that
    longitudes are in increasing order, outside of a wrap point.
    """
    wraps = (np.diff(lons) < 0.0).sum()
    if wraps > 1:
        raise ValueError("Only one wrapping discontinuity allowed.")
    elif wraps == 1:
        return True
    return False


def check_bases(ds, dsp, lon_col='lon', lon_colp='lon'):
    """Check if the bases of two datasets are the same."""
    if ds[lon_col].max() > 180.0:
        base = "base360"
    elif ds[lon_col].min() < 0.0:
        base = "base180"
    else:
        print("Warning: Dataset base is ambiguous")
        return 0

    if dsp[lon_colp].max() > 180.0:
        basep = "base360"
    elif dsp[lon_colp].min() < 0.0:
        basep = "base180"
    else:
        print("Warning: Dataset base is ambiguous")
        return 0

    # If bases are identifiable and unequal
    if base != basep:
        return -1
    return 0


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
