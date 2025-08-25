"""Space and geography utility functions for all parts of the data pipeline."""
import numpy as np
import xarray as xr
import geopandas as gpd

from .general_utils import load_object


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


def clip_region(ds, region, lon_dim='lon', lat_dim='lat', drop=False):
    """Clip a dataset to a region.

    Args:
        ds (xr.Dataset): The dataset to clip to a specific region.
        region (str): The region to clip to. One of:
            - africa, conus, global
        lon_dim (str): The name of the longitude dimension.
        lat_dim (str): The name of the latitude dimension.
        drop (bool): Whether to drop the original coordinates that are NaN'd by clipping.
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
    """Get the longitudes, latitudes boundaries or shapefile for a given region.

    Note: assumes longitude in base180 format.

    Args:
        region (str): The resolution to get the grid for. One of:
            - africa: the African continent
            - conus: the CONUS region
            - global: the global region

    Returns:
        data: The longitudes and latitudes of the region as a tuple,
            or the shapefile defining the region.
    """
    if region == "africa":
        # Get the countries of Africa shapefile
        lons = np.array([-23.0, 58.0])
        lats = np.array([-35.0, 37.5])
        filepath = 'gs://sheerwater-datalake/africa.geojson'
        gdf = gpd.read_file(load_object(filepath))
        data = (lons, lats, gdf)
    elif region == "east_africa":
        # Get the countries of Africa shapefile
        lons = np.array([28.2, 42.6])
        lats = np.array([-12.1, 5.6])
        filepath = 'gs://sheerwater-datalake/regions/africa.geojson'
        gdf = gpd.read_file(load_object(filepath))

        # Filter the gdf
        gdf = gdf.where(gdf['sovereignt'].isin(['Kenya',
                                                'Burundi',
                                                'Rwanda',
                                                'United Republic of Tanzania',
                                                'Uganda'])).dropna(how='all')
        data = (lons, lats, gdf)
    elif region == "kenya":
        # Get the countries of Africa shapefile
        lons = np.array([33.5, 42.0])
        lats = np.array([-5.0, 5.2])
        filepath = 'gs://sheerwater-datalake/regions/africa.geojson'
        gdf = gpd.read_file(load_object(filepath))

        # Filter the gdf
        gdf = gdf.where(gdf['sovereignt'].isin(['Kenya'])).dropna(how='all')
        data = (lons, lats, gdf)
    elif region == "conus":
        lons = np.array([-125.0, -67.0])
        lats = np.array([25.0, 50.0])
        filepath = 'gs://sheerwater-datalake/regions/usa.geojson'
        gdf = gpd.read_file(load_object(filepath))
        # Remove non-CONUS states
        gdf = gdf[~gdf['NAME'].isin(['Alaska', 'Hawaii', 'Puerto Rico'])]
        data = (lons, lats, gdf)
    elif region == "global":
        lons = np.array([-180.0, 180.0])
        lats = np.array([-90.0, 90.0])
        data = (lons, lats)
    else:
        filepath = 'gs://sheerwater-datalake/regions/world_countries.geojson'
        gdf = gpd.read_file(load_object(filepath))
        # Make name lowercase
        gdf = gdf[gdf['name'].str.lower() == region.lower()]
        if len(gdf) == 0:
            raise NotImplementedError(
                f"Region {region} has not been implemented.")
        tol = 0.01
        lons = np.array([gdf['geometry'].bounds['minx'].values[0]-tol, gdf['geometry'].bounds['maxx'].values[0] + tol])
        lats = np.array([gdf['geometry'].bounds['miny'].values[0]-tol, gdf['geometry'].bounds['maxy'].values[0] + tol])
        data = (lons, lats, gdf)
    return data


def add_region(ds, admin_level='country'):
    """Add a region coordinate to an xarray dataset at a specific admin level.

    Available admin levels are 'country', 'region', 'continent', and 'world'.

    Args:
        ds (xarray.Dataset): The dataset to add region coordinates to
        admin_level (str): The admin level to add to the dataset

    Returns:
        xarray.Dataset: Dataset with added region coordinate
    """
    # Get the list of regions for the specified admin level
    region_names = get_region_labels(admin_level)

    # Create region coordinate using xarray coordinates
    regions = xr.full_like(ds.lat * ds.lon, 'no region', dtype=object)

    # Loop through each region and label grid cells
    for region in region_names:
        # Clip dataset to this region
        region_ds = clip_region(ds, region)

        # Get the valid grid cells for this region using xarray operations
        valid_mask = ~region_ds.isnull().any(dim=[d for d in region_ds.dims if d not in ['lat', 'lon']])

        # Label those cells with the region name using xarray where
        for var in valid_mask.data_vars:
            if 'lat' in valid_mask[var].dims and 'lon' in valid_mask[var].dims:
                regions = regions.where(~valid_mask[var], region)
                break

    # Add region coordinate to dataset
    ds = ds.assign_coords({admin_level: regions})

    return ds


def get_region_labels(admin_level='country'):
    """Get default names for administrative levels.

    Args:
        admin_level (str): The administrative level to get labels for.
            One of: 'country', 'region', 'continent', 'world'

    Returns:
        list: List of region names for the specified admin level

    Raises:
        NotImplementedError: If the admin level is not supported
    """
    if admin_level == 'country':
        # Load world countries from the existing data source
        filepath = 'gs://sheerwater-datalake/regions/world_countries.geojson'
        regions_df = gpd.read_file(load_object(filepath))
        # Return the country names from the geojson file
        return regions_df['name'].tolist()
    elif admin_level == 'region':
        return [
            'east_africa', 'west_africa', 'north_africa', 'central_africa',
            'southern_africa', 'horn_of_africa', 'sahel', 'maghreb',
            'west_africa', 'central_africa', 'east_africa', 'southern_africa'
        ]
    elif admin_level == 'continent':
        return ['africa', 'europe', 'asia', 'north_america', 'south_america', 'australia', 'antarctica']
    elif admin_level == 'world':
        return ['global', 'northern_hemisphere', 'southern_hemisphere', 'eastern_hemisphere', 'western_hemisphere']
    else:
        raise NotImplementedError(
            f"Admin level '{admin_level}' not supported. Use one of: 'country', 'region', 'continent', 'world'")


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
