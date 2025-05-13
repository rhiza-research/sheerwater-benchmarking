"""Space and geography utility functions for all parts of the data pipeline."""
import numpy as np
import xarray as xr
import geopandas as gpd

from .general_utils import load_object


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
        raise NotImplementedError(
            f"Region {region} has not been implemented.")
    return data


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
