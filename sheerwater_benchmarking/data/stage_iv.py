"""Stage IV precipitation data product.

Zarr Dataset: https://usgs.osn.mghpcc.org/mdmf/gdp/stageiv_combined.zarr
Attributes:
    Conventions:   CF-1.7
    institution:   US National Weather Service - NCEP
    last_timestep: 2025-02-03T13

Dimensions:
    time: 203040
    y:    881
    x:    1121

Coordinates:
    latitude:
        long_name:     latitude
        standard_name: latitude
        units:         degrees_north
    longitude:
        long_name:     longitude
        standard_name: longitude
        units:         degrees_east
    time:
        calendar: proleptic_gregorian
        units:    hours since 2002-01-01 00:00:00

Data Variables:
    Total_precipitation_surface_1_Hour_Accumulation:
        type:          float32
        shape:         time * y * x
        cell_methods:  time: sum (interval: 1 hr)
        description:   Total precipitation
        long_name:     Total precipitation (1_Hour Accumulation) at ground or water surface
        standard_name: precipitation_amount
        units:         kg m-2
    crs:
        type:                        int64
        shape:
        crs_wkt:                     GEOGCRS["WGS 84",ENSEMBLE["World Geodetic System 1984 ensemble",MEMBER["World
                                     Geodetic System 1984 (Transit)"],MEMBER["World Geodetic System 1984
                                     (G730)"],MEMBER["World Geodetic System 1984 (G873)"],MEMBER["World Geodetic System
                                     1984 (G1150)"],MEMBER["World Geodetic System 1984 (G1674)"],MEMBER["World Geodetic
                                     System 1984 (G1762)"],MEMBER["World Geodetic System 1984 (G2139)"],ELLIPSOID["WGS 8
                                     4",6378137,298.257223563,LENGTHUNIT["metre",1]],ENSEMBLEACCURACY[2.0]],PRIMEM["Gree
                                     nwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic
                                     latitude
                                     (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic
                                     longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE[
                                     "Horizontal component of 3D
                                     system."],AREA["World."],BBOX[-90,-180,90,180]],ID["EPSG",4326]]
        geographic_crs_name:         WGS 84
        grid_mapping_name:           latitude_longitude
        horizontal_datum_name:       World Geodetic System 1984 ensemble
        inverse_flattening:          298.257223563
        longitude_of_prime_meridian: 0.0
        prime_meridian_name:         Greenwich
        reference_ellipsoid_name:    WGS 84
        semi_major_axis:             6378137.0
        semi_minor_axis:             6356752.314245179
"""
import numpy as np
import xarray as xr
from dateutil import parser

from sheerwater_benchmarking.utils import apply_mask, cacheable, clip_region, dask_remote, regrid, roll_and_agg


@dask_remote
@cacheable(data_type='array',
           cache_args=['year', 'grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def stage_iv_gridded(year, grid):
    """Regridded Stage IV precipitation data by year.

    Reads data from USGS Geo Data Portal.

    Data was stored in grib format, but was converted to zarr format.
    https://code.usgs.gov/wma/nhgf/geo-data-portal/stage_iv_precip
    We are using the zarr produced from that conversion.

    Data is available from 2002-present.

    Args:
        year: The year to load data for
        grid: The target grid to regrid to
    """
    # Open the zarr store
    store = 'https://usgs.osn.mghpcc.org/mdmf/gdp/stageiv_combined.zarr'
    ds = xr.open_dataset(
        store,
        engine='zarr',
        chunks={},
        drop_variables=['crs'],
    )

    # Select the year
    ds = ds.sel(time=slice(f"{year}-01-01", f"{year}-12-31"))

    # Ensure consistent naming
    ds = ds.rename(
        {'Total_precipitation_surface_1_Hour_Accumulation': 'precip'},
    )

    # stage iv precipitation is measured in kg/m^2 and hourly
    # 1 kg/m^2 = 1 mm
    # to convert to our target units of mm/day, we are summing to daily values
    ds['precip'] = ds['precip'].resample(time='1D').sum()
    ds['precip'].attrs.update(units='mm/day')

    # Fix coordinates and dimensions
    # The Stage IV data comes in a polar stereographic projection with 2D lat/lon coordinates.
    # This means each point in the grid has a unique latitude and longitude value, stored in 2D arrays.
    # However, for regridding and further processing, we need a regular lat/lon grid where:
    #   1. Latitudes only vary along rows (constant within columns)
    #   2. Longitudes only vary along columns (constant within rows)
    #   3. The grid spacing is uniform
    #
    # The process to convert from 2D irregular coordinates to 1D regular coordinates is:
    #   1. Find the bounds of the data (min/max lat/lon)
    #   2. Create regular 1D arrays of lat/lon within these bounds
    #   3. Create a target grid with these regular coordinates
    #   4. Interpolate the original data onto this regular grid
    #   5. Set up the final coordinates for regridding

    # Step 1: Get min/max bounds of the data
    lat_min = ds.latitude.min().values
    lat_max = ds.latitude.max().values
    lon_min = ds.longitude.min().values
    lon_max = ds.longitude.max().values

    # Step 2: Create regular lat/lon coordinates at approximately the same resolution
    # We maintain the same number of points as the original grid
    n_lat = ds.sizes['y']
    n_lon = ds.sizes['x']
    new_lats = np.linspace(lat_min, lat_max, n_lat)  # Regular spacing in latitude
    new_lons = np.linspace(lon_min, lon_max, n_lon)  # Regular spacing in longitude

    # First assign coordinates with y and x
    ds = ds.assign_coords({
        'y': ('y', new_lats),
        'x': ('x', new_lons)
    })

    # Step 3: Create a new dataset with the target regular grid
    # First, set up the basic coordinates
    target_ds = xr.Dataset(
        coords={
            'y': ('y', new_lats),  # y-dimension varies with latitude
            'x': ('x', new_lons),  # x-dimension varies with longitude
        }
    )

    # Create 2D meshgrid for the target dataset
    # This creates 2D arrays where:
    #   - lon_mesh has the same longitude repeated for each latitude
    #   - lat_mesh has the same latitude repeated for each longitude
    lon_mesh, lat_mesh = np.meshgrid(new_lons, new_lats)
    target_ds = target_ds.assign_coords({
        'latitude': (['y', 'x'], lat_mesh),   # 2D latitude field
        'longitude': (['y', 'x'], lon_mesh),  # 2D longitude field
    })

    # Step 4: Interpolate the original data onto the regular grid
    # xarray's interp_like uses the 2D lat/lon coordinates to perform the interpolation
    # This ensures each point in the original grid is mapped to the correct location
    # in the regular grid based on its actual coordinates
    ds = ds.interp_like(target_ds, method='linear')

    # Step 5: Set up the final coordinates for regridding
    # First, clean up any existing coordinates to start fresh
    ds = ds.reset_coords(drop=True)

    # Assign the 1D coordinates with the correct dimension names
    # The regridding function expects dimensions named 'lat' and 'lon'
    ds = ds.assign_coords({
        'y': ('y', new_lats),
        'x': ('x', new_lons)
    })

    # Rename the dimensions to what the regridding function expects
    ds = ds.rename({'y': 'lat', 'x': 'lon'})

    # At this point, the dataset has:
    #   1. Regular spacing in both latitude and longitude
    #   2. 1D coordinate arrays instead of 2D coordinates
    #   3. Correct dimension names for regridding

    # Now we can regrid to the final target grid
    ds = regrid(ds, grid, base='base180', method='conservative')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['agg_days', 'grid'],
           chunking={'lat': 300, 'lon': 300, 'time': 365})
def stage_iv_rolled(start_time, end_time, agg_days, grid):
    """Stage IV rolled and aggregated."""
    years = range(parser.parse(start_time).year, parser.parse(end_time).year + 1)

    datasets = []
    for year in years:
        ds = stage_iv_gridded(year, grid, filepath_only=True)
        datasets.append(ds)

    ds = xr.open_mfdataset(
        datasets,
        engine='zarr',
        parallel=True,
        chunks={'lat': 300, 'lon': 300, 'time': 365},
    )

    ds = roll_and_agg(ds, agg=agg_days, agg_col="time", agg_fn='mean')

    return ds


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache_args=['variable', 'agg_days', 'grid', 'mask', 'region'],
           cache=False)
def stage_iv(start_time, end_time, variable, agg_days, grid='global0_25', mask='lsm', region='global'):
    """Final Stage IV product."""
    if variable != 'precip':
        raise NotImplementedError("Only precip provided by Stage IV.")

    ds = stage_iv_rolled(start_time, end_time, agg_days, grid)

    # Apply masking
    ds = apply_mask(ds, mask, var=variable, grid=grid)

    # Clip to specified region
    ds = clip_region(ds, region=region)

    return ds
