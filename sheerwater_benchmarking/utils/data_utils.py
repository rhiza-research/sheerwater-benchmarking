"""Utility functions for data processing."""
import xarray as xr
import numpy as np


def get_variable(variable_name, variable_type='era5'):
    """Converts a variable in any other type to a variable name of the requested type"""

    variable_ordering = ['sheerwater', 'era5']

    weather_variables = [
        # Static variables (2):
        ('z', 'geopotential'),
        ('lsm', 'land_sea_mask'),

        # Surface variables (6):
        ('tmp2m', '2m_temperature'),
        ("precip", "total_precipitation"),
        ("vwind10m", "10m_v_component_of_wind"),
        ("uwind10m", "10m_u_component_of_wind"),
        ("msl", "mean_sea_level_pressure"),
        ("tisr", "toa_incident_solar_radiation"),

        # Atmospheric variables (6):
        ("tmp", "temperature"),
        ("uwind", "u_component_of_wind"),
        ("vwind", "v_component_of_wind"),
        ("hgt", "geopotential"),
        ("q", "specific_humidity"),
        ("w", "vertical_velocity"),
    ]

    name_index = variable_ordering.index(variable_type)

    for tup in weather_variables:
        for name in tup:
            if name == variable_name:
                return tup[name_index]

    raise ValueError(f"Variable {variable} not found")


def get_grid(region_id):
    """Get the longitudes, latitudes and grid size for a named region."""
    if region_id == "global1_5":
        longitudes = np.arange(0, 360, 1.5)
        latitudes = np.arange(-90, 90, 1.5)
        grid_size = 1.5
    elif region_id == "global0_5":
        longitudes = np.arange(0.25, 360, 0.5)
        latitudes = np.arange(-89.75, 90, 0.5)
        grid_size = 0.5
    elif region_id == "global0_25":
        longitudes = np.arange(0, 360, 0.25)
        latitudes = np.arange(-90, 90, 0.25)
        grid_size = 0.25
    elif region_id == "us1_0":
        longitudes = np.arange(-125.0, -67.0, 1)
        latitudes = np.arange(25.0, 50.0, 1)
        grid_size = 1.0
    elif region_id == "us1_5":
        longitudes = np.arange(-123.0, -67.5, 1.5)
        latitudes = np.arange(25.5, 48, 1.5)
        grid_size = 1.5
    elif region_id == "salient_common":
        longitudes = np.arange(0.125, 360, 0.25)
        latitudes = np.arange(-89.875, 90, 0.25)
        grid_size = 0.25
    else:
        raise NotImplementedError(
            "Only grids global1_5, us1_0 and us1_5 have been implemented.")
    return longitudes, latitudes, grid_size


def regrid(ds, output_grid, method='bilinear', lat_col='lat', lon_col='lon'):

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
