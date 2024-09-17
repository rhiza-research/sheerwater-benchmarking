"""General utility functions for all parts of the data pipeline."""
import numpy as np

import gcsfs
import xarray as xr
from datetime import datetime
from dateutil.rrule import rrule, DAILY, MONTHLY, WEEKLY, YEARLY


DATETIME_FORMAT = "%Y-%m-%d"


def load_netcdf(filepath):
    """Load a NetCDF dataset from cloud bucket."""
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    netc = fs.open(filepath)
    ds = xr.open_dataset(netc, engine="h5netcdf")
    return ds


def write_zarr(ds, filepath):
    """Write an xarray to a Zarr file in cloud bucket."""
    # Load the dataset
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    _ = fs.open(filepath)
    # write it back out to ZARR
    gcsmap = fs.get_mapper(filepath)
    ds.to_zarr(store=gcsmap, mode='w')


def load_zarr(filename):
    """Load a Zarr dataset from cloud bucket."""
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    cache_map = fs.get_mapper(filename)
    ds = xr.open_dataset(cache_map, engine='zarr')
    return ds


def string_to_dt(string):
    """Transforms string to datetime."""
    return datetime.strptime(string, DATETIME_FORMAT)


def dt_to_string(dt):
    """Transforms datetime to string."""
    return datetime.strftime(dt, DATETIME_FORMAT)


def is_valid_forecast_date(model, forecast_type, forecast_date):
    """Checks if the forecast date is valid for the given model and forecast type."""
    valid_forecast_dates = {
        "reforecast": {
            "ecmwf": (string_to_dt("2015-05-14"), datetime.today(), "monday/thursday"),
        },
        "forecast": {
            "ecmwf": (string_to_dt("2015-05-14"), datetime.today(), "monday/thursday"),
        },
    }
    assert isinstance(forecast_date, datetime)
    try:
        return forecast_date in generate_dates_in_between(
            *valid_forecast_dates[forecast_type][model])
    except KeyError:
        return False


def get_dates(start_time, end_time, stride="day", return_string=False):
    """Outputs the list of dates corresponding to input date string."""
    # Input is of the form '20170101-20180130'
    start_date = datetime.strptime(start_time, DATETIME_FORMAT)
    end_date = datetime.strptime(end_time, DATETIME_FORMAT)

    if stride == "day":
        stride = DAILY
    elif stride == "week":
        stride = WEEKLY
    elif stride == "month":
        stride = MONTHLY
    elif stride == "year":
        stride = YEARLY
    else:
        raise ValueError(
            "Only day, week, month, and year strides are supported.")
    dates = [dt for dt in rrule(stride, dtstart=start_date, until=end_date)]
    if return_string:
        dates = [date.strftime(DATETIME_FORMAT) for date in dates]
    return dates


def get_variable(variable_name, variable_type='era5'):
    """Converts a variable in any other type to a variable name of the requested type."""
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

    raise ValueError(f"Variable {variable_name} not found")


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
    elif region_id == "africa0_25":
        longitudes = np.arange(0.125, 360, 0.25)
        latitudes = np.arange(-89.875, 90, 0.25)
        grid_size = 0.25
    elif region_id == "africa1_5":
        longitudes = np.arange(-26.0, 73.0, 1.5)
        latitudes = np.arange(-35.0, 38.0, 1.5)
        grid_size = 1.5
    elif region_id == "africa0_25":
        longitudes = np.arange(-26.0, 73.0, 0.25)
        latitudes = np.arange(-35.0, 38.0, 0.25)
        grid_size = 0.25
    else:
        raise NotImplementedError(
            f"Grid {region_id} has not been implemented.")
    return longitudes, latitudes, grid_size
