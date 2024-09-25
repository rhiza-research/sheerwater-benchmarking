"""General utility functions for all parts of the data pipeline."""
import numpy as np

import gcsfs
import xarray as xr
from datetime import datetime, timedelta
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


def generate_dates_in_between(first_date, last_date, date_frequency):
    """Generates dates between two dates based on the frequency.

    Args:
        first_date (datetime): The first date.
        last_date (datetime): The last date.
        date_frequency (str): The frequency of the dates.
            One of "daily", "weekly", "monday/thursday".
    """
    if date_frequency == "monday/thursday":
        dates = [
            date
            for date in generate_dates_in_between(first_date, last_date, "daily")
            if date.strftime("%A") in ["Monday", "Thursday"]
        ]
        return dates
    else:
        frequency_to_int = {"daily": 1, "weekly": 7}
        dates = [
            first_date +
            timedelta(days=x * frequency_to_int[date_frequency])
            for x in range(0, int((last_date - first_date).days /
                                  (frequency_to_int[date_frequency])) + 1,)
        ]
        return dates


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


def get_grid(region_id, base="base360"):
    """Get the longitudes, latitudes and grid size for a named region.

    Args:
        region_id (str): The region to get the grid for. One of:
            - global1_5: 1.5 degree global grid
            - global0_5: 0.5 degree global grid
            - global0_25: 0.25 degree global grid
            - us1_0: 1.0 degree US grid
            - us1_5: 1.5 degree US grid
            - salient_common: Salient common grid
            - africa1_5: 1.5 degree African grid
            - africa0_25: 0.25 degree African grid
        base (str): The base grid to use. One of:
            - base360: 360 degree base longitude grid
            - base180: 180 degree base longitude grid
    """
    if region_id == "global1_5":
        lons = np.arange(0, 360, 1.5)
        lats = np.arange(-90, 90, 1.5)
        grid_size = 1.5
    elif region_id == "global0_5":
        lons = np.arange(0, 360, 0.5)
        lats = np.arange(-90, 90, 0.5)
        grid_size = 0.5
    elif region_id == "global0_25":
        lons = np.arange(0, 360, 0.25)
        lats = np.arange(-90, 90, 0.25)
        grid_size = 0.25
    elif region_id == "us1_0":
        lons = np.arange(235, 303, 1.0)
        lats = np.arange(25.5, 48, 1.0)
        grid_size = 1.0
    elif region_id == "us1_5":
        lons = np.arange(235, 303, 1.5)
        lats = np.arange(25.5, 48, 1.5)
        grid_size = 1.5
    elif region_id == "salient_common":
        # Continuous representation in base180
        lons_180 = np.arange(-25.875, 73.0, 0.25)
        lons = base180_to_base360(lons_180)
        lats = np.arange(-34.875, 38.0, 0.25)
        grid_size = 0.25
    elif region_id == "africa1_5":
        # Continuous representation in base180
        lons_180 = np.arange(-26.0, 73.0, 1.5)
        # Convert to base 360 (default)
        lons = base180_to_base360(lons_180)
        lats = np.arange(-35.0, 38.0, 1.5)
        grid_size = 1.5
    elif region_id == "africa0_25":
        # Continuous representation in base180
        lons_180 = np.arange(-26.0, 73.0, 0.25)
        # Convert to base 360 (default)
        lons = base180_to_base360(lons_180)
        lats = np.arange(-35.0, 38.0, 0.25)
        grid_size = 0.25
    else:
        raise NotImplementedError(
            f"Grid {region_id} has not been implemented.")
    if base == "base180":
        lons = base360_to_base180(lons)
    return lons, lats, grid_size


def get_global_grid(region_id):
    """Get the corresponding global grid to a specified grid."""
    if '0_25' in region_id:
        return 'global0_25'
    elif '1_5' in region_id:
        return 'global1_5'
    else:
        raise NotImplementedError(f"Global grid {region_id} has not been implemented.")


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
