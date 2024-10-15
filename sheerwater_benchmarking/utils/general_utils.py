"""General utility functions for all parts of the data pipeline."""
import numpy as np
import dateparser
import gcsfs
import xarray as xr
import geopandas as gpd
from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY, MONTHLY, WEEKLY, YEARLY


DATETIME_FORMAT = "%Y-%m-%d"


def load_object(filepath):
    """Load a file from cloud bucket."""
    fs = gcsfs.GCSFileSystem(project='sheerwater', token='google_default')
    return fs.open(filepath)


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


def generate_dates_in_between(start_time, end_time, date_frequency, return_string=False):
    """Generates dates between two dates based on the frequency.

    Args:
        start_time (str): The start date.
        end_time (str): The end date.
        date_frequency (str): The frequency of the dates.
            One of "daily", "weekly", a day of the week (e.g., "Monday"), or a combination of days
            separated by a slash (e.g., "Monday/Thursday").
        return_string (bool): Whether to return the dates as strings or datetime objects.
    """
    start_date = dateparser.parse(start_time)
    end_date = dateparser.parse(end_time)

    if date_frequency not in ["daily", "weekly"]:
        dates = [
            date
            for date in generate_dates_in_between(start_time, end_time, "daily",
                                                  return_string=False)
            if date.strftime("%A") in date_frequency.split("/")
        ]
    else:
        frequency_to_int = {"daily": 1, "weekly": 7}
        dates = [
            start_date +
            timedelta(days=x * frequency_to_int[date_frequency])
            for x in range(0, int((end_date - start_date).days /
                                  (frequency_to_int[date_frequency])) + 1,)
        ]

    if return_string:
        dates = [date.strftime(DATETIME_FORMAT) for date in dates]
    return dates


def is_valid_forecast_date(model, forecast_type, forecast_date):
    """Checks if the forecast date is valid for the given model and forecast type."""
    valid_forecast_dates = {
        "reforecast": {
            "ecmwf": ("2015-05-14", datetime.today().strftime(DATETIME_FORMAT), "Monday/Thursday"),
            "salient": ("2022-01-01", datetime.today().strftime(DATETIME_FORMAT), "Wednesday"),
        },
        "forecast": {
            "ecmwf": ("2015-05-14", datetime.today().strftime(DATETIME_FORMAT), "Monday/Thursday"),
            "salient": ("2022-01-01", datetime.today().strftime(DATETIME_FORMAT), "Wednesday"),
        },
    }
    assert isinstance(forecast_date, datetime)
    try:
        return forecast_date in generate_dates_in_between(
            *valid_forecast_dates[forecast_type][model], return_string=False)
    except KeyError:
        return False


def get_dates(start_time, end_time, stride="day", return_string=True):
    """Outputs the list of dates corresponding to input date string."""
    # Input is of the form '20170101-20180130'
    start_date = dateparser.parse(start_time)
    end_date = dateparser.parse(end_time)

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
    variable_ordering = ['sheerwater', 'era5', 'ecmwf_hres', 'salient']

    weather_variables = [
        # Static variables (2):
        ('z', 'geopotential', 'geopotential', None),
        ('lsm', 'land_sea_mask', 'land_sea_mask', None),

        # Surface variables (6):
        ('tmp2m', '2m_temperature', '2m_temperature', 'temp'),
        ('precip', 'total_precipitation', 'total_precipitation_6hr', 'precip'),
        ("vwind10m", "10m_v_component_of_wind", "10m_v_component_of_wind", None),
        ("uwind10m", "10m_u_component_of_wind", "10m_u_component_of_wind", None),
        ("msl", "mean_sea_level_pressure", "mean_sea_level_pressure", None),
        ("tisr", "toa_incident_solar_radiation", "toa_incident_solar_radiation", "tsi"),

        # Atmospheric variables (6):
        ("tmp", "temperature", "temperature", None),
        ("uwind", "u_component_of_wind", "u_component_of_wind", None),
        ("vwind", "v_component_of_wind", "v_component_of_wind", None),
        ("hgt", "geopotential", "geopotential", None),
        ("q", "specific_humidity", "specific_humidity", None),
        ("w", "vertical_velocity", "vertical_velocity", None),
    ]

    name_index = variable_ordering.index(variable_type)

    for tup in weather_variables:
        for name in tup:
            if name == variable_name:
                val = tup[name_index]
                if val is None:
                    raise ValueError(f"Variable {variable_name} not implemented.")
                return val

    raise ValueError(f"Variable {variable_name} not found")


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
        lons = np.arange(-180, 180, 1.5)
        lats = np.arange(-90, 90+grid_size, 1.5)
    elif grid == "global0_25":
        grid_size = 0.25
        lons = np.arange(-180, 180, 0.25)
        lats = np.arange(-90, 90+grid_size, 0.25)
    elif grid == "salient0_25":
        grid_size = 0.25
        offset = 0.125
        lons = np.arange(-180.0+offset, 180.0, 0.25)
        lats = np.arange(-90.0+offset, 90.0, 0.25)
    else:
        raise NotImplementedError(
            f"Grid {grid} has not been implemented.")
    if base == "base360":
        lons = base180_to_base360(lons)
        lons = np.sort(lons)
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
        lons = [-24.0, 74.5]
        lats = [-33.0, 37.5]
        filepath = 'gs://sheerwater-datalake/africa.geojson'
        gdf = gpd.read_file(load_object(filepath))
        data = (lons, lats, gdf)
    elif region == "conus":
        lons = [-125.0, -67.0]
        lats = [25.0, 50.0]
        data = (lons, lats)
    elif region == "global":
        lons = [-180.0, 180.0]
        lats = [-90.0, 90.0]
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
