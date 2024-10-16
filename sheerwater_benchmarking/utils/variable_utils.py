"""Variable-related utility functions for all parts of the data pipeline."""
from .space_utils import is_wrapped
from .data_utils import lon_base_change


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


def plot_map(ds, var, lon_dim='lon'):
    """Plot a map of a dataset variable, handling longitude wrapping.

    Args:
        ds (xr.Dataset): Dataset to change.
        var (str): The variable in the dataset to plot.
        lon_dim (str): The longitude column name.
    """
    if is_wrapped(ds[lon_dim].values):
        print("Warning: Wrapped data cannot be plotted. Converting bases for visualization")
        if ds[lon_dim].max() > 180.0:
            plot_ds = lon_base_change(ds, to_base="base180")
        else:
            plot_ds = lon_base_change(ds, to_base="base360")
    else:
        plot_ds = ds
    plot_ds[var].plot(x=lon_dim)
