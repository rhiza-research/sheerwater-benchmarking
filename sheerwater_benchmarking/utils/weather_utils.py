"""Variable-related utility functions for all parts of the data pipeline."""


def get_variable(variable_name, variable_type='era5'):
    """Converts a variable in any other type to a variable name of the requested type."""
    variable_ordering = ['sheerwater', 'era5', 'ecmwf_hres', 'ecmwf_ifs_er', 'salient']

    weather_variables = [
        # Static variables (2):
        ('z', 'geopotential', 'geopotential', None, None),
        ('lsm', 'land_sea_mask', 'land_sea_mask', None, None),

        # Surface variables (6):
        ('tmp2m', '2m_temperature', '2m_temperature', '2m_temperature', 'temp'),
        ('precip', 'total_precipitation', 'total_precipitation_6hr', 'total_precipitation_24hr', 'precip'),
        ("vwind10m", "10m_v_component_of_wind", "10m_v_component_of_wind", None, None),
        ("uwind10m", "10m_u_component_of_wind", "10m_u_component_of_wind", None, None),
        ("msl", "mean_sea_level_pressure", "mean_sea_level_pressure", None, None),
        ("tisr", "toa_incident_solar_radiation", "toa_incident_solar_radiation", None, "tsi"),

        # Atmospheric variables (6):
        ("tmp", "temperature", "temperature", None, None),
        ("uwind", "u_component_of_wind", "u_component_of_wind", None, None),
        ("vwind", "v_component_of_wind", "v_component_of_wind", None, None),
        ("hgt", "geopotential", "geopotential", None, None),
        ("q", "specific_humidity", "specific_humidity", None, None),
        ("w", "vertical_velocity", "vertical_velocity", None, None),
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
