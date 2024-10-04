"""A climatology baseline for benchmarking."""
from sheerwater_benchmarking.utils import dask_remote, cacheable
from sheerwater_benchmarking.reanalysis import era5_agg


@dask_remote
@cacheable(data_type='array',
           cache_args=['first_year', 'last_year', 'variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           auto_rechunk=False)
def climatology(first_year, last_year, variable, grid="global1_5", mask="lsm"):
    """Compute the climatology of the ERA5 data. Years are inclusive."""
    start_time = f"{first_year}-01-01"
    end_time = f"{last_year}-12-31"

    # Get single day, masked data between start and end years
    ds = era5_agg(start_time, end_time, variable=variable,
                  grid=grid, agg=1, mask=mask)

    # Add day of year as a coordinate
    ds = ds.assign_coords(dayofyear=ds.time.dt.dayofyear)

    # Take average over the period to produce climatology
    return ds.groupby('dayofyear').mean(dim='time')


@dask_remote
@cacheable(data_type='array',
           cache_args=['variable', 'grid', 'mask'],
           chunking={"lat": 121, "lon": 240, "dayofyear": 366},
           cache=False,
           auto_rechunk=False)
def climatology_standard_30yr(variable, grid="global1_5", mask="lsm"):
    """Compute the standard 30-year climatology of ERA5 data from 1991-2020."""
    # Get single day, masked data between start and end years
    return climatology(1991, 2020, variable, grid=grid, mask=mask)


@dask_remote
@cacheable(data_type='array',
           timeseries='time',
           cache=False,
           cache_args=['variable', 'lead', 'dorp', 'grid', 'mask'])
def climatology_forecast(start_time, end_time, variable, lead, dorp='d',
                         grid='africa0_25', mask='lsm'):
    """Standard format forecast data for climatology forecast."""
    leads_param = {
    }
    lead_params = {
        "week1": (1, 0, 'W'),
        "week2": (1, 1, 'W'),
        "week3": (1, 2, 'W'),
        "week4": (1, 3, 'W'),
        "week5": (1, 4, 'W'),
        "week6": (1, 5, 'W'),
        "weeks12": (2, 0, 'W'),
        "weeks23": (2, 1, 'W'),
        "weeks34": (2, 2, 'W'),
        "weeks45": (2, 3, 'W'),
        "weeks56": (2, 4, 'W'),
        "month1": (1, 0, 'M'),
        "month2": (1, 1, 'M'),
        "month3": (1, 2, 'M'),
        "quarter1": (3, 0, 'M'),
        "quarter2": (3, 1, 'M'),
        "quarter3": (3, 2, 'M'),
        "quarter4": (3, 3, 'M')
    }
    duration, offset, date_str = lead_params['lead']

    ds = climatology_standard_30yr(grid=grid, mask=mask)
    if dorp != 'd':
        raise NotImplementedError('Probabilistic climatology not available.')

    ds = ds.rename({'quantiles': 'member'})
    ds = ds.rename({'forecast_date': 'time'})

    return ds
